import { ChainGun, GunGraph } from '@notabug/chaingun'
import { NabLmdbGraphConnector } from './NabLmdbGraphConnector'
import SocketClusterGraphConnector from '@notabug/chaingun-socket-cluster-connector'
import WS from 'ws'
import { Schema } from '@notabug/peer'

interface Opts extends GunChainOptions {
  socketCluster: any
  lmdb?: any
}

const DEFAULT_OPTS: Opts = {
  WS,
  peers: [],
  socketCluster: {
    hostname: process.env.GUN_SC_HOST || 'localhost',
    port: process.env.GUN_SC_PORT || '4444',
    autoReconnect: true,
    autoReconnectOptions: {
      initialDelay: 1,
      randomness: 100,
      maxDelay: 500
    }
  },
  lmdb: {
    path: './data',
    mapSize: 1024 ** 4
  }
}

const promiseNothing = Promise.resolve(undefined)

export class NabLmdbStorage extends ChainGun {
  lmdb: NabLmdbGraphConnector
  socket: SocketClusterGraphConnector

  constructor(options = DEFAULT_OPTS) {
    const { socketCluster: scOpts, lmdb: lmdbOpts, ...opts } = {
      ...DEFAULT_OPTS,
      ...options
    }

    const graph = new GunGraph()
    const lmdb = new NabLmdbGraphConnector(lmdbOpts)
    const socket = new SocketClusterGraphConnector(options.socketCluster)

    super({ graph, ...opts })

    this.lmdb = lmdb
    this.socket = socket

    this.publishDiff = this.publishDiff.bind(this)
    this.authenticateAndListen()
    this.persistIncoming()
    this.respondToGets()
  }

  authenticateAndListen() {
    if (process.env.GUN_SC_PUB && process.env.GUN_SC_PRIV) {
      this.socket
        .authenticate(process.env.GUN_SC_PUB, process.env.GUN_SC_PRIV)
        .then(() => {
          console.log(`Logged in as ${process.env.GUN_SC_PUB}`)
        })
        .catch(err => console.error('Error logging in:', err.stack || err))
    }
  }

  respondToGets() {
    this.socket.subscribeToChannel(
      'gun/get/validated',
      (msg: GunMsg) => {
        if (msg && !msg.get && !msg.put) console.log('msg', msg)
        if (!msg) return
        const msgId = msg['#']
        const soul = (msg.get && msg.get['#']) || ''
        if (!soul) return
        this.lmdb.get({
          soul,
          msgId,
          cb: (m: GunMsg) => {
            if (!m.put) this.socket.publishToChannel('gun/get/missing', msg)
            this.socket.publishToChannel(`gun/@${msgId}`, m)
          }
        })
      },
      { waitForAuth: true }
    )
  }

  persistIncoming() {
    this.socket.subscribeToChannel('gun/put/validated', (msg: GunMsg) => {
      if (!msg) return
      const data = msg.put
      if (!data) return
      const msgId = msg['#']
      const putGraph: GunPut = {}
      let hasNodes = false

      for (let soul in data) {
        const node = data[soul]
        if (node) {
          putGraph[soul] = node
          hasNodes = true
        }
      }

      if (!hasNodes) return

      this.lmdb.put({
        msgId,
        graph: putGraph,
        diffCb: this.publishDiff,
        cb: m => this.socket.publishToChannel(`gun/@${msgId}`, m)
      })
    })
  }

  publishDiff(msg: GunMsg) {
    const msgId = msg['#']
    const diff = msg.put
    if (!diff) return
    this.socket.publishToChannel('gun/put/diff', msg)
    const souls = Object.keys(diff)

    if (souls.length === 1) {
      // No need to split into multiple messages
      const soul = souls[0]
      if (!diff[soul]) return
      this.socket.publishToChannel(`gun/nodes/${soul}`, msg)
      return
    }

    for (let i = 0; i < souls.length; i++) {
      const soul = souls[i]
      const nodeDiff = diff[soul]
      if (!nodeDiff) continue
      this.socket.publishToChannel(`gun/nodes/${soul}`, {
        '#': `${msgId}/${soul}`,
        put: {
          [soul]: nodeDiff
        }
      })
    }
  }

  uploadThing(thingId: string): Promise<void> {
    const thingSoul = Schema.Thing.route.reverse({
      thingId
    })
    const thing = this.lmdb.readNode(thingSoul)
    if (!thing) {
      return promiseNothing
    }
    const dataSoul = thing.data && thing.data['#']
    if (!dataSoul) return promiseNothing
    const thingData = this.lmdb.readNode(dataSoul)
    if (!thingData) return promiseNothing

    return new Promise(ok => {
      const cb = (ack: any) => {
        if (this.socket.isConnected) console.log('uploaded', thingId, ack)
        ok()
        off()
      }
      const off = this.socket.put({
        graph: {
          [thingSoul]: thing,
          [dataSoul]: thingData
        },
        cb
      })
    })
  }

  uploadThings() {
    this.lmdb.eachThingIdAsync(this.uploadThing.bind(this))
  }

  scanThings(terms: string) {
    const re = new RegExp(terms, "i")
    this.lmdb.eachThingId(thingId => {
      const thingSoul = Schema.Thing.route.reverse({
        thingId
      })
      const thing = this.lmdb.readNode(thingSoul)
      if (!thing) {
        return
      }
      const dataSoul = thing.data && thing.data['#']
      if (!dataSoul) return
      const thingData = this.lmdb.readNode(dataSoul)
      if (!thingData) return
      const body = thingData.body || ''
      const title = thingData.title || ''

      if (re.test(body) || re.test(title)) {
        console.log('thing Matches', thingId, thingData)
      }
    })
  }
}
