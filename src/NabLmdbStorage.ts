import { ChainGun, GunGraph } from '@notabug/chaingun'
import SocketClusterGraphConnector from '@notabug/chaingun-socket-cluster-connector'
import { Schema } from '@notabug/peer'
import WS from 'ws'
import { NabLmdbGraphConnector } from './NabLmdbGraphConnector'

interface Opts extends GunChainOptions {
  readonly socketCluster: any
  readonly lmdb?: any
}

const DEFAULT_OPTS: Opts = {
  WS,
  lmdb: {
    mapSize: 1024 ** 4,
    path: './data'
  },
  peers: [],
  socketCluster: {
    autoReconnect: true,
    hostname: process.env.GUN_SC_HOST || 'localhost',
    port: process.env.GUN_SC_PORT || '4444'
  }
}

const promiseNothing = Promise.resolve(undefined)

export class NabLmdbStorage extends ChainGun {
  protected readonly lmdb: NabLmdbGraphConnector
  protected readonly socket: SocketClusterGraphConnector

  constructor(options = DEFAULT_OPTS) {
    const { socketCluster: scOpts, lmdb: lmdbOpts, ...opts } = {
      ...DEFAULT_OPTS,
      ...options
    }

    const graph = new GunGraph()
    const lmdb = new NabLmdbGraphConnector(lmdbOpts)
    const socket = new SocketClusterGraphConnector(options.socketCluster)

    super({ graph, ...opts })
    socket.socket!.on('connect', this.authenticate.bind(this))

    this.lmdb = lmdb
    this.socket = socket

    this.publishDiff = this.publishDiff.bind(this)
    this.authenticate()
    this.persistIncoming()
    this.respondToGets()
    setInterval(() => this.authenticate(), 1000 * 60 * 30)
  }

  public respondToGets(): void {
    this.socket.subscribeToChannel(
      'gun/get/validated',
      (msg: GunMsg) => {
        if (msg && !msg.get && !msg.put) {
          // tslint:disable-next-line: no-console
          console.log('msg', msg)
        }
        if (!msg) {
          return
        }
        const msgId = msg['#']
        const soul = (msg.get && msg.get['#']) || ''
        if (!soul) {
          return
        }
        this.lmdb.get({
          cb: (m: GunMsg) => {
            if (!m.put) {
              this.socket.publishToChannel('gun/get/missing', msg)
            }
            this.socket.publishToChannel(`gun/@${msgId}`, m)
          },
          msgId,
          soul
        })
      },
      { waitForAuth: true }
    )
  }

  public persistIncoming(): void {
    this.socket.subscribeToChannel('gun/put/validated', (msg: GunMsg) => {
      if (!msg) {
        return
      }
      const data = msg.put
      if (!data) {
        return
      }
      const msgId = msg['#']
      const putGraph: GunPut = {}
      let hasNodes = false

      for (const soul in data) {
        if (!soul) {
          continue
        }
        const node = data[soul]
        if (node) {
          putGraph[soul] = node
          hasNodes = true
        }
      }

      if (!hasNodes) {
        return
      }

      this.lmdb.put({
        cb: m => this.socket.publishToChannel(`gun/@${msgId}`, m),
        diffCb: this.publishDiff,
        graph: putGraph,
        msgId
      })
    })
  }

  public uploadThing(thingId: string): Promise<void> {
    const thingSoul = Schema.Thing.route.reverse({
      thingId
    })
    const thing = this.lmdb.readNode(thingSoul)
    if (!thing) {
      return promiseNothing
    }
    const dataSoul = thing.data && thing.data['#']
    if (!dataSoul) {
      return promiseNothing
    }
    const thingData = this.lmdb.readNode(dataSoul)
    if (!thingData) {
      return promiseNothing
    }

    return new Promise(ok => {
      const cb = (ack: any) => {
        if (this.socket.isConnected) {
          // tslint:disable-next-line: no-console
          console.log('uploaded', thingId, ack)
        }
        ok()
        off()
      }
      const off = this.socket.put({
        cb,
        graph: {
          [thingSoul]: thing,
          [dataSoul]: thingData
        }
      })
    })
  }

  public uploadThings(): void {
    this.lmdb.eachThingIdAsync(this.uploadThing.bind(this))
  }

  public scanThings(terms: string): void {
    const re = new RegExp(terms, 'i')
    this.lmdb.eachThingId(thingId => {
      const thingSoul = Schema.Thing.route.reverse({
        thingId
      })
      const thing = this.lmdb.readNode(thingSoul)
      if (!thing) {
        return
      }
      const dataSoul = thing.data && thing.data['#']
      if (!dataSoul) {
        return
      }
      const thingData = this.lmdb.readNode(dataSoul)
      if (!thingData) {
        return
      }
      const body = thingData.body || ''
      const title = thingData.title || ''

      if (re.test(body) || re.test(title)) {
        // tslint:disable-next-line: no-console
        console.log('thing Matches', thingId, thingData)
      }
    })
  }

  public authenticate(): Promise<void> {
    if (!process.env.GUN_SC_PUB || !process.env.GUN_SC_PRIV) {
      throw new Error('Missing GUN_SC_PUB or GUN_SC_PRIV')
    }

    return (
      this.socket
        .authenticate(process.env.GUN_SC_PUB, process.env.GUN_SC_PRIV)
        .then(() => {
          // tslint:disable-next-line: no-console
          console.log(`Logged in as ${process.env.GUN_SC_PUB}`)
          // tslint:disable-next-line: no-console
          console.log('socket id', this.socket.socket!.id)
        })
        // tslint:disable-next-line: no-console
        .catch(err => console.error('Error logging in:', err.stack || err))
    )
  }

  protected publishDiff(msg: GunMsg): void {
    const msgId = msg['#']
    const diff = msg.put
    if (!diff) {
      return
    }
    this.socket.publishToChannel('gun/put/diff', msg)
    const souls = Object.keys(diff)

    if (souls.length === 1) {
      // No need to split into multiple messages
      const soul = souls[0]
      if (!diff[soul]) {
        return
      }
      this.socket.publishToChannel(`gun/nodes/${soul}`, msg)
      return
    }

    for (const soul of souls) {
      const nodeDiff = diff[soul]
      if (!nodeDiff) {
        continue
      }
      this.socket.publishToChannel(`gun/nodes/${soul}`, {
        '#': `${msgId}/${soul}`,
        put: {
          [soul]: nodeDiff
        }
      })
    }
  }
}
