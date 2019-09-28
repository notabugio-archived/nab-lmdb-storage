import { ChainGun, GunGraph, diffGunCRDT } from "@notabug/chaingun"
import { NabLmdbGraphConnector } from "./NabLmdbGraphConnector"
import SocketClusterGraphConnector from "@notabug/chaingun-socket-cluster-connector"
import WS from "ws"
import { Schema } from "@notabug/peer"

interface Opts extends GunChainOptions {
  socketCluster: any
  enforceCRDT?: boolean
  lmdb?: any
}

const DEFAULT_OPTS: Opts = {
  WS,
  peers: [],
  socketCluster: {
    hostname: "127.0.0.1",
    port: 4444,
    autoReconnect: true,
    autoReconnectOptions: {
      initialDelay: 1,
      randomness: 100,
      maxDelay: 500
    }
  },
  enforceCRDT: false,
  lmdb: {
    path: "./data",
    mapSize: 1024 ** 4
  }
}

const promiseNothing = Promise.resolve(undefined)

export class Gun extends ChainGun {
  lmdb: NabLmdbGraphConnector
  socket: SocketClusterGraphConnector

  constructor(options = DEFAULT_OPTS) {
    const { enforceCRDT, socketCluster: scOpts, lmdb: lmdbOpts, ...opts } = {
      ...DEFAULT_OPTS,
      ...options
    }

    const graph = new GunGraph()
    const lmdb = new NabLmdbGraphConnector(lmdbOpts)
    const socket = new SocketClusterGraphConnector(options.socketCluster)

    if (enforceCRDT) {
      graph.use(diffGunCRDT)
      graph.use(diffGunCRDT, "write")
    }

    graph.connect(socket)
    graph.connect(lmdb)

    super({ graph, ...opts })

    this.lmdb = lmdb
    this.socket = socket

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
          this.socket.subscribeToChannel("gun/get/validated")
          this.socket.subscribeToChannel("gun/put/validated")
        })
        .catch(err => console.error("Error logging in:", err.stack || err))
    }
  }

  respondToGets() {
    this.socket.events.receiveMessage.on(msg => {
      if (msg && !msg.get && !msg.put) console.log("msg", msg)
      if (!msg) return
      const msgId = msg["#"]
      const soul = (msg.get && msg.get["#"]) || ""
      if (!soul) return
      this.lmdb.get({
        soul,
        msgId,
        cb: (m: GunMsg) => this.socket.send([m])
      })
    })
  }

  persistIncoming() {
    this.graph.graphData.on((data, msgId) => {
      if (!data) return
      const putGraph: GunPut = {}
      const souls = Object.keys(data)

      for (let i = 0; i < souls.length; i++) {
        const soul = souls[i]
        const node = data[soul]
        if (node) putGraph[soul] = node
      }

      if (Object.keys(putGraph).length) {
        this.lmdb.put({
          msgId,
          graph: putGraph,
          cb: m => m && this.socket.send([m])
        })
      }
    })
  }

  uploadThing(thingId: string): Promise<void> {
    const thingSoul = Schema.Thing.route.reverse({
      thingId
    })
    const thing = this.lmdb.readNode(thingSoul)
    if (!thing) {
      return promiseNothing
    }
    const dataSoul = thing.data && thing.data["#"]
    if (!dataSoul) return promiseNothing
    const thingData = this.lmdb.readNode(dataSoul)
    if (!thingData) return promiseNothing

    return new Promise(ok => {
      const cb = (ack: any) => {
        if (this.socket.isConnected) console.log("uploaded", thingId, ack)
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

      setTimeout(() => cb(false), 10)
    })
  }

  uploadThings() {
    this.lmdb.eachThingIdAsync(this.uploadThing.bind(this))
  }
}
