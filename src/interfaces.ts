interface LmdbOptions {
  path: string
  mapSize?: number
}

interface GunNodeState {
  [key: string]: number
}

interface GunGraphData {
  [key: string]: GunNode | undefined
}

type GunValue = object | string | number | boolean | null
type GunChainOptions = any
type SendFn = (msg: GunMsg) => void
type GunOnCb = (node: GunValue | undefined, key?: string) => void
type GunMsgCb = (msg: GunMsg) => void
type GunNodeListenCb = (node: GunNode | undefined) => void

interface PathData {
  souls: string[]
  value: GunValue | undefined
  complete: boolean
}

type ChainGunMiddleware = (
  updates: GunGraphData,
  existingGraph: GunGraphData
) => GunGraphData | undefined | Promise<GunGraphData | undefined>
type ChainGunMiddlewareType = "read" | "write"

interface GunNode {
  _: {
    "#": string
    ">": GunNodeState
  }
  [key: string]: any
}

interface GunPut {
  [soul: string]: GunNode
}

interface GunMsg {
  "#"?: string
  "@"?: string

  get?: {
    "#": string
  }

  put?: GunGraphData | null

  ok?: number | boolean
  ack?: number | boolean
  err?: any
}
