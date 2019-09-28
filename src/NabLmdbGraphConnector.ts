import LmdbGraphConnector from "@notabug/chaingun-lmdb"
import { Schema } from "@notabug/peer"

export class NabLmdbGraphConnector extends LmdbGraphConnector {
  constructor(lmdbOpts?: any) {
    super(lmdbOpts)
  }

  eachThingId(cb: (id: string) => void) {
    const txn = this._client.env.beginTxn({ readOnly: true })
    const cursor = new this._client.lmdb.Cursor(txn, this._client.dbi)
    let soul = cursor.goToRange(`nab/things/`)
    let count = 0
    while (soul) {
      const match = Schema.Thing.route.match(soul)
      if (match && match.thingId) cb(match.thingId)
      soul = cursor.goToNext()
      count++
    }
    console.log(`found ${count} ids`)
    txn.abort()
  }

  readNode(soul: string) {
    return this._client.get(soul)
  }

  async eachThingIdAsync(cb: (id: string) => Promise<void>) {
    const txn = this._client.env.beginTxn({ readOnly: true })
    const cursor = new this._client.lmdb.Cursor(txn, this._client.dbi)
    let soul = cursor.goToRange(`nab/things/`)
    let count = 0
    while (soul) {
      const match = Schema.Thing.route.match(soul)
      if (match && match.thingId) await cb(match.thingId)
      soul = cursor.goToNext()
      count++
    }
    console.log(`found ${count} ids`)
    txn.abort()
  }
}
