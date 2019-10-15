import { NabLmdbStorage } from './NabLmdbStorage'

const storage = new NabLmdbStorage()
storage.authenticate().then(() => {
  storage.persistIncoming()
  storage.respondToGets()
})
