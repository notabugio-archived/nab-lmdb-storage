import { NabLmdbStorage } from "./NabLmdbStorage"

const gun = new NabLmdbStorage()
setTimeout(() => gun.uploadThings(), 1000)
