import { NabLmdbStorage } from './NabLmdbStorage'

const args = process.argv.slice(2)
const gun = new NabLmdbStorage()
gun.scanThings(args[0])