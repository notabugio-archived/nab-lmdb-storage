import { makeServer } from './server'

const PORT = parseInt(process.env.GUN_HTTP_PORT || '', 10) || 5555
const HOST = process.env.GUN_HTTP_HOST || '127.0.0.1'

const server = makeServer()
// @ts-ignore
server.listen(PORT, HOST)
