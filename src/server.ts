import { createServer } from '@chaingun/http-server'
import { createGraphAdapter } from '@chaingun/lmdb-adapter'
import { GunGraphAdapter, GunGraphData } from '@chaingun/types'
import { Validation } from '@notabug/nab-wire-validation'
import Gun from 'gun'

const DEFAULT_OPTS = {
  mapSize: 1024 ** 4,
  path: './data'
}

const suppressor = Validation.createSuppressor(Gun)

export function makeValidationAdapter(
  adapter: GunGraphAdapter,
  validate: (graph: GunGraphData) => Promise<boolean>
): GunGraphAdapter {
  return {
    ...adapter,
    put: (graph: GunGraphData) => {
      return validate(graph).then(isValid => {
        if (isValid) {
          return adapter.put(graph)
        }

        throw new Error('Invalid graph data')
      })
    },
    putSync: undefined
  }
}

// tslint:disable-next-line: variable-name
export async function validateGraph(graph: GunGraphData): Promise<boolean> {
  return suppressor.validate({
    '#': 'dummymsgid',
    put: graph
  })
}

export function makeServer(opts = DEFAULT_OPTS): Express.Application {
  return createServer(
    makeValidationAdapter(createGraphAdapter(opts), validateGraph)
  )
}
