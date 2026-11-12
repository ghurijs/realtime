// In-process rooms: Node (one process) and tests. Workers need durableObjectLayer().
import {
  createContext,
  type Env,
  type ExecutionContextLike,
  type GhuriSettings,
  getContext,
  ImproperlyConfigured,
  runWithContext,
  tryGetContext,
} from '@ghuri/core'
import type { ConsumerClass } from './consumer'
import { RealtimeError } from './errors'
import { type HostConnection, type HostDriver, RoomHost } from './host'
import type { ConnectArgs, LayerConfig, RealtimeLayer } from './settings'
import type { ConnectionRecord, Room, RoomStorage } from './types'
import { BufferedSocket, pendingUpgrades } from './upgrade'

interface MemoryConnection extends HostConnection {
  cls: ConsumerClass
  initkwargs: Record<string, unknown>
  env: Env
  settings: GhuriSettings
  executionCtx?: ExecutionContextLike
}

export function memoryStorage(map: Map<string, unknown> = new Map()): RoomStorage {
  return {
    async get(key) {
      return map.has(key) ? structuredClone(map.get(key) as any) : undefined
    },
    async put(key, value) {
      map.set(key, structuredClone(value))
    },
    async delete(key) {
      return map.delete(key)
    },
    async list(opts = {}) {
      const keys = [...map.keys()].filter((k) => !opts.prefix || k.startsWith(opts.prefix)).sort()
      const out = new Map<string, any>()
      for (const k of keys.slice(0, opts.limit ?? keys.length)) out.set(k, structuredClone(map.get(k)))
      return out
    },
  }
}

class MemoryDriver implements HostDriver {
  readonly conns = new Map<string, MemoryConnection>()
  readonly storage: RoomStorage

  constructor(
    readonly name: string,
    data: Map<string, unknown>,
    private readonly onEmpty: () => void,
  ) {
    this.storage = memoryStorage(data)
  }

  list(): HostConnection[] {
    return [...this.conns.values()]
  }

  remove(conn: HostConnection): void {
    this.conns.delete(conn.record.scope.connectionId)
    if (this.conns.size === 0) this.onEmpty()
  }

  async consumerFor(conn: HostConnection) {
    const c = conn as MemoryConnection
    return { cls: c.cls, initkwargs: c.initkwargs }
  }

  run<T>(conn: HostConnection | undefined, fn: () => Promise<T>): Promise<T> {
    if (!conn) {
      if (tryGetContext()) return fn()
      throw new RealtimeError('Room code needs a Ghuri context (run it inside a request or app.run()).')
    }
    const c = conn as MemoryConnection
    return runWithContext(
      createContext({ env: c.env, settings: c.settings, executionCtx: c.executionCtx }),
      fn,
    )
  }
}

interface Registry {
  hosts: Map<string, RoomHost>
  data: Map<string, Map<string, unknown>>
}

const REGISTRIES = Symbol.for('ghuri.realtime.memoryRegistries')
function registry(name: string): Registry {
  const g = globalThis as any
  if (!g[REGISTRIES]) g[REGISTRIES] = new Map<string, Registry>()
  const all: Map<string, Registry> = g[REGISTRIES]
  let r = all.get(name)
  if (!r) {
    r = { hosts: new Map(), data: new Map() }
    all.set(name, r)
  }
  return r
}

class MemoryLayer implements RealtimeLayer {
  readonly backend = 'memory'
  private readonly reg: Registry

  constructor(name: string) {
    this.reg = registry(name)
  }

  host(name: string): RoomHost {
    let host = this.reg.hosts.get(name)
    if (!host) {
      let data = this.reg.data.get(name)
      if (!data) {
        data = new Map()
        this.reg.data.set(name, data)
      }
      const driver = new MemoryDriver(name, data, () => {
        if (this.reg.hosts.get(name) === host) this.reg.hosts.delete(name)
      })
      host = new RoomHost(driver)
      this.reg.hosts.set(name, host)
    }
    return host
  }

  room(name: string): Room {
    return this.host(name).room
  }

  async connect({ request, scope, consumer, initkwargs }: ConnectArgs): Promise<Response> {
    const host = this.host(scope.room)
    const driver = host.driver as MemoryDriver
    const ctx = getContext()
    const pending = pendingUpgrades.get(request)
    if (typeof (globalThis as any).WebSocketPair === 'function') {
      // Workers forbid using a socket from another request's handler, so in-isolate rooms cannot fan out.
      throw new ImproperlyConfigured(
        "memoryLayer() can't host rooms on Cloudflare Workers. Use REALTIME.LAYER = durableObjectLayer('REALTIME') " +
          'and export createRoomClass(app) from your Worker entry (wrangler dev runs Durable Objects locally).',
      )
    }
    if (!pending) {
      return new Response(
        'WebSockets are not wired into this server. Call attachRealtime(server, app) from @ghuri/realtime/node.',
        { status: 426, headers: { 'content-type': 'text/plain; charset=utf-8', Upgrade: 'websocket' } },
      )
    }

    const open = (record: ConnectionRecord): HostConnection => {
      const conn: MemoryConnection = {
        record,
        socket: new BufferedSocket(),
        save: () => {},
        cls: consumer,
        initkwargs,
        env: ctx.env,
        settings: ctx.settings,
      }
      driver.conns.set(record.scope.connectionId, conn)
      return conn
    }

    const outcome = await host.handshake(scope, consumer, initkwargs, open)
    if (!outcome.accepted || !outcome.conn) {
      return new Response('WebSocket connection rejected.', {
        status: 403,
        headers: { 'content-type': 'text/plain; charset=utf-8' },
      })
    }
    const conn = outcome.conn
    const headers: Record<string, string> = {}
    if (outcome.subprotocol) headers['Sec-WebSocket-Protocol'] = outcome.subprotocol

    pending.accept(outcome.subprotocol, (transport) => {
      ;(conn.socket as BufferedSocket).attach(transport)
      return {
        message: (data) => host.receive(conn, data),
        close: (code, reason) => host.disconnect(conn, code, reason),
      }
    })
    return new Response(null, { status: 200, headers })
  }
}

/** In-process rooms: one Node process and tests. On Workers use `durableObjectLayer()`. */
export function memoryLayer(opts: { name?: string } = {}): LayerConfig {
  const name = opts.name ?? 'default'
  return { backend: 'memory', create: () => new MemoryLayer(name) }
}
