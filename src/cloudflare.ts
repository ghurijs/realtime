// @ghuri/realtime/cloudflare — rooms as Durable Objects, with the WebSocket Hibernation API.
//
//   // src/index.ts
//   const app = createApp(settings)
//   export default app
//   export const GhuriRoom = createRoomClass(app)
//
//   // wrangler.jsonc
//   "durable_objects": { "bindings": [{ "name": "REALTIME", "class_name": "GhuriRoom" }] },
//   "migrations": [{ "tag": "realtime-v1", "new_sqlite_classes": ["GhuriRoom"] }]
import { DurableObject } from 'cloudflare:workers'
import { type Env, type GhuriApp, ImproperlyConfigured, resolve } from '@ghuri/core'
import type { ConsumerClass } from './consumer'
import { RealtimeError } from './errors'
import { type HostConnection, type HostDriver, RoomHost } from './host'
import type { ConnectArgs, LayerConfig, RealtimeLayer } from './settings'
import type {
  BroadcastOptions,
  ConnectionInfo,
  ConnectionRecord,
  GroupEvent,
  Room,
  RoomStorage,
} from './types'

const SCOPE_HEADER = 'x-ghuri-scope'
const STORAGE_PREFIX = 'room:'
const WS_OPEN = 1

/** Consumers (and other objects) can't cross RPC; keep only the connection id. */
function rpcOptions(opts?: BroadcastOptions): { tag?: string; exclude?: string } | undefined {
  if (!opts) return undefined
  const out: { tag?: string; exclude?: string } = {}
  if (opts.tag !== undefined) out.tag = opts.tag
  if (opts.exclude !== undefined)
    out.exclude = typeof opts.exclude === 'string' ? opts.exclude : opts.exclude.scope.connectionId
  return out
}

/** The RPC surface of the room object (what the layer calls from Workers and other rooms). */
export interface RoomStub {
  fetch(request: Request): Promise<Response>
  groupSend(room: string, event: GroupEvent, opts?: { tag?: string; exclude?: string }): Promise<void>
  broadcast(room: string, message: unknown, opts?: { tag?: string; exclude?: string }): Promise<void>
  sendTo(room: string, connectionId: string, event: GroupEvent): Promise<void>
  connections(room: string, opts?: { tag?: string }): Promise<ConnectionInfo[]>
  storageGet(key: string): Promise<unknown>
  storagePut(key: string, value: unknown): Promise<void>
  storageDelete(key: string): Promise<boolean>
  storageList(opts?: { prefix?: string; limit?: number }): Promise<Map<string, unknown>>
}

/** The room Durable Object: RPC surface plus the Hibernation API handlers the runtime calls. */
export interface RoomObject extends RoomStub {
  webSocketMessage(ws: unknown, message: string | ArrayBuffer): Promise<void>
  webSocketClose(ws: unknown, code: number, reason: string, wasClean?: boolean): Promise<void>
  webSocketError(ws: unknown, error?: unknown): Promise<void>
}

class DurableObjectLayer implements RealtimeLayer {
  readonly backend = 'durable-object'

  constructor(private readonly ns: any) {}

  private stub(name: string): RoomStub {
    return this.ns.get(this.ns.idFromName(name)) as RoomStub
  }

  async connect({ request, scope }: ConnectArgs): Promise<Response> {
    const headers = new Headers(request.headers)
    headers.set(SCOPE_HEADER, encodeURIComponent(JSON.stringify(scope)))
    const res = await this.stub(scope.room).fetch(new Request(request.url, { method: 'GET', headers }))
    const ws = (res as any).webSocket
    if (res.status !== 101 || !ws) return res
    // The room's response has immutable headers; give MIDDLEWARE a mutable copy that keeps the socket.
    return new Response(null, { status: 101, webSocket: ws, headers: res.headers } as ResponseInit)
  }

  room(name: string): Room {
    const stub = () => this.stub(name)
    const storage: RoomStorage = {
      get: async (key) => (await stub().storageGet(key)) as any,
      put: (key, value) => stub().storagePut(key, value),
      delete: (key) => stub().storageDelete(key),
      list: async (opts) => (await stub().storageList(opts)) as Map<string, any>,
    }
    return {
      name,
      groupSend: (event, opts) => stub().groupSend(name, event, rpcOptions(opts)),
      broadcast: (message, opts) => stub().broadcast(name, message, rpcOptions(opts)),
      send: (connectionId, event) => stub().sendTo(name, connectionId, event),
      connections: (opts) => stub().connections(name, opts),
      count: async (opts) => (await stub().connections(name, opts)).length,
      storage,
    }
  }
}

/** Rooms hosted by the Durable Object bound as `binding` (default `REALTIME`); see `createRoomClass`. */
export function durableObjectLayer(binding = 'REALTIME'): LayerConfig {
  return {
    backend: 'durable-object',
    create(env: Env) {
      const ns = env[binding]
      if (!ns || typeof ns.idFromName !== 'function') {
        throw new ImproperlyConfigured(
          `REALTIME.LAYER uses durableObjectLayer('${binding}') but env.${binding} is not a Durable Object ` +
            'namespace. Add the binding to wrangler.jsonc and export createRoomClass(app) from your entry file.',
        )
      }
      return new DurableObjectLayer(ns)
    },
  }
}

function consumerFromPath(path: string): { cls: ConsumerClass; initkwargs: Record<string, unknown> } {
  const view = resolve(path).view as
    | (((c: unknown) => unknown) & { consumerClass?: ConsumerClass; initkwargs?: Record<string, unknown> })
    | undefined
  if (!view?.consumerClass) throw new RealtimeError(`No WebSocket consumer is routed at '${path}'.`)
  return { cls: view.consumerClass, initkwargs: view.initkwargs ?? {} }
}

function storageFor(storage: any): RoomStorage {
  return {
    get: (key) => storage.get(STORAGE_PREFIX + key),
    put: (key, value) => storage.put(STORAGE_PREFIX + key, value),
    delete: (key) => storage.delete(STORAGE_PREFIX + key),
    async list(opts = {}) {
      const raw: Map<string, any> = await storage.list({
        prefix: STORAGE_PREFIX + (opts.prefix ?? ''),
        limit: opts.limit,
      })
      const out = new Map<string, any>()
      for (const [k, v] of raw) out.set(k.slice(STORAGE_PREFIX.length), v)
      return out
    },
  }
}

/**
 * The Durable Object class that hosts rooms. Export it from the Worker entry next to the app:
 * `export const GhuriRoom = createRoomClass(app)`.
 */
export function createRoomClass(app: GhuriApp): new (ctx: any, env: Env) => DurableObject<Env> & RoomObject {
  if (typeof (app as any).run !== 'function') {
    throw new ImproperlyConfigured(
      '@ghuri/realtime needs a @ghuri/core release with app.run(); upgrade @ghuri/core.',
    )
  }

  class GhuriRoom extends DurableObject<Env> implements RoomObject {
    private host?: RoomHost
    private readonly conns = new WeakMap<object, HostConnection>()
    private readonly roomStorage: RoomStorage

    constructor(ctx: any, env: Env) {
      super(ctx, env)
      this.roomStorage = storageFor(ctx.storage)
      const Pair = (globalThis as any).WebSocketRequestResponsePair
      if (Pair) {
        try {
          // Keep-alive pings are answered without waking the object.
          ctx.setWebSocketAutoResponse(new Pair('ping', 'pong'))
        } catch {
          // older runtimes
        }
      }
    }

    private conn(ws: any): HostConnection | undefined {
      let conn = this.conns.get(ws)
      if (!conn) {
        const record = ws.deserializeAttachment() as ConnectionRecord | null
        if (!record) return undefined
        conn = { record, socket: ws, save: (r) => ws.serializeAttachment(r) }
        this.conns.set(ws, conn)
      }
      return conn
    }

    private hostFor(name: string): RoomHost {
      if (this.host) return this.host
      const self = this
      const driver: HostDriver = {
        name,
        storage: this.roomStorage,
        list() {
          const out: HostConnection[] = []
          for (const ws of self.ctx.getWebSockets()) {
            if (ws.readyState !== WS_OPEN) continue
            const c = self.conn(ws)
            if (c) out.push(c)
          }
          return out
        },
        remove() {
          // The runtime drops closed sockets from getWebSockets().
        },
        run: (_conn, fn) => app.run(self.env, self.ctx, () => fn()),
        consumerFor: async (conn) => consumerFromPath(conn.record.scope.path),
      }
      this.host = new RoomHost(driver)
      return this.host
    }

    async fetch(request: Request): Promise<Response> {
      const raw = request.headers.get(SCOPE_HEADER)
      if (!raw || (request.headers.get('upgrade') ?? '').toLowerCase() !== 'websocket') {
        return new Response('Expected a WebSocket handshake from the Ghuri realtime layer.', { status: 400 })
      }
      const scope = JSON.parse(decodeURIComponent(raw))
      const host = this.hostFor(scope.room)
      let client: any
      const outcome = await app.run(this.env, this.ctx, async () => {
        const { cls, initkwargs } = consumerFromPath(scope.path)
        return host.handshake(scope, cls, initkwargs, (record) => {
          const pair = new (globalThis as any).WebSocketPair()
          client = pair[0]
          const server = pair[1]
          this.ctx.acceptWebSocket(server)
          server.serializeAttachment(record)
          const conn: HostConnection = { record, socket: server, save: (r) => server.serializeAttachment(r) }
          this.conns.set(server, conn)
          return conn
        })
      })
      if (!outcome.accepted || !client) {
        return new Response('WebSocket connection rejected.', {
          status: 403,
          headers: { 'content-type': 'text/plain; charset=utf-8' },
        })
      }
      const headers: Record<string, string> = {}
      if (outcome.subprotocol) headers['Sec-WebSocket-Protocol'] = outcome.subprotocol
      return new Response(null, { status: 101, webSocket: client, headers } as ResponseInit)
    }

    async webSocketMessage(ws: any, message: string | ArrayBuffer): Promise<void> {
      const conn = this.conn(ws)
      if (conn) await this.hostFor(conn.record.scope.room).receive(conn, message)
    }

    async webSocketClose(ws: any, code: number, reason: string): Promise<void> {
      try {
        ws.close(code, reason)
      } catch {
        try {
          ws.close()
        } catch {
          // already closed
        }
      }
      const conn = this.conn(ws)
      if (conn) await this.hostFor(conn.record.scope.room).disconnect(conn, code, reason)
    }

    async webSocketError(ws: any): Promise<void> {
      const conn = this.conn(ws)
      if (conn) await this.hostFor(conn.record.scope.room).disconnect(conn, 1006, 'error')
    }

    // ---- RPC (called by durableObjectLayer().room(name)) ----

    groupSend(room: string, event: GroupEvent, opts?: { tag?: string; exclude?: string }): Promise<void> {
      return this.hostFor(room).groupSend(event, opts)
    }

    broadcast(room: string, message: unknown, opts?: { tag?: string; exclude?: string }): Promise<void> {
      return this.hostFor(room).broadcast(message, opts)
    }

    sendTo(room: string, connectionId: string, event: GroupEvent): Promise<void> {
      return this.hostFor(room).sendTo(connectionId, event)
    }

    async connections(room: string, opts?: { tag?: string }): Promise<ConnectionInfo[]> {
      return this.hostFor(room).connections(opts)
    }

    storageGet(key: string): Promise<unknown> {
      return this.roomStorage.get(key)
    }

    storagePut(key: string, value: unknown): Promise<void> {
      return this.roomStorage.put(key, value)
    }

    storageDelete(key: string): Promise<boolean> {
      return this.roomStorage.delete(key)
    }

    storageList(opts?: { prefix?: string; limit?: number }): Promise<Map<string, unknown>> {
      return this.roomStorage.list(opts)
    }
  }
  return GhuriRoom as any
}
