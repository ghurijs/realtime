// The Durable Object room class against a fake hibernation runtime: a second instance over the same sockets
// stands in for an object woken from hibernation (no memory, only attachments and storage survive).
import { createApp, defineSettings, path } from '@ghuri/core'
import { beforeAll, describe, expect, it } from 'vitest'
import { createRoomClass, durableObjectLayer } from '../src/cloudflare'
import { JsonWebSocketConsumer, MAX_RECORD_BYTES, type Scope } from '../src/index'

class FakeSocket {
  readyState = 1
  sent: string[] = []
  closedWith?: { code?: number; reason?: string }
  private attachment: unknown = null
  constructor(readonly peer?: FakeSocket) {}
  send(data: string) {
    if (this.readyState !== 1) throw new Error('socket not open')
    this.sent.push(data)
  }
  close(code?: number, reason?: string) {
    this.closedWith = { code, reason }
    this.readyState = 3
  }
  serializeAttachment(v: unknown) {
    const text = JSON.stringify(v)
    if (new TextEncoder().encode(text).byteLength > MAX_RECORD_BYTES) throw new Error('attachment too large')
    this.attachment = JSON.parse(text)
  }
  deserializeAttachment() {
    return this.attachment === null ? null : structuredClone(this.attachment)
  }
}

class FakeState {
  sockets: FakeSocket[] = []
  data = new Map<string, unknown>()
  waited: Promise<unknown>[] = []
  autoResponse: unknown
  storage = {
    get: async (k: string) => this.data.get(k),
    put: async (k: string, v: unknown) => void this.data.set(k, structuredClone(v)),
    delete: async (k: string) => this.data.delete(k),
    list: async ({ prefix = '' }: { prefix?: string } = {}) =>
      new Map([...this.data].filter(([k]) => k.startsWith(prefix)).sort(([a], [b]) => a.localeCompare(b))),
  }
  acceptWebSocket(ws: FakeSocket) {
    this.sockets.push(ws)
  }
  getWebSockets() {
    return this.sockets
  }
  setWebSocketAutoResponse(pair: unknown) {
    this.autoResponse = pair
  }
  waitUntil(p: Promise<unknown>) {
    this.waited.push(p)
  }
}

const g = globalThis as any
const realResponse = g.Response

class Counter extends JsonWebSocketConsumer {
  static override room = (s: Scope) => `count.${s.params.name}`
  override async connect() {
    this.accept()
    this.state.n = 0
  }
  override async receiveJson(data: any) {
    this.state.n = (this.state.n as number) + 1
    if (data.tag) this.tag(data.tag)
    this.sendJson({ n: this.state.n, user: this.user.username, room: this.room.name, tags: this.tags })
  }
  async ping(e: any) {
    this.sendJson({ pong: e.v, n: this.state.n })
  }
  override async disconnect(code: number) {
    await this.room.storage.put('lastClose', code)
  }
}

describe('Durable Object room class across hibernation', () => {
  const app = createApp(
    defineSettings(() => ({
      SECRET_KEY: 'x',
      INSTALLED_APPS: [],
      ROOT_URLCONF: [path('ws/<slug:name>/', Counter.asView())],
      REALTIME: { LAYER: durableObjectLayer('REALTIME') },
    })),
    { checks: false },
  )
  const Room = createRoomClass(app)

  beforeAll(() => {
    g.WebSocketPair = class {
      0: FakeSocket
      1: FakeSocket
      constructor() {
        const server = new FakeSocket()
        const client = new FakeSocket(server)
        this[0] = client
        this[1] = server
      }
    }
    g.WebSocketRequestResponsePair = class {
      constructor(
        readonly request: string,
        readonly response: string,
      ) {}
    }
    // Node's Response rejects status 101; workerd allows it with a webSocket.
    g.Response = class extends realResponse {
      webSocket: unknown
      constructor(body: any, init: any = {}) {
        super(body, init.status === 101 ? { ...init, status: 200 } : init)
        this.webSocket = init.webSocket
        if (init.status === 101) Object.defineProperty(this, 'status', { value: 101 })
      }
    }
  })

  function handshake(state: FakeState, name: string, scopeOverrides: Partial<Scope> = {}) {
    const scope: Scope = {
      path: `/ws/${name}/`,
      params: { name },
      query: {},
      room: `count.${name}`,
      connectionId: `count.${name}:${crypto.randomUUID()}`,
      headers: { subprotocols: [] },
      user: { pk: 7, username: 'ann', isAuthenticated: true },
      ...scopeOverrides,
    }
    const room = new Room(state, {})
    const req = new Request('https://example.com/ws/x/', {
      headers: { upgrade: 'websocket', 'x-ghuri-scope': encodeURIComponent(JSON.stringify(scope)) },
    })
    return { room, scope, res: room.fetch(req) }
  }

  it('rebuilds consumers from attachments after the object is recreated', async () => {
    const state = new FakeState()
    const { room, res } = handshake(state, 'a')
    expect((await res).status).toBe(101)
    expect(state.autoResponse).toMatchObject({ request: 'ping', response: 'pong' })
    const server = state.sockets[0]
    await room.webSocketMessage(server, JSON.stringify({ tag: 'blue' }))
    expect(JSON.parse(server.sent.at(-1)!)).toEqual({ n: 1, user: 'ann', room: 'count.a', tags: ['blue'] })

    // "Hibernation": a new instance with no memory over the same sockets.
    const woken = new Room(state, {})
    await woken.webSocketMessage(server, JSON.stringify({}))
    expect(JSON.parse(server.sent.at(-1)!)).toEqual({ n: 2, user: 'ann', room: 'count.a', tags: ['blue'] })
    await woken.groupSend('count.a', { type: 'ping', v: 1 }, { tag: 'blue' })
    expect(JSON.parse(server.sent.at(-1)!)).toEqual({ pong: 1, n: 2 })
    expect(await woken.connections('count.a', { tag: 'blue' })).toHaveLength(1)

    await woken.webSocketClose(server, 1001, 'away')
    expect(server.closedWith).toEqual({ code: 1001, reason: 'away' })
    expect(await woken.storageGet('lastClose')).toBe(1001)
    expect(await woken.connections('count.a')).toHaveLength(0)
  })

  it('rejects handshakes that do not come from the layer and oversized scopes', async () => {
    const state = new FakeState()
    const room = new Room(state, {})
    expect((await room.fetch(new Request('https://example.com/ws/a/'))).status).toBe(400)
    const { res } = handshake(state, 'big', { query: { blob: 'x'.repeat(3000) } })
    await expect(res).rejects.toThrow(/2048/)
    expect(state.sockets).toHaveLength(0)
  })

  it('keeps room storage under its own prefix', async () => {
    const state = new FakeState()
    const room = new Room(state, {})
    await room.storagePut('a', { x: 1 })
    await room.storagePut('b', 2)
    expect([...state.data.keys()]).toEqual(['room:a', 'room:b'])
    expect(await room.storageList({ prefix: 'a' })).toEqual(new Map([['a', { x: 1 }]]))
    expect(await room.storageDelete('a')).toBe(true)
  })
})
