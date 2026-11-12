// Consumers on the memory layer, end to end through createApp + MIDDLEWARE + @ghuri/auth.
import {
  AuthConfig,
  AuthenticationMiddleware,
  AuthTokenConfig,
  ContentType,
  ContentTypesConfig,
  PBKDF2PasswordHasher,
  SessionMiddleware,
  SessionsConfig,
  Token,
  User,
} from '@ghuri/auth'
import { TokenAuthentication } from '@ghuri/auth/rest'
import {
  CommonMiddleware,
  CsrfMiddleware,
  createApp,
  defineSettings,
  type GhuriApp,
  getUser,
  json,
  path,
  SecurityMiddleware,
} from '@ghuri/core'
import { createTestClient } from '@ghuri/core/testing'
import { setupTestDatabase, type TestDatabase } from '@ghuri/orm/testing'
import { IsAuthenticated } from '@ghuri/rest'
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { channelLayer, JsonWebSocketConsumer, memoryLayer, type Scope, WebSocketConsumer } from '../src/index'
import { CommunicatorClosed, WebSocketCommunicator } from '../src/testing'

const log: string[] = []

class EchoConsumer extends WebSocketConsumer {
  override async receive(m: { text?: string; bytes?: Uint8Array }) {
    if (m.text !== undefined) this.send(`echo:${m.text}`)
    else this.send(m.bytes!)
  }
  override async disconnect(code: number) {
    log.push(`echo-disconnect:${code}`)
  }
}

class ChatConsumer extends JsonWebSocketConsumer {
  static override room = (scope: Scope) => `chat.${scope.params.room}`

  override async connect() {
    this.accept()
    this.state.count = 0
    await this.room.groupSend({ type: 'chat.joined', who: this.user.username || 'anon' }, { exclude: this })
  }
  override async receiveJson(data: any) {
    this.state.count = (this.state.count as number) + 1
    if (data.cmd === 'count') return this.sendJson({ count: this.state.count })
    if (data.cmd === 'tag') return this.tag(data.tag)
    if (data.cmd === 'big') {
      this.state.blob = 'x'.repeat(4000)
      return
    }
    if (data.cmd === 'boom') throw new Error('boom')
    if (data.cmd === 'who')
      return this.sendJson({ who: getUser().username, scopeUser: this.scope.user.username })
    if (data.cmd === 'perm') return this.sendJson({ perm: await this.user.hasPerm('auth.add_user') })
    if (data.cmd === 'refresh') return this.sendJson({ ok: await this.refreshUser() })
    if (data.cmd === 'presence')
      return this.sendJson({ n: await this.room.count(), tagged: await this.room.count({ tag: 'mod' }) })
    if (data.cmd === 'store') {
      await this.room.storage.put('topic', data.topic)
      return this.sendJson({ topic: await this.room.storage.get('topic') })
    }
    await this.room.groupSend({ type: 'chat.message', text: data.text, from: this.scope.connectionId })
  }
  async chatMessage(event: any) {
    this.sendJson({ type: 'chat.message', text: event.text })
  }
  async chatJoined(event: any) {
    this.sendJson({ type: 'chat.joined', who: event.who })
  }
}

class PrivateConsumer extends JsonWebSocketConsumer {
  static override permissionClasses = [IsAuthenticated]
  static override room = (scope: Scope) => (scope.user.isAuthenticated ? `user.${scope.user.pk}` : null)
  async notify(event: any) {
    this.sendJson({ note: event.text })
  }
}

class TokenConsumer extends JsonWebSocketConsumer {
  static override authenticationClasses = [TokenAuthentication]
  static override permissionClasses = [IsAuthenticated]
  override async connect() {
    this.accept()
    this.sendJson({ hello: this.user.username, query: this.scope.query })
  }
}

class PickyConsumer extends WebSocketConsumer {
  static override subprotocols = ['v2.chat', 'v1.chat']
  override async connect() {
    if (this.scope.query.reject) return this.close(4000, 'nope')
    if (this.scope.query.silent) return
    this.accept()
  }
}

/** A permission that reads the scope (URL kwargs), as `PermissionLike` documents. */
class NotLockedRoom {
  message = 'This room is locked.'
  hasPermission(request: { scope: Scope }) {
    return request.scope.params.room !== 'locked' && request.scope.path.startsWith('/ws/guarded/')
  }
}

class GuardedConsumer extends WebSocketConsumer {
  static override permissionClasses = [NotLockedRoom]
}

class NoRoomConsumer extends WebSocketConsumer {
  static override room = () => null
}

const urls = [
  path('ws/echo/', EchoConsumer.asView()),
  path('ws/chat/<slug:room>/', ChatConsumer.asView(), { name: 'chat' }),
  path('ws/me/', PrivateConsumer.asView()),
  path('ws/token/', TokenConsumer.asView()),
  path('ws/picky/', PickyConsumer.asView()),
  path('ws/none/', NoRoomConsumer.asView()),
  path('ws/guarded/<slug:room>/', GuardedConsumer.asView()),
  path('announce/<slug:room>/', async (c) => {
    await channelLayer.groupSend(`chat.${c.req.param('room')}`, { type: 'chat.message', text: 'from http' })
    return json(c, { ok: true })
  }),
]

describe('consumers on the memory layer', () => {
  let app: GhuriApp
  let db: TestDatabase
  let ann: User
  const env: Record<string, unknown> = {}
  const extra: Record<string, unknown> = { LAYER: memoryLayer({ name: 'consumers-test' }) }

  beforeAll(async () => {
    const settings = defineSettings(() => ({
      SECRET_KEY: 'test-secret',
      DEBUG: false,
      ALLOWED_HOSTS: ['testserver'],
      CSRF_TRUSTED_ORIGINS: ['https://*.trusted.example'],
      INSTALLED_APPS: [ContentTypesConfig, AuthConfig, SessionsConfig, AuthTokenConfig],
      MIDDLEWARE: [
        SecurityMiddleware,
        SessionMiddleware,
        CommonMiddleware,
        CsrfMiddleware,
        AuthenticationMiddleware,
      ],
      ROOT_URLCONF: urls,
      PASSWORD_HASHERS: [new PBKDF2PasswordHasher({ iterations: 1000 })],
      REALTIME: extra,
    }))
    app = createApp(settings, { checks: false })
    await app.setup(env)
    db = await setupTestDatabase({ using: 'sqlite' })
  }, 60_000)
  afterAll(async () => {
    await db?.teardown()
  })
  beforeEach(async () => {
    ContentType.objects.clearCache()
    await db.flush()
    ContentType.objects.clearCache()
    log.length = 0
    for (const k of Object.keys(extra)) if (k !== 'LAYER') delete extra[k]
    ann = await User.objects.createUser('ann', 'ann@example.com', 'pw')
  })

  const ws = (path: string, opts: ConstructorParameters<typeof WebSocketCommunicator>[2] = {}) =>
    new WebSocketCommunicator(app, path, { env, ...opts })

  it('answers 426 to plain HTTP requests', async () => {
    const res = await createTestClient(app, { env }).get('/ws/echo/')
    expect(res.status).toBe(426)
    expect(res.headers.get('upgrade')).toBe('websocket')
  })

  it('accepts, echoes text and bytes, and runs disconnect once', async () => {
    const c = ws('/ws/echo/')
    expect(await c.connect()).toMatchObject({ connected: true, status: 101 })
    await c.sendText('hi')
    expect(await c.receiveFrom()).toBe('echo:hi')
    await c.sendBytes(new Uint8Array([1, 2, 3]))
    expect(await c.receiveFrom()).toEqual(new Uint8Array([1, 2, 3]))
    await c.disconnect(1001)
    await c.disconnect(1001)
    expect(log).toEqual(['echo-disconnect:1001'])
  })

  it('fans out group events to a room and skips excluded connections', async () => {
    const a = ws('/ws/chat/lobby/')
    const b = ws('/ws/chat/lobby/', { user: ann })
    const other = ws('/ws/chat/kitchen/')
    await a.connect()
    await other.connect()
    await b.connect()
    expect(await a.receiveJson()).toEqual({ type: 'chat.joined', who: 'ann' })
    expect(await b.receiveNothing()).toBe(true)
    await a.sendJson({ text: 'hello' })
    expect(await a.receiveJson()).toEqual({ type: 'chat.message', text: 'hello' })
    expect(await b.receiveJson()).toEqual({ type: 'chat.message', text: 'hello' })
    expect(await other.receiveNothing()).toBe(true)
    for (const c of [a, b, other]) await c.disconnect()
  })

  it('reaches rooms from ordinary views through channelLayer', async () => {
    const a = ws('/ws/chat/news/')
    await a.connect()
    const res = await createTestClient(app, { env }).get('/announce/news/')
    expect(res.status).toBe(200)
    expect(await a.receiveJson()).toEqual({ type: 'chat.message', text: 'from http' })
    await channelLayer.groupSend('chat.nobody-here', { type: 'chat.message', text: 'x' })
    await a.disconnect()
  })

  it('keeps state between events and enforces the 2 KB record limit', async () => {
    const a = ws('/ws/chat/state/')
    await a.connect()
    await a.sendJson({ cmd: 'x', text: 'one' })
    await a.receiveJson()
    await a.sendJson({ cmd: 'count' })
    expect(await a.receiveJson()).toEqual({ count: 2 })
    await a.sendJson({ cmd: 'big' })
    expect(await a.receiveClose()).toEqual({ code: 1011, reason: 'Server error' })
  })

  it('closes with 1011 when a handler throws and 1007 on invalid JSON', async () => {
    const a = ws('/ws/chat/err/')
    await a.connect()
    await a.sendJson({ cmd: 'boom' })
    expect((await a.receiveClose()).code).toBe(1011)
    const b = ws('/ws/chat/err/')
    await b.connect()
    await b.sendText('{nope')
    expect(await b.receiveClose()).toEqual({ code: 1007, reason: 'Invalid JSON' })
    await expect(b.receiveFrom()).rejects.toBeInstanceOf(CommunicatorClosed)
  })

  it('exposes the handshake user, loads permissions through @ghuri/auth and refreshes users', async () => {
    const a = ws('/ws/chat/users/', { user: ann })
    await a.connect()
    await a.sendJson({ cmd: 'who' })
    expect(await a.receiveJson()).toEqual({ who: 'ann', scopeUser: 'ann' })
    await a.sendJson({ cmd: 'perm' })
    expect(await a.receiveJson()).toEqual({ perm: false })
    await a.sendJson({ cmd: 'refresh' })
    expect(await a.receiveJson()).toEqual({ ok: true })
    ann.isActive = false
    await ann.save()
    await a.sendJson({ cmd: 'refresh' })
    expect(await a.receiveClose()).toEqual({ code: 4401, reason: 'User is no longer active' })
  })

  it('revalidates users every REVALIDATE_SECONDS', async () => {
    extra.REVALIDATE_SECONDS = 0.001
    const a = ws('/ws/chat/reval/', { user: ann })
    await a.connect()
    await User.objects.filter({ pk: ann.pk }).delete()
    await new Promise((r) => setTimeout(r, 5))
    await a.sendJson({ cmd: 'count' })
    expect((await a.receiveClose()).code).toBe(4401)
  })

  it('tags, presence and room storage', async () => {
    const a = ws('/ws/chat/tags/')
    const b = ws('/ws/chat/tags/')
    await a.connect()
    await b.connect()
    await a.receiveJson() // b joined
    await b.sendJson({ cmd: 'tag', tag: 'mod' })
    await a.sendJson({ cmd: 'presence' })
    expect(await a.receiveJson()).toEqual({ n: 2, tagged: 1 })
    await channelLayer.room('chat.tags').broadcast({ only: 'mods' }, { tag: 'mod' })
    expect(await b.receiveJson()).toEqual({ only: 'mods' })
    expect(await a.receiveNothing()).toBe(true)
    await a.sendJson({ cmd: 'store', topic: 'cats' })
    expect(await a.receiveJson()).toEqual({ topic: 'cats' })
    expect(await channelLayer.room('chat.tags').storage.get('topic')).toBe('cats')
    const conns = await channelLayer.room('chat.tags').connections()
    expect(conns.map((c) => c.id.startsWith('chat.tags:'))).toEqual([true, true])
    await a.disconnect()
    await b.disconnect()
    expect(await channelLayer.room('chat.tags').count()).toBe(0)
  })

  it('checks permissions and per-user rooms on the handshake', async () => {
    const anon = await ws('/ws/me/').connect()
    expect(anon).toMatchObject({ connected: false, status: 401 })
    const me = ws('/ws/me/', { user: ann })
    expect((await me.connect()).connected).toBe(true)
    await channelLayer.groupSend(`user.${ann.pk}`, { type: 'notify', text: 'hi ann' })
    expect(await me.receiveJson()).toEqual({ note: 'hi ann' })
    await me.disconnect()
    expect(await ws('/ws/none/').connect()).toMatchObject({ connected: false, status: 403 })
  })

  it('passes the scope to permission classes', async () => {
    const open = ws('/ws/guarded/lobby/', { user: ann })
    expect((await open.connect()).connected).toBe(true)
    await open.disconnect()
    const locked = await ws('/ws/guarded/locked/', { user: ann }).connect()
    expect(locked).toMatchObject({ connected: false, status: 403 })
  })

  it('authenticates tokens sent as a subprotocol and echoes a subprotocol back', async () => {
    const token = await Token.objects.create({ user: ann })
    const bad = await ws('/ws/token/', { subprotocols: ['ghuri', 'bearer.wrong'] }).connect()
    expect(bad).toMatchObject({ connected: false, status: 401 })
    const none = await ws('/ws/token/').connect()
    expect(none).toMatchObject({ connected: false, status: 401 })
    extra.TOKEN_KEYWORD = 'Token'
    const c = ws('/ws/token/?x=1', { subprotocols: ['ghuri', `bearer.${token.key}`] })
    expect(await c.connect()).toMatchObject({ connected: true, subprotocol: 'ghuri' })
    expect(await c.receiveJson()).toEqual({ hello: 'ann', query: { x: '1' } })
    await c.disconnect()
  })

  it('accepts tokens from the query string only when enabled', async () => {
    const token = await Token.objects.create({ user: ann })
    extra.TOKEN_KEYWORD = 'Token'
    expect((await ws(`/ws/token/?token=${token.key}`).connect()).connected).toBe(false)
    extra.TOKEN_SOURCES = ['query']
    const c = ws(`/ws/token/?token=${token.key}`)
    expect((await c.connect()).connected).toBe(true)
    expect(await c.receiveJson()).toEqual({ hello: 'ann', query: {} })
    await c.disconnect()
  })

  it('negotiates subprotocols and rejects with 403 when connect() closes or does nothing', async () => {
    const c = ws('/ws/picky/', { subprotocols: ['v1.chat', 'v2.chat'] })
    expect(await c.connect()).toMatchObject({ connected: true, subprotocol: 'v2.chat' })
    await c.disconnect()
    expect(await ws('/ws/picky/?reject=1').connect()).toMatchObject({ connected: false, status: 403 })
    expect(await ws('/ws/picky/?silent=1').connect()).toMatchObject({ connected: false, status: 403 })
  })

  it('checks the Origin header against the host and trusted origins', async () => {
    const evil = await ws('/ws/echo/', { headers: { origin: 'https://evil.example' } }).connect()
    expect(evil).toMatchObject({ connected: false, status: 403 })
    for (const origin of ['http://testserver', 'https://app.trusted.example']) {
      const c = ws('/ws/echo/', { headers: { origin } })
      expect((await c.connect()).connected).toBe(true)
      await c.disconnect()
    }
    extra.ALLOWED_ORIGINS = ['https://evil.example']
    const allowed = ws('/ws/echo/', { headers: { origin: 'https://evil.example' } })
    expect((await allowed.connect()).connected).toBe(true)
    await allowed.disconnect()
  })

  it('refuses handshakes whose scope would not fit in a connection record', async () => {
    const res = await ws(`/ws/echo/?q=${'x'.repeat(3000)}`).connect()
    expect(res).toMatchObject({ connected: false, status: 431 })
  })

  it('closes oversized frames with 1009', async () => {
    extra.MAX_MESSAGE_BYTES = 8
    const c = ws('/ws/echo/')
    await c.connect()
    await c.sendText('0123456789')
    expect(await c.receiveClose()).toEqual({ code: 1009, reason: 'Message too big' })
  })
})
