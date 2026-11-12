import { loadUser, tryGetContext, type ViewFunction } from '@ghuri/core'
import { ConsumerStateError } from './errors'
import type {
  AuthenticationClassLike,
  ConnectionRecord,
  GroupEvent,
  PermissionClassLike,
  Room,
  Scope,
} from './types'
import { MAX_TAG_LENGTH, MAX_TAGS } from './types'
import { RealtimeUser, snapshotUser } from './user'
import { handshake } from './view'

/** What a consumer instance uses to talk to its connection. Provided by the room host. */
export interface ConsumerHandle {
  readonly phase: 'handshake' | 'open'
  readonly accepted: boolean
  readonly record: ConnectionRecord
  readonly room: Room
  accept(subprotocol?: string): void
  send(data: string | ArrayBuffer | Uint8Array): void
  close(code?: number, reason?: string): void
}

export type ConsumerClass = typeof WebSocketConsumer

const HANDLE = Symbol('ghuri.realtime.handle')

/** Methods a group event may never dispatch to. */
const RESERVED = new Set([
  'constructor',
  'connect',
  'receive',
  'receiveJson',
  'disconnect',
  'accept',
  'send',
  'sendJson',
  'close',
  'tag',
  'untag',
  'refreshUser',
  'dispatch',
])

/** `chat.message` / `chat-message` / `chat_message` → `chatMessage`. */
export function handlerName(type: string): string {
  const name = type.replace(/[._-]+([a-zA-Z0-9])/g, (_, ch: string) => ch.toUpperCase())
  if (!/^[a-zA-Z][a-zA-Z0-9]*$/.test(name) || RESERVED.has(name)) {
    throw new ConsumerStateError(`Event type '${type}' does not map to a handler method.`)
  }
  return name
}

/**
 * A WebSocket consumer (Django Channels' `WebsocketConsumer`, async). Route it with
 * `path('ws/chat/<slug:room>/', ChatConsumer.asView())`.
 *
 * Instances are rebuilt for every event from `scope` and `state` (a hibernating Durable Object keeps no
 * memory). Anything you want between events goes in `this.state` (JSON, 2 KB with the scope).
 */
export class WebSocketConsumer {
  /** The room that owns the connection. Default: the request path (one room per URL). `null` rejects with 403. */
  static room?: string | ((scope: Scope) => string | null | Promise<string | null>)
  /** Authentication classes run on the handshake (REST contract). Default: the user set by MIDDLEWARE. */
  static authenticationClasses?: AuthenticationClassLike[]
  /** Permission classes checked on the handshake, before `connect()`. */
  static permissionClasses?: PermissionClassLike[]
  /** Subprotocols this consumer speaks; the first one the client offered is accepted by default. */
  static subprotocols?: string[]

  /** A view for `path()`. Values in `initkwargs` are assigned to every instance. */
  static asView(initkwargs: Record<string, unknown> = {}): ViewFunction & { consumerClass: ConsumerClass } {
    const cls = this as ConsumerClass
    const view = ((c) => handshake(c, cls, initkwargs)) as ViewFunction & {
      consumerClass: ConsumerClass
      initkwargs: Record<string, unknown>
    }
    view.consumerClass = cls
    view.initkwargs = initkwargs
    Object.defineProperty(view, 'name', { value: `${cls.name}View` })
    return view
  }

  scope!: Scope
  state: Record<string, unknown> = {}
  user!: RealtimeUser
  room!: Room
  declare [HANDLE]: ConsumerHandle

  /** Called when the handshake arrives. Default: accept. Close (or don't accept) to reject with 403. */
  async connect(): Promise<void> {
    this.accept()
  }

  async receive(_message: { text?: string; bytes?: Uint8Array }): Promise<void> {}

  async disconnect(_code: number, _reason: string): Promise<void> {}

  /** Accept the handshake (only valid in `connect()`). */
  accept(subprotocol?: string): void {
    this[HANDLE].accept(subprotocol)
  }

  send(data: string | ArrayBuffer | Uint8Array): void {
    const h = this[HANDLE]
    if (h.phase === 'handshake' && !h.accepted) throw new ConsumerStateError('send() before accept().')
    h.send(data)
  }

  /** Close the socket. In `connect()` before `accept()`, rejects the handshake with 403. */
  close(code = 1000, reason = ''): void {
    this[HANDLE].close(code, reason)
  }

  /** Tags label connections inside the room, for `broadcast({ tag })` / `groupSend({ tag })`. */
  tag(...tags: string[]): void {
    const rec = this[HANDLE].record
    for (const t of tags) {
      if (!t || t.length > MAX_TAG_LENGTH)
        throw new ConsumerStateError(`Tags must be 1-${MAX_TAG_LENGTH} characters.`)
      if (!rec.tags.includes(t)) rec.tags.push(t)
    }
    if (rec.tags.length > MAX_TAGS)
      throw new ConsumerStateError(`A connection can carry at most ${MAX_TAGS} tags.`)
  }

  untag(...tags: string[]): void {
    const rec = this[HANDLE].record
    rec.tags = rec.tags.filter((t) => !tags.includes(t))
  }

  get tags(): readonly string[] {
    return this[HANDLE].record.tags
  }

  /**
   * Reload the user from the database. Returns false and closes with 4401 when the user is gone or inactive.
   */
  async refreshUser(): Promise<boolean> {
    if (!this.user.isAuthenticated) return true
    const fresh = await loadUser(this.user.pk)
    const rec = this[HANDLE].record
    rec.validatedAt = Date.now()
    if (!fresh || fresh.isActive === false) {
      this.close(4401, 'User is no longer active')
      return false
    }
    rec.scope.user = snapshotUser(fresh)
    this.scope = rec.scope
    this.user = new RealtimeUser(rec.scope.user)
    return true
  }

  /** Run the handler for a group event (`chat.message` → `chatMessage(event)`). */
  async dispatch(event: GroupEvent): Promise<void> {
    const name = handlerName(event.type)
    const fn = (this as any)[name]
    if (typeof fn !== 'function') {
      if (tryGetContext()?.settings.DEBUG) {
        console.warn(`${this.constructor.name} has no handler '${name}' for event '${event.type}'; dropped.`)
      }
      return
    }
    await fn.call(this, event)
  }
}

/** A consumer that speaks JSON text frames. */
export class JsonWebSocketConsumer extends WebSocketConsumer {
  override async receive(message: { text?: string; bytes?: Uint8Array }): Promise<void> {
    if (message.text === undefined) {
      this.close(1003, 'Expected a JSON text frame')
      return
    }
    let data: unknown
    try {
      data = JSON.parse(message.text)
    } catch {
      this.close(1007, 'Invalid JSON')
      return
    }
    await this.receiveJson(data)
  }

  async receiveJson(_data: unknown): Promise<void> {}

  sendJson(data: unknown): void {
    this.send(JSON.stringify(data))
  }
}

/** Build a consumer instance for one event. */
export function instantiate(
  cls: ConsumerClass,
  handle: ConsumerHandle,
  initkwargs: Record<string, unknown> = {},
): WebSocketConsumer {
  const consumer = new cls()
  Object.assign(consumer, initkwargs)
  consumer[HANDLE] = handle
  consumer.scope = handle.record.scope
  consumer.state = handle.record.state
  consumer.user = new RealtimeUser(handle.record.scope.user)
  consumer.room = handle.room
  return consumer
}

/** The state to persist after a handler ran (consumers may replace `this.state`). */
export function stateOf(consumer: WebSocketConsumer): Record<string, unknown> {
  const s = consumer.state
  return s && typeof s === 'object' ? s : {}
}
