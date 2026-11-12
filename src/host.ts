// The room host: consumer lifecycle and fan-out for one room, shared by every layer. A layer supplies a
// driver (where sockets live, how to run code in a Ghuri context, storage); the host does the rest.
import { setUser, tryGetContext } from '@ghuri/core'
import {
  type ConsumerClass,
  type ConsumerHandle,
  instantiate,
  stateOf,
  type WebSocketConsumer,
} from './consumer'
import { ConsumerStateError, RealtimeStateTooLarge } from './errors'
import { realtimeSettings } from './settings'
import {
  type BroadcastOptions,
  type ConnectionInfo,
  type ConnectionRecord,
  type GroupEvent,
  MAX_RECORD_BYTES,
  type Room,
  type RoomStorage,
  type Scope,
  type SocketLike,
} from './types'

export interface HostConnection {
  record: ConnectionRecord
  socket: SocketLike
  /** Persist the record (Durable Object attachment); memory drivers keep it in place. */
  save(record: ConnectionRecord): void
}

export interface HostDriver {
  readonly name: string
  readonly storage: RoomStorage
  /** Open connections in this room. */
  list(): HostConnection[]
  /** Forget a connection after it closed. */
  remove(conn: HostConnection): void
  /** Run `fn` in a Ghuri context suitable for this connection (or for the room when `conn` is undefined). */
  run<T>(conn: HostConnection | undefined, fn: () => Promise<T>): Promise<T>
  /** The consumer class (and initkwargs) for a connection, e.g. resolved from `scope.path` after hibernation. */
  consumerFor(conn: HostConnection): Promise<{ cls: ConsumerClass; initkwargs: Record<string, unknown> }>
}

export interface HandshakeOutcome {
  accepted: boolean
  subprotocol?: string
  conn?: HostConnection
  /** Code/reason when connect() closed before accepting. */
  rejected?: { code: number; reason: string }
}

const HOST_KEY = Symbol.for('ghuri.realtime.host')
const encoder = new TextEncoder()

/** The host whose code is running right now (consumer handlers), if any. */
export function currentHost(): RoomHost | undefined {
  return tryGetContext()?.state.get(HOST_KEY) as RoomHost | undefined
}

export function encodeFrame(message: unknown): string | ArrayBuffer | Uint8Array {
  if (typeof message === 'string' || message instanceof ArrayBuffer || message instanceof Uint8Array)
    return message
  return JSON.stringify(message)
}

export function recordSize(record: ConnectionRecord): number {
  return encoder.encode(JSON.stringify(record)).byteLength
}

function checkSize(record: ConnectionRecord): void {
  const size = recordSize(record)
  if (size > MAX_RECORD_BYTES) {
    throw new RealtimeStateTooLarge(
      `Connection scope + state + tags take ${size} bytes; the limit is ${MAX_RECORD_BYTES} ` +
        '(Cloudflare WebSocket attachments). Keep large data in room.storage or the database.',
    )
  }
}

function matches(conn: HostConnection, opts: BroadcastOptions | { tag?: string } = {}): boolean {
  if (opts.tag !== undefined && !conn.record.tags.includes(opts.tag)) return false
  const ex = (opts as BroadcastOptions).exclude
  if (ex !== undefined) {
    const id = typeof ex === 'string' ? ex : ex.scope.connectionId
    if (conn.record.scope.connectionId === id) return false
  }
  return true
}

function info(conn: HostConnection): ConnectionInfo {
  const r = conn.record
  return { id: r.scope.connectionId, userPk: r.scope.user.pk, tags: [...r.tags], connectedAt: r.connectedAt }
}

export class RoomHost {
  readonly room: Room
  /** Connections whose disconnect already ran (transports can report a close twice). */
  private readonly gone = new Set<string>()

  constructor(readonly driver: HostDriver) {
    this.room = {
      name: driver.name,
      groupSend: (event, opts) => this.groupSend(event, opts),
      broadcast: (message, opts) => this.broadcast(message, opts),
      send: (connectionId, event) => this.sendTo(connectionId, event),
      connections: async (opts) => this.connections(opts),
      count: async (opts) => this.connections(opts).length,
      storage: driver.storage,
    }
  }

  connections(opts?: { tag?: string }): ConnectionInfo[] {
    return this.driver
      .list()
      .filter((c) => matches(c, opts))
      .map(info)
  }

  /**
   * Run `connect()` for a new connection. `open` is called when the consumer accepts and must return the
   * live connection (the driver attaches the transport and starts listing it).
   */
  async handshake(
    scope: Scope,
    cls: ConsumerClass,
    initkwargs: Record<string, unknown>,
    open: (record: ConnectionRecord, subprotocol: string | undefined) => HostConnection,
  ): Promise<HandshakeOutcome> {
    const now = Date.now()
    const record: ConnectionRecord = { scope, state: {}, tags: [], connectedAt: now, validatedAt: now }
    checkSize(record)
    let conn: HostConnection | undefined
    let subprotocol: string | undefined
    let rejected: { code: number; reason: string } | undefined
    const offered = scope.headers.subprotocols
    const handle: ConsumerHandle = {
      get phase() {
        return 'handshake' as const
      },
      get accepted() {
        return conn !== undefined
      },
      record,
      room: this.room,
      accept: (sp) => {
        if (conn || rejected) return
        if (sp !== undefined && !offered.includes(sp)) {
          throw new ConsumerStateError(`accept('${sp}'): the client did not offer that subprotocol.`)
        }
        subprotocol = sp ?? defaultSubprotocol(cls, offered)
        conn = open(record, subprotocol)
      },
      send: (data) => conn?.socket.send(data),
      close: (code = 1000, reason = '') => {
        if (!conn) rejected ??= { code, reason }
        else this.closeConn(conn, code, reason)
      },
    }

    return this.driver.run(conn, async () => {
      tryGetContext()?.state.set(HOST_KEY, this)
      const consumer = instantiate(cls, handle, initkwargs)
      setUser(undefined, consumer.user)
      try {
        await consumer.connect()
        if (conn) this.persist(conn, consumer)
      } catch (err) {
        if (conn) this.closeConn(conn, 1011, 'Server error')
        throw err
      }
      if (!conn) {
        if (!rejected && tryGetContext()?.settings.DEBUG) {
          console.warn(`${cls.name}.connect() neither accepted nor closed; the handshake was rejected.`)
        }
        return { accepted: false, rejected }
      }
      return { accepted: true, subprotocol, conn }
    })
  }

  /** A frame arrived from the client. */
  async receive(conn: HostConnection, data: string | ArrayBuffer | Uint8Array): Promise<void> {
    await this.withConsumer(conn, async (consumer, settings) => {
      const size = typeof data === 'string' ? encoder.encode(data).byteLength : data.byteLength
      if (size > settings.maxMessageBytes) {
        this.closeConn(conn, 1009, 'Message too big')
        return
      }
      const message =
        typeof data === 'string'
          ? { text: data }
          : { bytes: data instanceof Uint8Array ? data : new Uint8Array(data) }
      await consumer.receive(message)
    })
  }

  /** The socket closed (client or server side). Runs `disconnect()` once. */
  async disconnect(conn: HostConnection, code: number, reason: string): Promise<void> {
    const id = conn.record.scope.connectionId
    if (this.gone.has(id)) return
    this.gone.add(id)
    this.driver.remove(conn)
    try {
      await this.driver.run(conn, async () => {
        tryGetContext()?.state.set(HOST_KEY, this)
        const { cls, initkwargs } = await this.driver.consumerFor(conn)
        const consumer = instantiate(cls, this.openHandle(conn), initkwargs)
        setUser(undefined, consumer.user)
        await consumer.disconnect(code, reason)
      })
    } catch (err) {
      console.error(`WebSocket disconnect handler failed (${id}):`, err)
    } finally {
      // Ids are unique; keep the set small.
      if (this.gone.size > 10_000) this.gone.clear()
    }
  }

  async groupSend(event: GroupEvent, opts: BroadcastOptions = {}): Promise<void> {
    if (!event || typeof event.type !== 'string')
      throw new ConsumerStateError('Group events need a string `type`.')
    const targets = this.driver.list().filter((c) => matches(c, opts))
    await Promise.all(targets.map((conn) => this.withConsumer(conn, (consumer) => consumer.dispatch(event))))
  }

  async sendTo(connectionId: string, event: GroupEvent): Promise<void> {
    const conn = this.driver.list().find((c) => c.record.scope.connectionId === connectionId)
    if (conn) await this.withConsumer(conn, (consumer) => consumer.dispatch(event))
  }

  async broadcast(message: unknown, opts: BroadcastOptions = {}): Promise<void> {
    const frame = encodeFrame(message)
    for (const conn of this.driver.list()) {
      if (!matches(conn, opts)) continue
      try {
        conn.socket.send(frame)
      } catch {
        // A socket that is closing; its close event cleans up.
      }
    }
  }

  private closeConn(conn: HostConnection, code: number, reason: string): void {
    try {
      conn.socket.close(code, reason)
    } catch {
      // already closed
    }
  }

  private openHandle(conn: HostConnection): ConsumerHandle {
    return {
      phase: 'open',
      accepted: true,
      record: conn.record,
      room: this.room,
      accept: () => {},
      send: (data) => conn.socket.send(data),
      close: (code = 1000, reason = '') => this.closeConn(conn, code, reason),
    }
  }

  private persist(conn: HostConnection, consumer: WebSocketConsumer): void {
    conn.record.state = stateOf(consumer)
    checkSize(conn.record)
    conn.save(conn.record)
  }

  /** Run a handler on a fresh consumer for `conn`; errors close the socket with 1011 (like Channels). */
  private async withConsumer(
    conn: HostConnection,
    fn: (consumer: WebSocketConsumer, settings: ReturnType<typeof realtimeSettings>) => Promise<void>,
  ): Promise<void> {
    try {
      await this.driver.run(conn, async () => {
        tryGetContext()?.state.set(HOST_KEY, this)
        const settings = realtimeSettings()
        const { cls, initkwargs } = await this.driver.consumerFor(conn)
        const consumer = instantiate(cls, this.openHandle(conn), initkwargs)
        setUser(undefined, consumer.user)
        const every = settings.revalidateSeconds * 1000
        if (every > 0 && consumer.user.isAuthenticated && Date.now() - conn.record.validatedAt >= every) {
          if (!(await consumer.refreshUser())) return
          setUser(undefined, consumer.user)
        }
        await fn(consumer, settings)
        this.persist(conn, consumer)
      })
    } catch (err) {
      console.error(`WebSocket consumer error (${conn.record.scope.connectionId}):`, err)
      this.closeConn(conn, 1011, 'Server error')
    }
  }
}

function defaultSubprotocol(cls: ConsumerClass, offered: string[]): string | undefined {
  for (const p of cls.subprotocols ?? []) if (offered.includes(p)) return p
  // Clients that sent a token as a subprotocol (`ghuri, bearer.<token>`) need one echoed back.
  if (offered.includes('ghuri')) return 'ghuri'
  return undefined
}
