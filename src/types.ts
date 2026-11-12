import type { Env, RequestUser } from '@ghuri/core'

/** The parts of a user kept with a connection (it must survive Durable Object hibernation). */
export interface UserSnapshot {
  pk: unknown
  username?: string
  isAuthenticated: boolean
  isActive?: boolean
  isStaff?: boolean
  isSuperuser?: boolean
}

/** What the handshake established. Stored with the socket; read-only for consumers. */
export interface Scope {
  path: string
  /** Route parameters as strings (`<int:id>` arrives as `'42'`). */
  params: Record<string, string>
  query: Record<string, string>
  /** `namespace:name` of the route, when it has a name. */
  routeName?: string
  room: string
  /** `<room>:<uuid>`, stable for the life of the socket. */
  connectionId: string
  headers: { origin?: string; userAgent?: string; subprotocols: string[] }
  user: UserSnapshot
  sessionKey?: string
  /** The token payload an authentication class returned, when JSON-safe and small. */
  auth?: unknown
}

/** A server-side event sent to a room; `type` picks the consumer method (`chat.message` → `chatMessage`). */
export interface GroupEvent {
  type: string
  [key: string]: unknown
}

export interface BroadcastOptions {
  /** Only connections carrying this tag. */
  tag?: string
  /** Skip one connection: a consumer or a connection id. */
  exclude?: string | { scope: { connectionId: string } }
}

export interface ConnectionInfo {
  id: string
  userPk: unknown
  tags: string[]
  connectedAt: number
}

/** Small per-room key/value storage (Durable Object storage on Workers, memory on Node). */
export interface RoomStorage {
  get<T = unknown>(key: string): Promise<T | undefined>
  put(key: string, value: unknown): Promise<void>
  delete(key: string): Promise<boolean>
  list<T = unknown>(opts?: { prefix?: string; limit?: number }): Promise<Map<string, T>>
}

export interface Room {
  readonly name: string
  /** Run the matching consumer handler on every connection (each decides what to send). */
  groupSend(event: GroupEvent, opts?: BroadcastOptions): Promise<void>
  /** Send the same frame to every connection without running consumer code. */
  broadcast(message: unknown, opts?: BroadcastOptions): Promise<void>
  /** Run the handler on one connection of this room. */
  send(connectionId: string, event: GroupEvent): Promise<void>
  connections(opts?: { tag?: string }): Promise<ConnectionInfo[]>
  count(opts?: { tag?: string }): Promise<number>
  readonly storage: RoomStorage
}

/** What the transport needs to know: a socket the host can write to. */
export interface SocketLike {
  send(data: string | ArrayBuffer | Uint8Array): void
  close(code?: number, reason?: string): void
}

/** Persisted with every socket (Durable Object attachment). Must stay under `MAX_RECORD_BYTES`. */
export interface ConnectionRecord {
  scope: Scope
  state: Record<string, unknown>
  tags: string[]
  connectedAt: number
  /** When the user was last checked against the database (ms). */
  validatedAt: number
}

/** Cloudflare's limit for `WebSocket.serializeAttachment()`; enforced on every backend for portability. */
export const MAX_RECORD_BYTES = 2048
export const MAX_TAGS = 10
export const MAX_TAG_LENGTH = 256

/** The (structural) REST authentication contract, so realtime does not import `@ghuri/rest`. */
export interface AuthenticationLike {
  authenticate(c: any): Promise<[RequestUser, unknown] | null>
  authenticateHeader?(c: any): string | undefined
}
export type AuthenticationClassLike = new () => AuthenticationLike

/** The (structural) REST permission contract. Realtime passes `{ user, auth, method: 'GET', c, scope }`. */
export interface PermissionLike {
  hasPermission?(request: any, view: any): boolean | Promise<boolean>
  message?: string | Record<string, unknown>
}
export type PermissionClassLike = new () => PermissionLike

export type RealtimeEnv = Env
