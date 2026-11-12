// The handshake: what `Consumer.asView()` runs for an incoming upgrade request, after MIDDLEWARE.
import { anonymousUser, getHost, getUser, type RequestUser, resolverMatch, tryGetContext } from '@ghuri/core'
import type { Context } from 'hono'
import type { ConsumerClass } from './consumer'
import { recordSize } from './host'
import { getLayer } from './layer'
import { type ResolvedRealtimeSettings, realtimeSettings } from './settings'
import { MAX_RECORD_BYTES, type Scope } from './types'
import { snapshotUser } from './user'

const MAX_ROOM_LENGTH = 512
const MAX_AUTH_BYTES = 512
const MAX_USER_AGENT = 200

function reject(status: number, message: string, headers: Record<string, string> = {}): Response {
  return new Response(message, {
    status,
    headers: { 'content-type': 'text/plain; charset=utf-8', ...headers },
  })
}

export function parseSubprotocols(header: string | undefined): string[] {
  return (header ?? '')
    .split(',')
    .map((p) => p.trim())
    .filter(Boolean)
}

function originMatches(origin: string, pattern: string): boolean {
  if (pattern === '*' || pattern === origin) return true
  const m = /^([a-z][a-z0-9+.-]*):\/\/\*\.(.+)$/i.exec(pattern)
  if (!m) return false
  try {
    const u = new URL(origin)
    return u.protocol === `${m[1].toLowerCase()}:` && (u.host === m[2] || u.host.endsWith(`.${m[2]}`))
  } catch {
    return false
  }
}

/** Browser origins must match the host, `CSRF_TRUSTED_ORIGINS` or `REALTIME.ALLOWED_ORIGINS`. */
export function originAllowed(c: Context, origin: string, settings: ResolvedRealtimeSettings): boolean {
  if (origin === 'null') return false
  let host: string
  try {
    host = new URL(origin).host
  } catch {
    return false
  }
  if (host === getHost(c)) return true
  const trusted = (tryGetContext()?.settings.CSRF_TRUSTED_ORIGINS as string[] | undefined) ?? []
  return [...trusted, ...settings.allowedOrigins].some((p) => originMatches(origin, p))
}

function extractToken(offered: string[], url: URL, settings: ResolvedRealtimeSettings): string | undefined {
  if (settings.tokenSources.includes('subprotocol')) {
    const p = offered.find((x) => x.toLowerCase().startsWith('bearer.'))
    if (p) return p.slice('bearer.'.length)
  }
  if (settings.tokenSources.includes('query')) return url.searchParams.get('token') ?? undefined
  return undefined
}

function bindAll<T extends object>(target: T, override: (prop: PropertyKey) => unknown): T {
  return new Proxy(target, {
    get(t, p) {
      const o = override(p)
      if (o !== undefined) return o
      const v = Reflect.get(t, p, t)
      return typeof v === 'function' ? v.bind(t) : v
    },
  })
}

/** The context handed to authentication classes, with the token copied into `Authorization`. */
function withAuthorization(c: Context, value: string): Context {
  let raw: Request | undefined
  const header = (name?: string) => {
    if (name === undefined) return { ...c.req.header(), authorization: value }
    return name.toLowerCase() === 'authorization' ? value : c.req.header(name)
  }
  const req = bindAll(c.req, (p) => {
    if (p === 'header') return header
    if (p === 'raw') {
      if (!raw) {
        const headers = new Headers(c.req.raw.headers)
        headers.set('authorization', value)
        raw = new Request(c.req.raw.url, { method: 'GET', headers })
      }
      return raw
    }
    return undefined
  })
  return bindAll(c, (p) => (p === 'req' ? req : undefined))
}

function statusOf(err: unknown): number | undefined {
  const e = err as any
  return e?.statusCode ?? e?.constructor?.statusCode ?? e?.status
}

function jsonSafe(value: unknown): unknown {
  if (value === undefined || value === null) return undefined
  try {
    const text = JSON.stringify(value)
    if (text === undefined || new TextEncoder().encode(text).byteLength > MAX_AUTH_BYTES) return undefined
    return JSON.parse(text)
  } catch {
    return undefined
  }
}

export async function resolveRoom(cls: ConsumerClass, scope: Scope): Promise<string | null> {
  const r = cls.room
  const room = r === undefined ? scope.path : typeof r === 'function' ? await r(scope) : r
  if (room === null || room === undefined) return null
  const name = String(room)
  if (!name || name.length > MAX_ROOM_LENGTH)
    throw new TypeError(`Room names must be 1-${MAX_ROOM_LENGTH} characters.`)
  return name
}

export async function handshake(
  c: Context,
  cls: ConsumerClass,
  initkwargs: Record<string, unknown>,
): Promise<Response> {
  if ((c.req.header('upgrade') ?? '').toLowerCase() !== 'websocket') {
    return reject(426, 'This URL only accepts WebSocket connections.', { Upgrade: 'websocket' })
  }
  const settings = realtimeSettings()
  const origin = c.req.header('origin')
  if (origin !== undefined && !originAllowed(c, origin, settings)) {
    return reject(403, `WebSocket connections from origin '${origin}' are not allowed.`)
  }

  const offered = parseSubprotocols(c.req.header('sec-websocket-protocol'))
  const url = new URL(c.req.url)
  const token = extractToken(offered, url, settings)

  let user: RequestUser = getUser(c)
  let auth: unknown
  if (cls.authenticationClasses?.length) {
    user = anonymousUser
    const authCtx =
      token && !c.req.header('authorization') ? withAuthorization(c, `${settings.tokenKeyword} ${token}`) : c
    for (const A of cls.authenticationClasses) {
      try {
        const result = await new A().authenticate(authCtx)
        if (result) {
          ;[user, auth] = result
          break
        }
      } catch (err) {
        if (statusOf(err) === 401) return reject(401, (err as Error).message || 'Authentication failed.')
        throw err
      }
    }
  }

  const query = Object.fromEntries(url.searchParams)
  if (settings.tokenSources.includes('query')) delete query.token
  const ua = c.req.header('user-agent')
  const scope: Scope = {
    path: url.pathname,
    params: { ...c.req.param() } as Record<string, string>,
    query,
    room: '',
    connectionId: '',
    headers: {
      subprotocols: offered.filter((p) => !p.toLowerCase().startsWith('bearer.')),
      ...(origin !== undefined ? { origin } : {}),
      ...(ua ? { userAgent: ua.slice(0, MAX_USER_AGENT) } : {}),
    },
    user: snapshotUser(user),
  }
  const routeName = resolverMatch(c)?.viewName
  if (routeName) scope.routeName = routeName
  const sessionKey = (c.get('session' as never) as { sessionKey?: string | null } | undefined)?.sessionKey
  if (sessionKey) scope.sessionKey = sessionKey
  const authJson = jsonSafe(auth)
  if (authJson !== undefined) scope.auth = authJson

  // Permissions see the scope too (room and connectionId are not resolved yet).
  for (const P of cls.permissionClasses ?? []) {
    const perm = new P()
    const ok = perm.hasPermission
      ? await perm.hasPermission({ user, auth, method: 'GET', c, scope }, cls)
      : true
    if (!ok) {
      const message = typeof perm.message === 'string' ? perm.message : 'Permission denied.'
      return reject(user.isAuthenticated ? 403 : 401, message)
    }
  }

  const room = await resolveRoom(cls, scope)
  if (room === null) return reject(403, 'No room for this connection.')
  scope.room = room
  scope.connectionId = `${room}:${crypto.randomUUID()}`
  if (recordSize({ scope, state: {}, tags: [], connectedAt: 0, validatedAt: 0 }) > MAX_RECORD_BYTES) {
    return reject(431, 'WebSocket handshake too large (query string, headers or user data).')
  }

  return getLayer().connect({ request: c.req.raw, scope, consumer: cls, initkwargs })
}
