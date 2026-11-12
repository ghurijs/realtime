// @ghuri/realtime/node — serve consumers from a Node HTTP server (the one `serve()` from @ghuri/core/node returns).
import type { IncomingMessage, Server } from 'node:http'
import type { Duplex } from 'node:stream'
import type { Env, ExecutionContextLike } from '@ghuri/core'
import { type WebSocket, WebSocketServer } from 'ws'
import { PendingUpgrade, pendingUpgrades } from './upgrade'

export interface FetchApp {
  fetch(request: Request, env?: Env, ctx?: ExecutionContextLike): Response | Promise<Response>
}

export interface AttachOptions {
  /** Passed to `app.fetch()`; default `process.env` (same as `serve()`). */
  env?: Env
  /** Largest accepted frame in bytes (default 1 MiB; consumers also enforce `REALTIME.MAX_MESSAGE_BYTES`). */
  maxPayload?: number
}

const PENDING = Symbol('ghuri.realtime.pending')
const SET_COOKIES = Symbol('ghuri.realtime.setCookies')

function toHeaders(req: IncomingMessage): Headers {
  const headers = new Headers()
  for (const [k, v] of Object.entries(req.headers)) {
    if (v === undefined) continue
    if (Array.isArray(v)) for (const x of v) headers.append(k, x)
    else headers.set(k, v)
  }
  return headers
}

function writeHttp(socket: Duplex, res: Response, body: string): void {
  const lines = [`HTTP/1.1 ${res.status} ${res.statusText || 'Error'}`]
  res.headers.forEach((v, k) => {
    if (k !== 'content-length' && k !== 'transfer-encoding' && k !== 'connection') lines.push(`${k}: ${v}`)
  })
  const bytes = Buffer.from(body)
  lines.push(`content-length: ${bytes.byteLength}`, 'connection: close', '', '')
  socket.end(Buffer.concat([Buffer.from(lines.join('\r\n')), bytes]))
}

/**
 * Accept WebSocket upgrades on a Node server and route them through the app (MIDDLEWARE, URL patterns,
 * consumers). Use with `memoryLayer()` for one process.
 *
 * ```ts
 * const server = await serve(app, { port: 8000 })
 * attachRealtime(server, app)
 * ```
 */
export function attachRealtime(server: Server, app: FetchApp, opts: AttachOptions = {}): WebSocketServer {
  const env = opts.env ?? (process.env as Env)
  const tasks = new Set<Promise<unknown>>()
  const execCtx: ExecutionContextLike = {
    waitUntil(p) {
      const t = Promise.resolve(p)
        .catch((err) => console.error('waitUntil task failed:', err))
        .finally(() => tasks.delete(t))
      tasks.add(t)
    },
  }
  const wss = new WebSocketServer({
    noServer: true,
    maxPayload: opts.maxPayload ?? 1_048_576,
    handleProtocols: (_protocols, req) =>
      ((req as any)[PENDING] as PendingUpgrade | undefined)?.subprotocol ?? false,
  })
  wss.on('headers', (headers, req) => {
    for (const c of ((req as any)[SET_COOKIES] as string[] | undefined) ?? [])
      headers.push(`Set-Cookie: ${c}`)
  })

  server.on('upgrade', (req: IncomingMessage, socket: Duplex, head: Buffer) => {
    void (async () => {
      const proto = (req.socket as any).encrypted ? 'https' : 'http'
      const request = new Request(`${proto}://${req.headers.host ?? 'localhost'}${req.url ?? '/'}`, {
        method: req.method ?? 'GET',
        headers: toHeaders(req),
      })
      const pending = new PendingUpgrade()
      pendingUpgrades.set(request, pending)
      let res: Response
      try {
        res = await app.fetch(request, env, execCtx)
      } catch (err) {
        console.error('WebSocket handshake failed:', err)
        res = new Response('Internal Server Error', { status: 500 })
      }
      if (!pending.accepted || res.status >= 300) {
        if (pending.accepted) await pending.open({ send() {}, close() {} }).close(1006, 'Handshake failed')
        writeHttp(socket, res, await res.text().catch(() => ''))
        return
      }
      ;(req as any)[PENDING] = pending
      ;(req as any)[SET_COOKIES] = res.headers.getSetCookie?.() ?? []
      wss.handleUpgrade(req, socket, head, (ws: WebSocket) => {
        const handlers = pending.open({
          send: (data) => ws.send(data),
          close: (code, reason) => ws.close(code, reason),
        })
        ws.on('message', (data, isBinary) => {
          const frame = isBinary
            ? new Uint8Array(Array.isArray(data) ? Buffer.concat(data) : (data as Buffer))
            : data.toString()
          void handlers.message(frame)
        })
        ws.on('close', (code, reason) => void handlers.close(code, reason.toString()))
        ws.on('error', () => void handlers.close(1006, 'error'))
      })
    })()
  })
  return wss
}
