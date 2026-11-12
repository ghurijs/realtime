// @ghuri/realtime/testing — drive consumers in tests without a network (Channels' WebsocketCommunicator).
import type { Env, ExecutionContextLike } from '@ghuri/core'
import { createTestClient, type TestableApp } from '@ghuri/core/testing'
import { PendingUpgrade, pendingUpgrades, type TransportHandlers } from './upgrade'

export interface CommunicatorOptions {
  /** Log this user in first (uses the test client's `forceLogin` hook from `@ghuri/auth`). */
  user?: unknown
  headers?: Record<string, string>
  subprotocols?: string[]
  env?: Env
  /** Default `http://testserver`. */
  baseUrl?: string
}

export interface ConnectResult {
  connected: boolean
  /** HTTP status of the handshake response (101 when connected). */
  status: number
  subprotocol?: string
  /** Body of a rejected handshake. */
  body?: string
}

type Frame =
  | { kind: 'text'; data: string }
  | { kind: 'bytes'; data: Uint8Array }
  | { kind: 'close'; code: number; reason: string }

export class CommunicatorClosed extends Error {
  constructor(
    readonly code: number,
    readonly reason: string,
  ) {
    super(`The server closed the connection (${code}${reason ? ` ${reason}` : ''}).`)
  }
}

/**
 * ```ts
 * const ws = new WebSocketCommunicator(app, '/ws/chat/lobby/', { user })
 * expect((await ws.connect()).connected).toBe(true)
 * await ws.sendJson({ text: 'hi' })
 * expect(await ws.receiveJson()).toEqual({ type: 'chat.message', text: 'hi' })
 * await ws.disconnect()
 * ```
 */
export class WebSocketCommunicator {
  private frames: Frame[] = []
  private waiters: Array<() => void> = []
  private handlers?: TransportHandlers
  private closedBy?: 'client' | 'server'
  private serverClose?: { code: number; reason: string }
  private readonly execCtx: ExecutionContextLike & { pending: Promise<unknown>[] }

  constructor(
    readonly app: TestableApp,
    readonly path: string,
    readonly options: CommunicatorOptions = {},
  ) {
    const pending: Promise<unknown>[] = []
    this.execCtx = { pending, waitUntil: (p) => void pending.push(p) }
  }

  async connect(): Promise<ConnectResult> {
    const client = createTestClient(this.app, { env: this.options.env, baseUrl: this.options.baseUrl })
    if (this.options.user !== undefined) await client.forceLogin(this.options.user)
    const url = new URL(this.path, client.baseUrl)
    const headers = new Headers({
      upgrade: 'websocket',
      connection: 'Upgrade',
      'sec-websocket-version': '13',
      'sec-websocket-key': btoa(String.fromCharCode(...crypto.getRandomValues(new Uint8Array(16)))),
      ...this.options.headers,
    })
    if (this.options.subprotocols?.length)
      headers.set('sec-websocket-protocol', this.options.subprotocols.join(', '))
    const cookies = [...client.cookies.values()].map((c) => `${c.name}=${c.value}`).join('; ')
    if (cookies && !headers.has('cookie')) headers.set('cookie', cookies)

    const request = new Request(url, { method: 'GET', headers })
    const pending = new PendingUpgrade()
    pendingUpgrades.set(request, pending)
    const res = await this.app.fetch(request, this.options.env ?? {}, this.execCtx)
    if (!pending.accepted || res.status >= 300) {
      if (pending.accepted) await pending.open(this.transport()).close(1006, 'Handshake failed')
      return { connected: false, status: res.status, body: await res.text() }
    }
    this.handlers = pending.open(this.transport())
    return { connected: true, status: 101, subprotocol: pending.subprotocol }
  }

  private transport() {
    return {
      send: (data: string | ArrayBuffer | Uint8Array) => {
        if (typeof data === 'string') this.push({ kind: 'text', data })
        else this.push({ kind: 'bytes', data: data instanceof Uint8Array ? data : new Uint8Array(data) })
      },
      close: (code = 1000, reason = '') => {
        if (this.closedBy) return
        this.closedBy = 'server'
        this.serverClose = { code, reason }
        this.push({ kind: 'close', code, reason })
        // The client answers the close; the server then runs disconnect().
        queueMicrotask(() => void this.handlers?.close(code, reason))
      },
    }
  }

  private push(frame: Frame): void {
    this.frames.push(frame)
    for (const w of this.waiters.splice(0)) w()
  }

  private requireOpen(): TransportHandlers {
    if (!this.handlers) throw new Error('Not connected: call connect() first.')
    if (this.closedBy) throw new Error('The connection is closed.')
    return this.handlers
  }

  /** Send a text frame; resolves after the consumer handled it. */
  async sendText(text: string): Promise<void> {
    await this.requireOpen().message(text)
  }

  async sendJson(data: unknown): Promise<void> {
    await this.sendText(JSON.stringify(data))
  }

  async sendBytes(data: Uint8Array): Promise<void> {
    await this.requireOpen().message(data)
  }

  private async next(timeout: number): Promise<Frame> {
    const deadline = Date.now() + timeout
    while (!this.frames.length) {
      if (this.serverClose) throw new CommunicatorClosed(this.serverClose.code, this.serverClose.reason)
      const left = deadline - Date.now()
      if (left <= 0) throw new Error(`No message within ${timeout} ms.`)
      await new Promise<void>((resolve) => {
        const t = setTimeout(resolve, left)
        this.waiters.push(() => {
          clearTimeout(t)
          resolve()
        })
      })
    }
    return this.frames.shift()!
  }

  /** The next frame from the server (text or bytes). Throws `CommunicatorClosed` if the server closed. */
  async receiveFrom(opts: { timeout?: number } = {}): Promise<string | Uint8Array> {
    const f = await this.next(opts.timeout ?? 1000)
    if (f.kind === 'close') {
      this.frames.unshift(f)
      throw new CommunicatorClosed(f.code, f.reason)
    }
    return f.data
  }

  async receiveJson<T = any>(opts: { timeout?: number } = {}): Promise<T> {
    const data = await this.receiveFrom(opts)
    if (typeof data !== 'string') throw new Error('Expected a text frame, got bytes.')
    return JSON.parse(data) as T
  }

  /** True when nothing arrives within `timeout` ms (default 100). */
  async receiveNothing(opts: { timeout?: number } = {}): Promise<boolean> {
    try {
      const f = await this.next(opts.timeout ?? 100)
      this.frames.unshift(f)
      return false
    } catch {
      return true
    }
  }

  /** Wait for the server to close; returns the close code and reason. */
  async receiveClose(opts: { timeout?: number } = {}): Promise<{ code: number; reason: string }> {
    for (;;) {
      const f = await this.next(opts.timeout ?? 1000)
      if (f.kind === 'close') return { code: f.code, reason: f.reason }
    }
  }

  /** Close from the client side; resolves after the consumer's disconnect() ran. */
  async disconnect(code = 1000, reason = ''): Promise<void> {
    if (!this.handlers || this.closedBy) return
    this.closedBy = 'client'
    await this.handlers.close(code, reason)
  }
}
