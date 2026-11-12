// Bridges an HTTP upgrade that happens outside `fetch` (Node's 'upgrade' event, the test communicator) to the
// consumer view: the caller registers a PendingUpgrade for the Request, runs it through `app.fetch`, and
// completes the WebSocket handshake only if the view accepted.
import type { SocketLike } from './types'

export interface TransportHandlers {
  message(data: string | ArrayBuffer | Uint8Array): Promise<void>
  close(code: number, reason: string): Promise<void>
}

export class PendingUpgrade {
  accepted = false
  subprotocol: string | undefined
  private attachFn?: (transport: SocketLike) => TransportHandlers

  /** Called by the layer when the consumer accepted. */
  accept(subprotocol: string | undefined, attach: (transport: SocketLike) => TransportHandlers): void {
    this.accepted = true
    this.subprotocol = subprotocol
    this.attachFn = attach
  }

  /** Called by the transport once the socket is open. */
  open(transport: SocketLike): TransportHandlers {
    if (!this.attachFn) throw new Error('open() on an upgrade that was not accepted')
    return this.attachFn(transport)
  }
}

const KEY = Symbol.for('ghuri.realtime.pendingUpgrades')
const g = globalThis as any
if (!g[KEY]) g[KEY] = new WeakMap<Request, PendingUpgrade>()
export const pendingUpgrades: WeakMap<Request, PendingUpgrade> = g[KEY]

/** A socket that queues frames until the real transport is attached (Node completes the upgrade later). */
export class BufferedSocket implements SocketLike {
  private target?: SocketLike
  private queue: Array<string | ArrayBuffer | Uint8Array> = []
  private closed?: { code?: number; reason?: string }

  attach(target: SocketLike): void {
    this.target = target
    for (const frame of this.queue) target.send(frame)
    this.queue = []
    if (this.closed) target.close(this.closed.code, this.closed.reason)
  }

  send(data: string | ArrayBuffer | Uint8Array): void {
    if (this.closed) return
    if (this.target) this.target.send(data)
    else this.queue.push(data)
  }

  close(code?: number, reason?: string): void {
    if (this.target) this.target.close(code, reason)
    else this.closed ??= { code, reason }
  }
}
