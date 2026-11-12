// A real Node server (core's serve()) with attachRealtime, driven by a `ws` client over TCP.
import type { Server } from 'node:http'
import { createApp, defineSettings, type GhuriApp, path, SecurityMiddleware } from '@ghuri/core'
import { serve } from '@ghuri/core/node'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import WebSocket from 'ws'
import { JsonWebSocketConsumer, memoryLayer, type Scope } from '../src/index'
import { attachRealtime } from '../src/node'

class RoomConsumer extends JsonWebSocketConsumer {
  static override room = (s: Scope) => `node.${s.params.name}`
  override async connect() {
    if (this.scope.query.deny) return this.close()
    this.accept()
  }
  override async receiveJson(data: any) {
    await this.room.groupSend({ type: 'say', text: data.text })
  }
  async say(e: any) {
    this.sendJson({ text: e.text })
  }
}

function open(url: string, protocols?: string[]): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url, protocols)
    ws.once('open', () => resolve(ws))
    ws.once('unexpected-response', (_req, res) => reject(new Error(`HTTP ${res.statusCode}`)))
    ws.once('error', reject)
  })
}

const nextJson = (ws: WebSocket) =>
  new Promise<any>((resolve) => ws.once('message', (d) => resolve(JSON.parse(d.toString()))))

describe('attachRealtime on a Node server', () => {
  let app: GhuriApp
  let server: Server
  let base: string

  beforeAll(async () => {
    app = createApp(
      defineSettings(() => ({
        SECRET_KEY: 'x',
        ALLOWED_HOSTS: ['*'],
        INSTALLED_APPS: [],
        MIDDLEWARE: [SecurityMiddleware],
        ROOT_URLCONF: [path('ws/<slug:name>/', RoomConsumer.asView())],
        REALTIME: { LAYER: memoryLayer({ name: 'node-test' }) },
      })),
      { checks: false },
    )
    let port = 0
    server = (await serve(app, {
      port: 0,
      hostname: '127.0.0.1',
      env: {},
      handleSignals: false,
      onListen: (info) => {
        port = info.port
      },
    })) as unknown as Server
    await new Promise((r) => setTimeout(r, 50))
    attachRealtime(server, app, { env: {} })
    base = `ws://127.0.0.1:${port}`
  })
  afterAll(() => new Promise<void>((r) => server.close(() => r())))

  it('upgrades through the app and fans out between real sockets', async () => {
    const a = await open(`${base}/ws/lobby/`)
    const b = await open(`${base}/ws/lobby/`, ['ghuri'])
    expect(b.protocol).toBe('ghuri')
    const got = Promise.all([nextJson(a), nextJson(b)])
    a.send(JSON.stringify({ text: 'over tcp' }))
    expect(await got).toEqual([{ text: 'over tcp' }, { text: 'over tcp' }])
    a.close()
    b.close()
  })

  it('answers rejected handshakes with an HTTP status', async () => {
    await expect(open(`${base}/ws/lobby/?deny=1`)).rejects.toThrow('HTTP 403')
    await expect(open(`${base}/missing/`)).rejects.toThrow('HTTP 404')
  })
})
