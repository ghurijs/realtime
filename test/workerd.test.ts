// Bundles test/fixtures/worker.ts with esbuild and runs it in workerd (Miniflare): rooms in a Durable Object
// (hibernation API, RPC, D1 from consumers, the default MIDDLEWARE stack on a 101), and the in-isolate memory layer.
import { fileURLToPath } from 'node:url'
import { build } from 'esbuild'
import { Miniflare } from 'miniflare'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'

async function bundle(): Promise<string> {
  const result = await build({
    entryPoints: [fileURLToPath(new URL('./fixtures/worker.ts', import.meta.url))],
    bundle: true,
    format: 'esm',
    platform: 'neutral',
    target: 'es2022',
    mainFields: ['module', 'main'],
    conditions: ['workerd', 'worker', 'browser', 'import'],
    external: ['node:*', 'cloudflare:*', 'postgres', 'ws'],
    write: false,
    logLevel: 'silent',
  })
  return result.outputFiles[0].text
}

interface Socket {
  ws: any
  next(): Promise<any>
  closed: Promise<{ code: number; reason: string }>
  send(data: unknown): void
}

async function connect(
  mf: Miniflare,
  url: string,
  headers: Record<string, string> = {},
): Promise<Socket | Response> {
  const res: any = await mf.dispatchFetch(url, { headers: { upgrade: 'websocket', ...headers } })
  if (!res.webSocket) return res
  const ws = res.webSocket
  const queue: any[] = []
  const waiters: Array<(v: any) => void> = []
  let resolveClosed!: (v: { code: number; reason: string }) => void
  const closed = new Promise<{ code: number; reason: string }>((r) => {
    resolveClosed = r
  })
  ws.addEventListener('message', (e: any) => {
    const v = JSON.parse(e.data)
    const w = waiters.shift()
    if (w) w(v)
    else queue.push(v)
  })
  ws.addEventListener('close', (e: any) => resolveClosed({ code: e.code, reason: e.reason }))
  ws.accept()
  return {
    ws,
    closed,
    send: (data) => ws.send(JSON.stringify(data)),
    next: () =>
      queue.length
        ? Promise.resolve(queue.shift())
        : new Promise((resolve, reject) => {
            const t = setTimeout(() => reject(new Error('no message within 3s')), 3000)
            waiters.push((v) => {
              clearTimeout(t)
              resolve(v)
            })
          }),
  }
}

const base = 'https://example.com'
let script: string

beforeAll(async () => {
  script = await bundle()
}, 60_000)

async function start(withRooms: boolean): Promise<Miniflare> {
  const mf = new Miniflare({
    modules: true,
    script,
    compatibilityDate: '2025-06-01',
    compatibilityFlags: ['nodejs_compat'],
    d1Databases: { DB: `realtime-${crypto.randomUUID()}` },
    ...(withRooms ? { durableObjects: { REALTIME: { className: 'GhuriRoom', useSQLite: true } } } : {}),
  } as any)
  const db = await mf.getD1Database('DB')
  await db.exec(
    'CREATE TABLE edge_note (id INTEGER PRIMARY KEY AUTOINCREMENT, room VARCHAR(50) NOT NULL, text VARCHAR(200) NOT NULL)',
  )
  return mf
}

describe('Durable Object rooms on workerd', () => {
  let mf: Miniflare
  beforeAll(async () => {
    mf = await start(true)
  }, 60_000)
  afterAll(async () => {
    await mf?.dispose()
  })

  it('upgrades through the default MIDDLEWARE stack and runs connect() in the room', async () => {
    const a = (await connect(mf, `${base}/ws/chat/lobby/`)) as Socket
    expect(a.ws).toBeTruthy()
    const hello = await a.next()
    expect(hello).toMatchObject({ type: 'welcome', runtime: 'Cloudflare-Workers' })
    expect(hello.id).toMatch(/^chat\.lobby:/)
    a.ws.close(1000)
  })

  it('fans out between sockets, queries D1 from consumers and keeps per-connection state', async () => {
    const a = (await connect(mf, `${base}/ws/chat/d1/`)) as Socket
    const b = (await connect(mf, `${base}/ws/chat/d1/`)) as Socket
    await a.next()
    await b.next()
    a.send({ text: 'from a' })
    const [ma, mb] = await Promise.all([a.next(), b.next()])
    expect(ma).toEqual({ type: 'chat.message', text: 'from a', id: 1 })
    expect(mb).toEqual(ma)
    a.send({ cmd: 'seen' })
    expect(await a.next()).toEqual({ seen: 2 })
    b.send({ cmd: 'presence' })
    expect(await b.next()).toEqual({ n: 2 })
    const rows = await (await mf.getD1Database('DB')).prepare('SELECT text FROM edge_note').all()
    expect(rows.results).toEqual([{ text: 'from a' }])
    a.ws.close(1000)
    b.ws.close(1000)
  })

  it('reaches rooms over RPC from plain Worker requests and from other rooms', async () => {
    const a = (await connect(mf, `${base}/ws/chat/rpc/`)) as Socket
    await a.next()
    const res = await mf.dispatchFetch(`${base}/announce/rpc/?text=edge`)
    expect(await res.json()).toEqual({ count: 1, last: 'edge' })
    expect(await a.next()).toEqual({ type: 'chat.message', text: 'edge' })
    const b = (await connect(mf, `${base}/ws/chat/elsewhere/`)) as Socket
    await b.next()
    b.send({ cmd: 'other', room: 'rpc', text: 'cross-room' })
    expect(await a.next()).toEqual({ type: 'chat.message', text: 'cross-room' })
    a.ws.close(1000)
    b.ws.close(1000)
  })

  it('passes rejections through as HTTP and closes from the server', async () => {
    const denied = (await connect(mf, `${base}/ws/chat/lobby/?deny=1`)) as Response
    expect(denied.status).toBe(403)
    const plain = await mf.dispatchFetch(`${base}/ws/chat/lobby/`)
    expect(plain.status).toBe(426)
    const evil = (await connect(mf, `${base}/ws/chat/lobby/`, { origin: 'https://evil.example' })) as Response
    expect(evil.status).toBe(403)
    const a = (await connect(mf, `${base}/ws/chat/bye/`)) as Socket
    await a.next()
    a.send({ cmd: 'close' })
    expect(await a.closed).toEqual({ code: 4000, reason: 'bye' })
  })
})

describe('memory layer on Workers', () => {
  let mf: Miniflare
  beforeAll(async () => {
    mf = await start(false)
  }, 60_000)
  afterAll(async () => {
    await mf?.dispose()
  })

  it('refuses with a pointer to durableObjectLayer', async () => {
    const res = (await connect(mf, `${base}/ws/chat/local/`)) as Response
    expect(res.status).toBe(500)
    expect(await res.text()).toContain('durableObjectLayer')
  })
})
