// A Worker with chat consumers: rooms in a Durable Object when env.REALTIME is bound, in-isolate otherwise.
import {
  AuthConfig,
  AuthenticationMiddleware,
  ContentTypesConfig,
  SessionMiddleware,
  SessionsConfig,
} from '@ghuri/auth'
import {
  CommonMiddleware,
  CsrfMiddleware,
  createApp,
  defineSettings,
  json,
  path,
  SecurityMiddleware,
  XFrameOptionsMiddleware,
} from '@ghuri/core'
import { CharField, d1, type Manager, Model } from '@ghuri/orm'
import { createRoomClass, durableObjectLayer } from '../../src/cloudflare'
import { channelLayer, JsonWebSocketConsumer, memoryLayer, type Scope } from '../../src/index'

class Note extends Model({
  room: new CharField({ maxLength: 50 }),
  text: new CharField({ maxLength: 200 }),
}) {
  declare static objects: Manager<Note>
  static Meta = { appLabel: 'edge', ordering: ['id'] }
}

class ChatConsumer extends JsonWebSocketConsumer {
  static override room = (s: Scope) => `chat.${s.params.room}`

  override async connect() {
    if (this.scope.query.deny) return this.close()
    this.accept()
    this.state.seen = 0
    this.sendJson({ type: 'welcome', id: this.scope.connectionId, runtime: navigator.userAgent })
  }

  override async receiveJson(data: any) {
    this.state.seen = (this.state.seen as number) + 1
    if (data.cmd === 'seen') return this.sendJson({ seen: this.state.seen })
    if (data.cmd === 'presence') return this.sendJson({ n: await this.room.count() })
    if (data.cmd === 'close') return this.close(4000, 'bye')
    if (data.cmd === 'other') {
      await channelLayer.groupSend(`chat.${data.room}`, { type: 'chat.message', text: data.text })
      return
    }
    const note = await Note.objects.create({ room: this.scope.params.room, text: data.text })
    await this.room.groupSend({ type: 'chat.message', text: note.text, id: note.pk })
  }

  async chatMessage(e: any) {
    this.sendJson({ type: 'chat.message', text: e.text, id: e.id })
  }
}

const settings = defineSettings((env) => ({
  SECRET_KEY: 'workers-secret',
  DEBUG: true,
  ALLOWED_HOSTS: ['example.com'],
  INSTALLED_APPS: [ContentTypesConfig, AuthConfig, SessionsConfig],
  MIDDLEWARE: [
    SecurityMiddleware,
    SessionMiddleware,
    CommonMiddleware,
    CsrfMiddleware,
    AuthenticationMiddleware,
    XFrameOptionsMiddleware,
  ],
  SESSION_ENGINE: 'signedCookie',
  ROOT_URLCONF: [
    path('ws/chat/<slug:room>/', ChatConsumer.asView(), { name: 'chat' }),
    path('announce/<slug:room>/', async (c) => {
      const room = channelLayer.room(`chat.${c.req.param('room')}`)
      await room.groupSend({ type: 'chat.message', text: c.req.query('text') ?? 'hi' })
      await room.storage.put('last', c.req.query('text') ?? 'hi')
      return json(c, { count: await room.count(), last: await room.storage.get('last') })
    }),
  ],
  DATABASES: { default: d1('DB') },
  REALTIME: { LAYER: env.REALTIME ? durableObjectLayer('REALTIME') : memoryLayer() },
}))

const app = createApp(settings, { checks: false })
export default app
export const GhuriRoom = createRoomClass(app)
