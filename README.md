# @ghuri/realtime

WebSockets for [Ghuri](https://github.com/ghurijs/ghuri), in the style of **Django Channels**: consumers routed in
`urls.ts`, a handshake that runs through your `MIDDLEWARE` and REST authentication classes, and rooms you can reach
from anywhere with `channelLayer`. On Cloudflare Workers each room is a **Durable Object** using the WebSocket
Hibernation API, so idle sockets cost nothing. On Node, rooms live in the process.

> Status: alpha (ARCHITECTURE.md §16). Needs `@ghuri/core` 0.1.0-alpha.1 or later (and `@ghuri/auth` 0.1.0-alpha.1 for
> `this.user.hasPerm()` and `refreshUser()`).

```sh
npm install @ghuri/realtime
```

## Contents

1. [A chat consumer](#1-a-chat-consumer)
2. [Running it on Cloudflare](#2-running-it-on-cloudflare)
3. [Running it on Node](#3-running-it-on-node)
4. [Consumers](#4-consumers)
5. [Rooms and the channel layer](#5-rooms-and-the-channel-layer)
6. [Authentication and permissions](#6-authentication-and-permissions)
7. [Settings](#7-settings)
8. [Testing](#8-testing)
9. [For Django Channels users](#9-for-django-channels-users)

## 1. A chat consumer

```ts
// src/chat/consumers.ts
import { JsonWebSocketConsumer, type Scope } from '@ghuri/realtime'
import { IsAuthenticated } from '@ghuri/rest'
import { Message } from './models'

export class ChatConsumer extends JsonWebSocketConsumer {
  static room = (scope: Scope) => `chat.${scope.params.room}`   // which room owns the socket
  static permissionClasses = [IsAuthenticated]

  async connect() {
    this.accept()
    await this.room.groupSend({ type: 'chat.joined', user: this.user.username }, { exclude: this })
  }

  async receiveJson(data: { text: string }) {
    const msg = await Message.objects.create({ room: this.scope.params.room, author: this.user.pk, text: data.text })
    await this.room.groupSend({ type: 'chat.message', id: msg.pk, text: msg.text, user: this.user.username })
  }

  async chatMessage(event: { id: number; text: string; user: string }) {   // 'chat.message' → chatMessage
    this.sendJson(event)
  }

  async chatJoined(event: { user: string }) {
    this.sendJson({ type: 'joined', user: event.user })
  }
}
```

```ts
// src/chat/urls.ts
import { path } from '@ghuri/core'
import { ChatConsumer } from './consumers'

export const urlpatterns = [path('ws/chat/<slug:room>/', ChatConsumer.asView(), { name: 'chat-socket' })]
```

```js
// browser
const ws = new WebSocket(`wss://${location.host}/ws/chat/lobby/`)
ws.onmessage = (e) => console.log(JSON.parse(e.data))
ws.onopen = () => ws.send(JSON.stringify({ text: 'hello' }))
```

Send to a room from anywhere: a view, a signal receiver, a cron or queue handler.

```ts
import { channelLayer } from '@ghuri/realtime'

await channelLayer.groupSend('chat.lobby', { type: 'chat.message', text: 'Deploying in 5 minutes', user: 'system' })
```

## 2. Running it on Cloudflare

Rooms are hosted by a Durable Object class that you export next to the app:

```ts
// src/index.ts
import { createApp } from '@ghuri/core'
import { createRoomClass } from '@ghuri/realtime/cloudflare'
import settings from './settings'

const app = createApp(settings)
export default app
export const GhuriRoom = createRoomClass(app)
```

```ts
// src/settings.ts
import { durableObjectLayer } from '@ghuri/realtime/cloudflare'
// …
REALTIME: { LAYER: durableObjectLayer('REALTIME') },
```

```jsonc
// wrangler.jsonc
"durable_objects": { "bindings": [{ "name": "REALTIME", "class_name": "GhuriRoom" }] },
"migrations": [{ "tag": "realtime-v1", "new_sqlite_classes": ["GhuriRoom"] }]
```

`wrangler dev` runs Durable Objects locally, so the same setup works in development.

How it works: the upgrade request goes through your MIDDLEWARE and the consumer's authentication and permission
classes in the Worker. The Worker then forwards it to the room's Durable Object, where `connect()` runs. The object
accepts the socket with the Hibernation API and keeps each connection's `scope`, `state` and tags in the socket's
attachment. When a message arrives after the object hibernated, the consumer is rebuilt from that attachment.
Consumers in a room can use D1, R2 and KV: the object has the Worker's bindings. Keep-alive `ping` frames get a
`pong` without waking the object.

A room is one single-threaded object: a few thousand sockets per room is a practical ceiling. Prefer `broadcast`
(no consumer code per socket) over `groupSend` for large rooms.

`memoryLayer()` does not work on Workers (the runtime forbids writing to a socket from another request), and
refuses with an error that points here.

## 3. Running it on Node

```ts
// src/server.ts
import { createApp } from '@ghuri/core'
import { serve } from '@ghuri/core/node'
import { attachRealtime } from '@ghuri/realtime/node'
import settings from './settings'

const app = createApp(settings)
const server = await serve(app, { port: 8000 })
attachRealtime(server, app)
```

With `REALTIME.LAYER` unset (or `memoryLayer()`), rooms live in the process. That is correct for one process.
Several processes or machines need a shared layer (a Postgres `LISTEN/NOTIFY` layer is planned).

## 4. Consumers

| | |
|---|---|
| `connect()` | The handshake arrived. Default: `accept()`. Close, or return without accepting, to reject with 403. |
| `receive({ text?, bytes? })` | A frame arrived. `JsonWebSocketConsumer` parses text into `receiveJson(data)`. |
| `disconnect(code, reason)` | The socket closed (client or server side). Runs once. |
| `accept(subprotocol?)` | Accept the handshake. The default subprotocol is the first of `static subprotocols` the client offered, else `ghuri` when offered. |
| `send(data)` / `sendJson(data)` | Send a frame. |
| `close(code?, reason?)` | Close the socket. |
| `tag(...)` / `untag(...)` / `tags` | Labels inside the room (at most 10), for `{ tag }` filters. |
| `refreshUser()` | Reload the user. Closes with 4401 and returns false if the user is gone or inactive. |
| `scope` | `path`, `params`, `query`, `routeName`, `room`, `connectionId`, `headers`, `user`, `sessionKey`, `auth`. |
| `state` | Your per-connection JSON state, saved after every handler. |
| `user` | The handshake's user. `hasPerm()` loads the full user through `@ghuri/auth`. |
| `room` | The connection's room (section 5). |

Group events are dispatched by `type`: `chat.message`, `chat-message` and `chat_message` all call `chatMessage(event)`.
A missing handler drops the event (with a warning when `DEBUG` is on).

**Consumers are rebuilt for every event.** Fields you set on `this` are gone by the next message; keep what you need
in `this.state`. `scope` + `state` + tags must fit in **2 KB** (the Cloudflare attachment limit), on every backend.
Larger state throws `RealtimeStateTooLarge` and the socket closes with 1011. Put bigger data in `room.storage` or the
database.

An exception in a handler is logged and closes the socket with 1011, like Channels. Frames larger than
`MAX_MESSAGE_BYTES` close it with 1009; invalid JSON in a `JsonWebSocketConsumer` closes it with 1007.

`asView(initkwargs)` assigns `initkwargs` to every instance: `path('ws/a/', Feed.asView({ kind: 'a' }))`.

## 5. Rooms and the channel layer

Every connection belongs to exactly one room, chosen on the handshake by `static room`:

```ts
static room = '/ws/chat/'                                  // a fixed room
static room = (s: Scope) => `chat.${s.params.room}`        // per URL parameter
static room = (s: Scope) => (s.user.isAuthenticated ? `user.${s.user.pk}` : null)   // per user; null → 403
```

Without `static room`, the room is the request path.

```ts
interface Room {
  name: string
  groupSend(event, { tag?, exclude? }?): Promise<void>   // run the handler on each connection
  broadcast(message, { tag?, exclude? }?): Promise<void> // same frame to each socket, no consumer code
  send(connectionId, event): Promise<void>               // one connection
  connections({ tag? }?): Promise<{ id, userPk, tags, connectedAt }[]>
  count({ tag? }?): Promise<number>
  storage: { get, put, delete, list({ prefix?, limit? }) }   // small durable per-room data
}
```

`this.room` inside a consumer; `channelLayer.room(name)` anywhere else. `channelLayer.groupSend(room, event)`,
`channelLayer.broadcast(room, message)` and `channelLayer.send(connectionId, event)` are shortcuts. Connection ids
look like `<room>:<uuid>`.

`groupSend` waits until every handler in the room has run. From a view that should not wait, hand it to
`waitUntil`: `getContext().executionCtx?.waitUntil(channelLayer.groupSend(…))`.

Delivery is at-most-once and there is no replay: a client that reconnects should load what it missed over HTTP.
A socket can't join a second room. Open one socket per room, or send to a per-user room from the server.

## 6. Authentication and permissions

The handshake is an ordinary GET through your MIDDLEWARE, so session users (`SessionMiddleware` +
`AuthenticationMiddleware`) work as they do for pages. Then:

1. **Origin check.** A browser `Origin` must match the request host, `CSRF_TRUSTED_ORIGINS` or
   `REALTIME.ALLOWED_ORIGINS`, else 403. This stops other sites from opening sockets with your users' cookies.
   Clients without `Origin` (servers, mobile apps) pass.
2. **`static authenticationClasses`** (the REST classes, e.g. `TokenAuthentication`, `JWTAuthentication`). Browsers
   can't set `Authorization` on a WebSocket, so send the token as a subprotocol and realtime copies it into
   `Authorization: <TOKEN_KEYWORD> <token>` for the classes:

   ```js
   new WebSocket(url, ['ghuri', `bearer.${token}`])   // the server answers with the 'ghuri' subprotocol
   ```

   `?token=` in the URL works too when `TOKEN_SOURCES` includes `'query'` (tokens in URLs end up in logs).
   Invalid credentials → 401.
3. **`static permissionClasses`** (REST permission classes; they get `request.user` and `request.auth`). Denied → 401
   for anonymous users, 403 otherwise.
4. **`static room`**; `null` → 403.

Users are checked on the handshake only. `await this.refreshUser()` re-checks one, and `REVALIDATE_SECONDS` re-checks
automatically before an event once that many seconds have passed. Logging out does not close open sockets; send to a
per-user room from a `userLoggedOut` receiver if you need that.

## 7. Settings

```ts
REALTIME: {
  LAYER: durableObjectLayer('REALTIME'),  // or memoryLayer() (Node, tests); default memoryLayer()
  ALLOWED_ORIGINS: [],                    // extra browser origins ('https://*.example.com' wildcards work)
  TOKEN_SOURCES: ['subprotocol'],         // and/or 'query'
  TOKEN_KEYWORD: 'Bearer',                // 'Token' for authtoken's TokenAuthentication
  MAX_MESSAGE_BYTES: 1_048_576,           // larger frames close with 1009
  REVALIDATE_SECONDS: 0,                  // 0 = only on the handshake
}
```

## 8. Testing

`@ghuri/realtime/testing` drives consumers without a network, on the memory layer:

```ts
import { WebSocketCommunicator } from '@ghuri/realtime/testing'

const ws = new WebSocketCommunicator(app, '/ws/chat/lobby/', { user: ann, env })
expect(await ws.connect()).toMatchObject({ connected: true })
await ws.sendJson({ text: 'hi' })
expect(await ws.receiveJson()).toMatchObject({ type: 'chat.message', text: 'hi' })
expect(await ws.receiveNothing()).toBe(true)
await ws.disconnect()
```

`user` logs in through the test client's `forceLogin` (from `@ghuri/auth`). Also: `sendText`, `sendBytes`,
`receiveFrom`, `receiveClose()` → `{ code, reason }`, and `connect()` → `{ connected: false, status, body }` for
rejected handshakes. Set `REALTIME.LAYER = memoryLayer()` in test settings.

## 9. For Django Channels users

| Channels | Ghuri |
|---|---|
| `AsyncWebsocketConsumer` / `AsyncJsonWebsocketConsumer` | `WebSocketConsumer` / `JsonWebSocketConsumer` |
| `websocket_urlpatterns` + `URLRouter` | `path('ws/…/', Consumer.asView())` in `urls.ts` |
| `AuthMiddlewareStack` | your `MIDDLEWARE` (runs on the handshake) + `authenticationClasses` |
| `AllowedHostsOriginValidator` | built in (host, `CSRF_TRUSTED_ORIGINS`, `ALLOWED_ORIGINS`) |
| `self.scope['user']` | `this.user` / `this.scope.user` |
| `self.scope['url_route']['kwargs']` | `this.scope.params` |
| `group_add(name)` | `static room` (one room per socket) or `tag(name)` inside the room |
| `channel_layer.group_send(group, event)` | `channelLayer.groupSend(room, event)` / `this.room.groupSend(event)` |
| `channel_layer.send(channel_name, event)` | `channelLayer.send(connectionId, event)` |
| `self.channel_name` | `this.scope.connectionId` |
| `InMemoryChannelLayer` / `RedisChannelLayer` | `memoryLayer()` / `durableObjectLayer()` |
| `WebsocketCommunicator` | `WebSocketCommunicator` |
