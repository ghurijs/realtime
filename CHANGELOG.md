# Changelog

## 0.1.0-alpha.0

- Initial alpha (ARCHITECTURE.md §16).
- `WebSocketConsumer` / `JsonWebSocketConsumer`: `connect`, `receive` / `receiveJson`, `disconnect`, `accept`,
  `send` / `sendJson`, `close`, `tag` / `untag`, `refreshUser`; group events dispatched by `type`; per-connection
  `state` persisted after every handler (2 KB limit with the scope, on every backend).
- Routing with `Consumer.asView()` in `urls.ts`. The handshake runs MIDDLEWARE, an Origin check, REST authentication
  classes (tokens via `Sec-WebSocket-Protocol: bearer.<token>` or, opt-in, `?token=`) and permission classes, then
  picks the room with `static room` (default: the path).
- Rooms: `groupSend`, `broadcast`, `send`, `connections`, `count`, `storage`; `channelLayer` to reach rooms from views,
  signals, cron and queue handlers.
- `durableObjectLayer()` + `createRoomClass(app)` (`@ghuri/realtime/cloudflare`): one Durable Object per room, WebSocket
  Hibernation API with attachments, RPC from Workers and other rooms, ping/pong auto-response.
- `memoryLayer()` for Node and tests; `attachRealtime(server, app)` (`@ghuri/realtime/node`, on `ws`).
- `WebSocketCommunicator` (`@ghuri/realtime/testing`).
