import { type Env, tryGetContext } from '@ghuri/core'
import { currentHost } from './host'
import { memoryLayer } from './memory'
import { type LayerConfig, type RealtimeLayer, realtimeSettings } from './settings'
import type { BroadcastOptions, GroupEvent, Room } from './types'

const EMPTY_ENV: Env = Object.freeze({}) as Env
const cache = new WeakMap<object, Map<LayerConfig, RealtimeLayer>>()
let fallback: LayerConfig | undefined

/** The layer configured in `REALTIME.LAYER` (default: `memoryLayer()`), for the current env. */
export function getLayer(): RealtimeLayer {
  if (!fallback) fallback = memoryLayer()
  const config = realtimeSettings().layer ?? fallback
  const env = tryGetContext()?.env ?? EMPTY_ENV
  let byConfig = cache.get(env)
  if (!byConfig) {
    byConfig = new Map()
    cache.set(env, byConfig)
  }
  let layer = byConfig.get(config)
  if (!layer) {
    layer = config.create(env)
    byConfig.set(config, layer)
  }
  return layer
}

/** The room `name`: local when called from a consumer in that room, otherwise through the layer. */
function room(name: string): Room {
  const host = currentHost()
  if (host && host.room.name === name) return host.room
  return getLayer().room(name)
}

/** Reach rooms from anywhere: views, signal receivers, cron and queue handlers, other consumers. */
export const channelLayer = {
  room,
  groupSend(roomName: string, event: GroupEvent, opts?: BroadcastOptions): Promise<void> {
    return room(roomName).groupSend(event, opts)
  },
  broadcast(roomName: string, message: unknown, opts?: BroadcastOptions): Promise<void> {
    return room(roomName).broadcast(message, opts)
  },
  /** Run a handler on one connection. Connection ids are `<room>:<uuid>`. */
  send(connectionId: string, event: GroupEvent): Promise<void> {
    const i = connectionId.lastIndexOf(':')
    if (i <= 0) throw new TypeError(`Not a connection id: '${connectionId}'`)
    return room(connectionId.slice(0, i)).send(connectionId, event)
  },
}
