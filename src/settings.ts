import { type Env, tryGetContext } from '@ghuri/core'
import type { Room } from './types'

/** A configured backend: `{ backend, create(env) }`, like database, storage and cache configs. */
export interface LayerConfig {
  backend: string
  create(env: Env): RealtimeLayer
}

export interface ConnectArgs {
  request: Request
  scope: import('./types').Scope
  consumer: import('./consumer').ConsumerClass
  initkwargs: Record<string, unknown>
}

/** Where rooms live. `connect` hands a handshake to the room; `room` reaches a room from anywhere. */
export interface RealtimeLayer {
  readonly backend: string
  connect(args: ConnectArgs): Promise<Response>
  room(name: string): Room
}

export interface RealtimeSettings {
  LAYER?: LayerConfig
  ALLOWED_ORIGINS?: string[]
  TOKEN_SOURCES?: Array<'subprotocol' | 'query'>
  TOKEN_KEYWORD?: string
  MAX_MESSAGE_BYTES?: number
  REVALIDATE_SECONDS?: number
}

export interface ResolvedRealtimeSettings {
  layer: LayerConfig | undefined
  allowedOrigins: string[]
  tokenSources: Array<'subprotocol' | 'query'>
  tokenKeyword: string
  maxMessageBytes: number
  revalidateSeconds: number
}

export function realtimeSettings(): ResolvedRealtimeSettings {
  const s = (tryGetContext()?.settings.REALTIME ?? {}) as RealtimeSettings
  return {
    layer: s.LAYER,
    allowedOrigins: s.ALLOWED_ORIGINS ?? [],
    tokenSources: s.TOKEN_SOURCES ?? ['subprotocol'],
    tokenKeyword: s.TOKEN_KEYWORD ?? 'Bearer',
    maxMessageBytes: s.MAX_MESSAGE_BYTES ?? 1_048_576,
    revalidateSeconds: s.REVALIDATE_SECONDS ?? 0,
  }
}
