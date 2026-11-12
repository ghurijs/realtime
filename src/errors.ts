import { GhuriError } from '@ghuri/core'

export class RealtimeError extends GhuriError {}

/** A connection's `scope` + `state` + tags no longer fit in 2 KB (Cloudflare's attachment limit). */
export class RealtimeStateTooLarge extends RealtimeError {}

/** A consumer method was used in the wrong phase (e.g. `send()` before `accept()`). */
export class ConsumerStateError extends RealtimeError {}
