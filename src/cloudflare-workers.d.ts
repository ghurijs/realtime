// Minimal ambient types for the Workers runtime module, so the package builds without @cloudflare/workers-types
// (whose globals clash with the DOM lib used by the rest of the package).
declare module 'cloudflare:workers' {
  export abstract class DurableObject<Env = unknown> {
    protected ctx: any
    protected env: Env
    constructor(ctx: any, env: Env)
  }
}
