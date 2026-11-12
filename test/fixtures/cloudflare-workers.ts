// Stand-in for the `cloudflare:workers` module in Node tests.
export abstract class DurableObject<Env = unknown> {
  protected ctx: any
  protected env: Env
  constructor(ctx: any, env: Env) {
    this.ctx = ctx
    this.env = env
  }
}
