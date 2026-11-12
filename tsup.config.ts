import { defineConfig } from 'tsup'

export default defineConfig({
  entry: ['src/index.ts', 'src/cloudflare.ts', 'src/node.ts', 'src/testing.ts'],
  format: ['esm'],
  dts: true,
  clean: true,
  sourcemap: true,
  target: 'es2022',
  removeNodeProtocol: false,
  external: [/^@ghuri\//, /^node:/, 'cloudflare:workers', 'ws', 'hono'],
})
