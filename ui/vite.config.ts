import path from 'node:path'

import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vitest/config'

// The dashboard API is the Rust loopback server started by `worker dashboard`.
// Proxying keeps the browser same-origin, which matters because that server
// sends no CORS headers and validates the request Host against its listener.
const DASHBOARD_ORIGIN = process.env.DASHBOARD_ORIGIN ?? 'http://127.0.0.1:9173'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: { '@': path.resolve(import.meta.dirname, './src') },
  },
  build: {
    // The Rust server embeds these by name with include_str!/include_bytes!,
    // so the output must not carry content hashes.
    outDir: '../src/dashboard/static/app',
    emptyOutDir: true,
    rollupOptions: {
      output: {
        entryFileNames: 'assets/index.js',
        chunkFileNames: 'assets/[name].js',
        assetFileNames: 'assets/[name][extname]',
      },
    },
  },
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./src/test/setup.ts'],
    include: ['src/**/*.test.{ts,tsx}'],
  },
  server: {
    proxy: {
      '/api': { target: DASHBOARD_ORIGIN, changeOrigin: true },
    },
  },
})
