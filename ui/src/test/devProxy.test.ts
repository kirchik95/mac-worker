import { describe, expect, it } from 'vitest'

import { dashboardProxyOrigin } from '../../devProxy'

const target = 'http://127.0.0.1:9173'

describe('dashboard development proxy origin', () => {
  it.each([
    ['http://127.0.0.1:5174', '127.0.0.1:5174', false],
    ['http://localhost:5173', 'localhost:5173', false],
    ['http://[::1]:5173', '[::1]:5173', false],
    ['https://localhost:5173', 'localhost:5173', true],
  ])('forwards a same-origin loopback request from %s', (origin, host, encrypted) => {
    expect(dashboardProxyOrigin(origin, host, target, encrypted)).toBe('http://127.0.0.1:9173')
  })

  it.each([
    ['http://attacker.example', '127.0.0.1:5174'],
    ['http://127.0.0.1:5173', '127.0.0.1:5174'],
    ['http://localhost:5174', '127.0.0.1:5174'],
    ['https://127.0.0.1:5174', '127.0.0.1:5174'],
    ['http://attacker.example:5174', 'attacker.example:5174'],
    ['http://127.0.0.1.attacker.example:5174', '127.0.0.1.attacker.example:5174'],
    [undefined, '127.0.0.1:5174'],
    ['null', '127.0.0.1:5174'],
    ['not an origin', '127.0.0.1:5174'],
    ['http://127.0.0.1:5174/', '127.0.0.1:5174'],
    ['http://127.0.0.1:5174/path', '127.0.0.1:5174'],
    ['http://127.0.0.1:5174?query', '127.0.0.1:5174'],
    ['http://127.0.0.1:5174#fragment', '127.0.0.1:5174'],
    ['http://user@127.0.0.1:5174', '127.0.0.1:5174'],
    ['http://127.0.0.1:5174 http://attacker.example', '127.0.0.1:5174'],
    ['http://127.0.0.1:5174', undefined],
    ['http://127.0.0.1:5174', 'user@127.0.0.1:5174'],
    ['http://127.0.0.1:5174', '127.0.0.1:5174/path'],
    ['http://127.0.0.1:5174', '127.0.0.1:5174/'],
    ['http://127.0.0.1:5174', '127.0.0.1:5174?query'],
    ['http://127.0.0.1:5174', '127.0.0.1:5174#fragment'],
    ['http://127.0.0.1:5174', 'not a host'],
  ])('preserves unverified origin %s for host %s', (origin, host) => {
    expect(dashboardProxyOrigin(origin, host, target)).toBe(origin)
  })

  it('uses only the target origin when the configured URL has a path', () => {
    expect(dashboardProxyOrigin('http://localhost:5173', 'localhost:5173', `${target}/api/`)).toBe(
      'http://127.0.0.1:9173',
    )
  })
})
