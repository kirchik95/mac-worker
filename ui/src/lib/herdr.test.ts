import { describe, expect, it } from 'vitest'

import { herdrChip } from './herdr'

describe('herdrChip', () => {
  it('names an available herdr and the interactive count when it is non-zero', () => {
    expect(herdrChip({ state: 'available', version: '0.9.0', interactive_agents: 2 })).toBe(
      'herdr 0.9.0 · 2 agents',
    )
    expect(herdrChip({ state: 'available', version: '0.9.0', interactive_agents: 1 })).toBe(
      'herdr 0.9.0 · 1 agent',
    )
    expect(herdrChip({ state: 'available', version: '0.9.0', interactive_agents: 0 })).toBe(
      'herdr 0.9.0',
    )
    expect(herdrChip({ state: 'available', version: '0.9.0' })).toBe('herdr 0.9.0')
  })

  it('labels every other fact state the operator can see', () => {
    expect(herdrChip({ state: 'not_installed' })).toBe('no herdr')
    expect(herdrChip({ state: 'no_socket', version: '0.9.0' })).toBe('herdr: no socket')
    expect(herdrChip({ state: 'no_response', version: '0.9.0' })).toBe('herdr: no response')
    expect(herdrChip(null)).toBe('herdr: unknown')
    expect(herdrChip(undefined)).toBe('herdr: unknown')
    expect(herdrChip({ state: 'mystery' })).toBe('herdr: unknown')
  })
})
