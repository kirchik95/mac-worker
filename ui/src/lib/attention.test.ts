import { describe, expect, it } from 'vitest'
import { task } from '@/test/fixtures'
import fixtures from './integration.fixtures.json'
import { decodeIntegrationView } from './integration.contract'
import { needsAttention } from './attention'

describe('integration attention', () => {
  it('excludes automatic work and integrated Open/Never tasks, including parked', () => {
    for (const name of ['pending', 'parked_controller_drained_pending', 'integrated']) {
      const view = decodeIntegrationView(fixtures.cases.find(row => row.name === name)!.view)
      expect(needsAttention({ ...task({ state: 'open' }), ...view })).toBe(false)
    }
  })
  it('includes integration blocked and keeps disabled Open tasks actionable', () => {
    const view = decodeIntegrationView(fixtures.cases.find(row => row.view.integration?.state === 'blocked')!.view)
    expect(needsAttention({ ...task({ state: 'open' }), ...view })).toBe(true)
    expect(needsAttention(task({ state: 'open' }))).toBe(true)
  })
  it('retains attention for a terminal dependency failure in an enabled task', () => {
    const view = decodeIntegrationView(fixtures.cases.find(row => row.name === 'armed')!.view)
    expect(needsAttention({ ...task({ state: 'abandoned', blocking_code: 'INTEGRATION_DEPENDENCY_NOT_INTEGRATED' }),
      ...view, workflow_state: 'needs_you' })).toBe(true)
  })
})
