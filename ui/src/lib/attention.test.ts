import { describe, expect, it } from 'vitest'
import { task } from '@/test/fixtures'
import fixtures from './integration.fixtures.json'
import { decodeIntegrationView } from './integration.contract'
import { needsAttention } from './attention'
import type { TaskState } from './api'

describe('integration attention', () => {
  it.each(fixtures.followups)('uses ordinary attention for $name with a historical receipt', followup => {
    const view = decodeIntegrationView(followup.view)
    const row = { ...task({ state: followup.ordinary_state as TaskState, last_outcome: followup.last_outcome }),
      ...view, integration: decodeIntegrationView({ ...view, integration: followup.history }).integration }
    expect(needsAttention(row)).toBe(view.attention)
  })
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
