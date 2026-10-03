import { describe, expect, it } from 'vitest'
import { task } from '@/test/fixtures'
import fixtures from './integration.fixtures.json'
import { decodeIntegrationView } from './integration.contract'
import { readyForReview, taskPresentation, taskEventKey, waitingReason } from './taskPresentation'

const integrated = () => ({ ...task({ state: 'open', review_state: 'ready_for_review' }),
  ...decodeIntegrationView(fixtures.cases.find(row => row.name === 'integrated')!.view) })
describe('integration presentation', () => {
  it('shows integrated done and no review demand despite legacy Open', () => {
    expect(readyForReview(integrated())).toBe(false)
    expect(taskPresentation(integrated()).label).toBe('Integrated')
  })
  it('shows parked reason and gives blocked work its recovery action', () => {
    const parked = { ...task({ state: 'open' }),
      ...decodeIntegrationView(fixtures.cases.find(row => row.name === 'parked_controller_drained_pending')!.view) }
    expect(taskPresentation(parked).label).toBe('Integration paused')
    expect(waitingReason(parked)).toContain(parked.integration!.pause_reason!)
    const blocked = { ...integrated(), integration: { ...integrated().integration!, state: 'blocked' as const,
      blocked_code: 'INTEGRATION_CHECKS_FAILED' as const }, workflow_state: 'needs_you' as const }
    expect(taskPresentation(blocked).action).toBe('Recover integration')
  })
  it('changes the event key when only integration revision changes', () => {
    const row = integrated()
    expect(taskEventKey(row)).not.toBe(taskEventKey({ ...row,
      integration: { ...row.integration!, revision: row.integration!.revision + 1 } }))
  })
})
