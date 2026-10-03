import { describe, expect, it } from 'vitest'
import fixtures from './integration.fixtures.json'
import {
  decodeIntegrationView, parseIntegrationAnnotation, integrationCodes,
} from './integration.contract'

describe('frozen integration public contracts', () => {
  it('loads every shared Rust fixture, preserving disabled omission and public workflow', () => {
    for (const row of fixtures.cases) {
      expect(decodeIntegrationView(row.view)).toEqual(row.view)
    }
    expect(fixtures.cases[0].view).not.toHaveProperty('integration')
    expect(fixtures.cases[0].view).not.toHaveProperty('workflow_state')
    expect(fixtures.codes).toEqual(integrationCodes)
  })

  it('enforces the exact compact annotation byte boundary', () => {
    expect(parseIntegrationAnnotation(fixtures.annotation_boundary_json)).toEqual(fixtures.annotation)
    expect(() => parseIntegrationAnnotation(fixtures.annotation_overflow_json)).toThrow('INTEGRATION_STATE_INVALID')
  })

  it('rejects changed identities, excessive counters, unknown keys and broken park binding', () => {
    const pending = fixtures.cases.find(row => row.name === 'pending')!.view
    for (const patch of [
      { source_head: 'bad' }, { attempts: 4 }, { resolve_turns: 3 }, { verify_turns: 4 },
      { target: 'é'.repeat(65) }, { revision: 0 }, { state: 'parked' },
      { integration_id: 'DF525AFD-647B-82BB-884E-D5EB88429385' }, { origin: 'private' },
    ]) {
      expect(() => decodeIntegrationView({
        ...pending, integration: { ...pending.integration, ...patch },
      })).toThrow('INTEGRATION_STATE_INVALID')
    }
    expect(() => decodeIntegrationView({ ...pending, future: true })).toThrow()
  })
})
