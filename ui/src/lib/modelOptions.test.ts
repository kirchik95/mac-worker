import { describe, expect, it } from 'vitest'
import { agentSettings } from '@/test/fixtures'
import { modelOptionsFor } from './modelOptions'

describe('worker model options', () => {
  it('uses the selected Mac catalogue without adding models from another account', () => {
    const setting = { ...agentSettings().agents[0], agent: 'cursor', model: 'worker-model',
      model_options: [{ id: 'worker-model', label: 'Worker model', effort_options: ['high'], fast_supported: true }] }
    expect(modelOptionsFor(setting)).toEqual(setting.model_options)
  })

  it('preserves an unlisted active model and its current capabilities', () => {
    const setting = { ...agentSettings().agents[0], agent: 'cursor', model: 'retired-model',
      effort_options: ['high'], fast_supported: true, model_options: [] }
    expect(modelOptionsFor(setting)).toEqual([
      { id: 'retired-model', label: 'retired-model', effort_options: ['high'], fast_supported: true },
    ])
  })
})
