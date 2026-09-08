import { describe, expect, it, vi } from 'vitest'

import { ApiError, getJson, questionOptions, questionText, saveAgentSettings } from './api'

describe('questions', () => {
  it('accepts both shapes the host emits', () => {
    expect(questionText('Which base?')).toBe('Which base?')
    expect(questionOptions('Which base?')).toEqual([])
    expect(questionText({ text: 'Which base?', options: ['main'] })).toBe('Which base?')
    expect(questionOptions({ text: 'Which base?', options: ['main'] })).toEqual(['main'])
  })
})

describe('getJson', () => {
  it('raises a typed error carrying the status', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({ ok: false, status: 503, json: async () => ({}) }),
    )
    await expect(getJson('/api/v1/snapshot')).rejects.toBeInstanceOf(ApiError)
    await expect(getJson('/api/v1/snapshot')).rejects.toMatchObject({ status: 503 })
    vi.unstubAllGlobals()
  })

  it('asks the server not to cache', async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({}) })
    vi.stubGlobal('fetch', fetchMock)
    await getJson('/api/v1/snapshot')
    expect(fetchMock).toHaveBeenCalledWith('/api/v1/snapshot', expect.objectContaining({ cache: 'no-store' }))
    vi.unstubAllGlobals()
  })
})

describe('saveAgentSettings', () => {
  it('sends the browser-safe guard and returns the saved agent', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        agent: 'codex',
        model: 'gpt-6-astra',
        effort: 'xhigh',
        fast: true,
        fast_supported: true,
        effort_options: ['low', 'high', 'xhigh'],
        model_options: [],
        source: 'native-codex',
        revision: 'rev-2',
        writable: true,
        message: null,
      }),
    })
    vi.stubGlobal('fetch', fetchMock)

    const request = {
      agent: 'codex',
      model: 'gpt-6-astra',
      effort: 'xhigh',
      fast: true,
      revision: 'rev-1',
    }
    const saved = await saveAgentSettings('mini-1', request)
    const [, init] = fetchMock.mock.calls[0]

    expect(init.headers).toMatchObject({
      'content-type': 'application/json',
      'x-mac-worker-settings': '1',
    })
    expect(init.headers).not.toHaveProperty('origin')
    expect(JSON.parse(init.body)).toMatchObject({
      agent: 'codex',
      revision: 'rev-1',
    })
    expect(saved).toMatchObject({ agent: 'codex', revision: 'rev-2' })
    vi.unstubAllGlobals()
  })

  it('sends the revision the host checks and reports a rejection', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: false,
      status: 409,
      json: async () => ({ error: { code: 'REVISION_STALE' } }),
    })
    vi.stubGlobal('fetch', fetchMock)

    await expect(
      saveAgentSettings('mini-1', {
        agent: 'codex',
        model: 'gpt-6-astra',
        effort: 'xhigh',
        fast: null,
        revision: 'rev-1',
      }),
    ).rejects.toMatchObject({ status: 409 })

    const [, init] = fetchMock.mock.calls[0]
    expect(JSON.parse(init.body)).toMatchObject({ agent: 'codex', revision: 'rev-1' })
    vi.unstubAllGlobals()
  })
})
