import { describe, expect, it, vi } from 'vitest'

import { ApiError, describeError, getJson, integrateTask, questionOptions, questionText, saveAgentSettings, taskMutationForDetail, type TaskDetail } from './api'
import { task } from '@/test/fixtures'

describe('integration actions', () => {
  it('uses the task guard and preserves old-peer unavailable as a typed error', async () => {
    const fetch = vi.fn().mockResolvedValue({ ok: false, status: 503, json: async () => ({
      error: { code: 'INTEGRATION_UNAVAILABLE', message: 'Compatible owner unavailable' },
    }) })
    vi.stubGlobal('fetch', fetch)
    const body = { expected: { expected_task_id: 'task', expected_turn_id: null, expected_turn_count: 1,
      expected_head_oid: null, expected_updated_at_millis: 1, expected_state: 'open' as const },
      expected_integration_id: 'integration', integration: { task_id: 'task', expected: 1, request_id: 'f'.repeat(32) } }
    await expect(integrateTask('task/id', body)).rejects.toMatchObject({ status: 503, code: 'INTEGRATION_UNAVAILABLE' })
    expect(fetch.mock.calls[0][0]).toBe('/api/v1/tasks/task%2Fid/integrate')
    expect(fetch.mock.calls[0][1].headers).toEqual({ 'content-type': 'application/json', 'x-mac-worker-task': '1' })
    expect(JSON.parse(fetch.mock.calls[0][1].body)).toEqual(body)
    vi.unstubAllGlobals()
  })
  it('refuses an unsafe integration revision for every detail mutation', () => {
    const row = task({ state: 'open' })
    const detail = { task: row, timeline: [], turns: [], head_oid: null,
      integration: { integration_id: 'integration', revision: Number.MAX_SAFE_INTEGER + 2 } } as unknown as TaskDetail
    expect(() => taskMutationForDetail(detail, 'preserved draft')).toThrow('Integration revision is unavailable')
  })
  it('keeps disabled mutation bytes and omits integration fences', () => {
    const row = task({ state: 'open' })
    const detail = { task: row, timeline: [], turns: [], head_oid: null } as unknown as TaskDetail
    expect(JSON.stringify(taskMutationForDetail(detail))).toBe(JSON.stringify({
      expected_task_id: row.task_id, expected_turn_id: null, expected_turn_count: row.turn_count,
      expected_head_oid: null, expected_updated_at_millis: row.updated_at_millis, expected_state: row.state,
    }))
  })
})

describe('questions', () => {
  it('accepts both shapes the host emits', () => {
    expect(questionText('Which base?')).toBe('Which base?')
    expect(questionOptions('Which base?')).toEqual([])
    expect(questionText({ text: 'Which base?', options: ['main'] })).toBe('Which base?')
    expect(questionOptions({ text: 'Which base?', options: ['main'] })).toEqual(['main'])
  })
})

describe('describeError', () => {
  it('reads the object the host serializes', () => {
    expect(describeError({ code: 'SSH_UNAVAILABLE', message: 'ssh timed out' })).toEqual({
      code: 'SSH_UNAVAILABLE',
      message: 'ssh timed out',
    })
  })

  it('treats a legacy string as the message', () => {
    expect(describeError('SSH_UNAVAILABLE')).toEqual({ code: null, message: 'SSH_UNAVAILABLE' })
  })

  it('does not throw on junk that is not a valid React child', () => {
    expect(describeError({})).toBeNull()
    expect(describeError({ code: 31 })).toBeNull()
    expect(describeError(null)).toBeNull()
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
