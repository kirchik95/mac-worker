import { type ReactNode } from 'react'
import { act, renderHook, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { ControllerEventsProvider } from '@/hooks/ControllerEventsContext'
import { useAttentionQuestions } from './useAttentionQuestions'

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((done, fail) => {
    resolve = done
    reject = fail
  })
  return { promise, resolve, reject }
}

function ok(questions: unknown) {
  return { ok: true as const, status: 200, json: async () => ({ questions }) }
}

function taskIdFrom(url: string) {
  const path = String(url).split('?')[0]
  return decodeURIComponent(path.slice(path.lastIndexOf('/') + 1))
}

afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
})

describe('useAttentionQuestions', () => {
  it('loads more than six waiting ids with at most six active fetches', async () => {
    const ids = Array.from({ length: 8 }, (_, index) => `id-${index + 1}`)
    const id8 = ids[7]
    const pending = new Map(ids.map((id) => [id, deferred<ReturnType<typeof ok>>()]))
    let active = 0
    let maximumActive = 0
    const fetchMock = vi.fn((url: string) => {
      const id = taskIdFrom(url)
      const held = pending.get(id)
      if (!held) return Promise.reject(new Error(`unexpected ${id}`))
      active += 1
      maximumActive = Math.max(maximumActive, active)
      return held.promise.finally(() => {
        active -= 1
      })
    })
    vi.stubGlobal('fetch', fetchMock)

    const { result } = renderHook(() => useAttentionQuestions(ids))
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(6))
    expect(maximumActive).toBe(6)
    expect(Object.keys(result.current)).toHaveLength(0)

    await act(async () => {
      pending.get(ids[0])!.resolve(ok([{ text: 'question 1' }]))
    })
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(7))

    await act(async () => {
      pending.get(ids[1])!.resolve(ok([{ text: 'question 2' }]))
    })
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(8))

    await act(async () => {
      ids.slice(2).forEach((id, index) => {
        pending.get(id)!.resolve(ok([{ text: `question ${index + 3}` }]))
      })
    })

    await waitFor(() => expect(Object.keys(result.current)).toHaveLength(8))
    expect(maximumActive).toBe(6)
    expect(result.current[id8]?.[0]).toMatchObject({ text: 'question 8' })
  })

  it('maps responses to their requested id and leaves failures unloaded', async () => {
    const ids = Array.from({ length: 8 }, (_, index) => `map-${index + 1}`)
    const pending = new Map(ids.map((id) => [id, deferred<ReturnType<typeof ok>>()]))
    const fetchMock = vi.fn((url: string) => pending.get(taskIdFrom(url))!.promise)
    vi.stubGlobal('fetch', fetchMock)

    const { result } = renderHook(() => useAttentionQuestions(ids))
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(6))
    for (const id of ids) expect(result.current[id]).toBeUndefined()

    await act(async () => {
      pending.get(ids[5])!.resolve(ok([{ text: 'question 6' }]))
    })
    await waitFor(() => expect(result.current[ids[5]]?.[0]).toMatchObject({ text: 'question 6' }))
    expect(result.current[ids[0]]).toBeUndefined()

    await act(async () => {
      pending.get(ids[0])!.resolve(ok([]))
    })
    await waitFor(() => expect(result.current[ids[0]]).toEqual([]))

    await act(async () => {
      pending.get(ids[1])!.reject(new Error('unavailable'))
    })
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(8))
    expect(result.current[ids[1]]).toBeUndefined()
    expect(Object.prototype.hasOwnProperty.call(result.current, ids[1])).toBe(false)

    await act(async () => {
      pending.get(ids[2])!.resolve(ok([{ text: 'question 3' }]))
      pending.get(ids[3])!.resolve(ok([{ text: 'question 4' }]))
      pending.get(ids[4])!.resolve(ok([{ text: 'question 5' }]))
      pending.get(ids[6])!.resolve(ok([{ text: 'question 7' }]))
      pending.get(ids[7])!.resolve(ok([{ text: 'question 8' }]))
    })

    await waitFor(() => expect(result.current[ids[7]]?.[0]).toMatchObject({ text: 'question 8' }))
    expect(result.current[ids[5]]?.[0]).toMatchObject({ text: 'question 6' })
    expect(result.current[ids[0]]).toEqual([])
    expect(result.current[ids[1]]).toBeUndefined()
  })

  it('drops a cancelled generation and does not dequeue its remaining ids', async () => {
    const first = Array.from({ length: 8 }, (_, index) => `old-${index + 1}`)
    const next = Array.from({ length: 8 }, (_, index) => `new-${index + 1}`)
    const pending = new Map(
      [...first, ...next].map((id) => [id, deferred<ReturnType<typeof ok>>()]),
    )
    const requested: string[] = []
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      const id = taskIdFrom(url)
      requested.push(id)
      const held = pending.get(id)
      if (!held) return Promise.reject(new Error(`unexpected ${id}`))
      const signal = init?.signal
      if (signal?.aborted) return Promise.reject(new DOMException('Aborted', 'AbortError'))
      signal?.addEventListener('abort', () => {
        held.reject(new DOMException('Aborted', 'AbortError'))
      })
      return held.promise
    })
    vi.stubGlobal('fetch', fetchMock)

    const { result, rerender } = renderHook(({ ids }) => useAttentionQuestions(ids), {
      initialProps: { ids: first },
    })
    await waitFor(() => expect(requested).toHaveLength(6))
    expect(requested).toEqual(first.slice(0, 6))

    rerender({ ids: next })
    await waitFor(() => expect(requested.filter((id) => id.startsWith('new-'))).toHaveLength(6))
    expect(requested.filter((id) => id.startsWith('old-'))).toEqual(first.slice(0, 6))
    expect(result.current).toEqual({})

    await act(async () => {
      pending.get(first[0])!.resolve(ok([{ text: 'stale' }]))
    })
    expect(result.current[first[0]]).toBeUndefined()
    expect(requested.filter((id) => id.startsWith('old-'))).toEqual(first.slice(0, 6))
    expect(requested.filter((id) => id.startsWith('new-'))).toHaveLength(6)
  })

  it('six_fetch_cap', async () => {
    const ids = Array.from({ length: 8 }, (_, index) => `cap-${index + 1}`)
    let active = 0
    let maximumActive = 0
    const pending = deferred<ReturnType<typeof ok>>()
    const fetchMock = vi.fn(() => {
      active += 1
      maximumActive = Math.max(maximumActive, active)
      return pending.promise.finally(() => {
        active -= 1
      })
    })
    vi.stubGlobal('fetch', fetchMock)

    renderHook(() => useAttentionQuestions(ids))
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(6))
    expect(maximumActive).toBe(6)
    expect(fetchMock).toHaveBeenCalledTimes(6)
  })
})

class QuestionSource {
  readonly url: string
  private listeners = new Map<string, Set<(event: Event) => void>>()

  constructor(url: string) {
    this.url = url
  }

  addEventListener(type: string, listener: (event: Event) => void) {
    const set = this.listeners.get(type) ?? new Set()
    set.add(listener)
    this.listeners.set(type, set)
  }

  close() {}

  emit(name: string, data: string, lastEventId = '') {
    const event = new MessageEvent(name, { data, lastEventId })
    this.listeners.get(name)?.forEach((listener) => listener(event))
  }
}

describe('attention questions follow task revisions', () => {
  const taskId = 'a'.repeat(32)
  const journal = '614dc3be-668f-4922-bd31-b1d7a0056790'

  function mount(ids: string[]) {
    const sources: QuestionSource[] = []
    const makeSource = (url: string) => {
      const source = new QuestionSource(url)
      sources.push(source)
      return source as unknown as EventSource
    }
    const wrapper = ({ children }: { children: ReactNode }) => (
      <ControllerEventsProvider makeSource={makeSource}>{children}</ControllerEventsProvider>
    )
    const view = renderHook(() => useAttentionQuestions(ids), { wrapper })
    return { ...view, sources }
  }

  it('same_waiting_ids_new_turn_refetches', async () => {
    const responses = [ok([{ text: 'first question' }]), ok([{ text: 'new turn question' }])]
    let calls = 0
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.resolve(responses[Math.min(calls++, responses.length - 1)])),
    )
    vi.useFakeTimers({ shouldAdvanceTime: true })
    const { result, sources } = mount([taskId])
    await waitFor(() =>
      expect(result.current[taskId]?.[0]).toMatchObject({ text: 'first question' }),
    )

    await act(async () => {
      sources[0].emit(
        'controller.event',
        JSON.stringify({
          schema_version: 1,
          journal_id: journal,
          seq: '9',
          time_millis: 10,
          kind: 'turn.finished',
          data: {
            task_id: taskId,
            turn_id: 'b'.repeat(32),
            run_id: null,
            outcome: 'needs_input',
            code: null,
          },
        }),
        `${journal}:9`,
      )
      await vi.advanceTimersByTimeAsync(100)
    })

    await waitFor(() =>
      expect(result.current[taskId]?.[0]).toMatchObject({ text: 'new turn question' }),
    )
    expect(calls).toBe(2)
  })

  it('stale_detail_or_questions_does_not_overwrite', async () => {
    const first = deferred<ReturnType<typeof ok>>()
    const second = deferred<ReturnType<typeof ok>>()
    const pending = [first, second]
    let calls = 0
    vi.stubGlobal(
      'fetch',
      vi.fn(() => pending[Math.min(calls++, pending.length - 1)].promise),
    )
    vi.useFakeTimers({ shouldAdvanceTime: true })
    const { result, sources } = mount([taskId])
    await waitFor(() => expect(calls).toBe(1))

    await act(async () => {
      sources[0].emit(
        'controller.event',
        JSON.stringify({
          schema_version: 1,
          journal_id: journal,
          seq: '10',
          time_millis: 11,
          kind: 'turn.outcome_changed',
          data: {
            task_id: taskId,
            turn_id: 'b'.repeat(32),
            run_id: null,
            outcome: 'imaginary_outcome',
            code: null,
          },
        }),
        `${journal}:10`,
      )
      await vi.advanceTimersByTimeAsync(100)
    })
    expect(calls).toBe(2)

    await act(async () => {
      first.resolve(ok([{ text: 'stale question' }]))
    })
    expect(result.current[taskId]).toBeUndefined()
    expect(JSON.stringify(result.current)).not.toContain('imaginary_outcome')

    await act(async () => {
      second.resolve(ok([{ text: 'fresh question' }]))
    })
    await waitFor(() =>
      expect(result.current[taskId]?.[0]).toMatchObject({ text: 'fresh question' }),
    )
    expect(JSON.stringify(result.current)).not.toContain('stale question')
    expect(JSON.stringify(result.current)).not.toContain('imaginary_outcome')
  })
})
