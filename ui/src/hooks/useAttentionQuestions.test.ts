import { act, renderHook, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

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

afterEach(() => vi.unstubAllGlobals())

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
})
