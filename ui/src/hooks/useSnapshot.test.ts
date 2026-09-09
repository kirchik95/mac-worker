import { StrictMode } from 'react'
import { act, renderHook, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { snapshot } from '@/test/fixtures'
import { useSnapshot } from './useSnapshot'

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
})

describe('useSnapshot', () => {
  it('publishes the first snapshot it reads', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => snapshot() }),
    )
    const { result } = renderHook(() => useSnapshot())
    await waitFor(() => expect(result.current.snapshot).not.toBeNull())
    expect(result.current.offline).toBe(false)
    expect(result.current.snapshot?.revision).toBe(7)
  })

  it('keeps the last good snapshot when a poll fails, and says it is offline', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce({ ok: true, status: 200, json: async () => snapshot() })
      .mockRejectedValue(new Error('connection refused'))
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    const { result } = renderHook(() => useSnapshot())
    await waitFor(() => expect(result.current.snapshot).not.toBeNull())

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2100)
    })

    await waitFor(() => expect(result.current.offline).toBe(true))
    // Blanking the page on one failed poll would hide the fleet from the operator.
    expect(result.current.snapshot?.revision).toBe(7)
    expect(result.current.error).toContain('connection refused')
  })

  it('stops polling once unmounted', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue({ ok: true, status: 200, json: async () => snapshot() })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    const { result, unmount } = renderHook(() => useSnapshot())
    await waitFor(() => expect(result.current.snapshot).not.toBeNull())
    unmount()
    const after = fetchMock.mock.calls.length

    await act(async () => {
      await vi.advanceTimersByTimeAsync(6000)
    })
    expect(fetchMock.mock.calls.length).toBe(after)
  })

  it('does not start another snapshot request while the previous poll is pending', async () => {
    const pending = deferred<{ ok: true; status: number; json: () => Promise<unknown> }>()
    let inFlight = 0
    let peak = 0
    const fetchMock = vi.fn(() => {
      inFlight += 1
      peak = Math.max(peak, inFlight)
      return pending.promise.finally(() => {
        inFlight -= 1
      })
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    renderHook(() => useSnapshot())
    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(peak).toBe(1)

    await act(async () => {
      await vi.advanceTimersByTimeAsync(6000)
    })
    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(peak).toBe(1)

    await act(async () => {
      pending.resolve({ ok: true, status: 200, json: async () => snapshot() })
    })
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1))

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000)
    })
    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(peak).toBe(1)
  })

  it('retries 2s after a rejected poll and never overlaps', async () => {
    const fetchMock = vi
      .fn()
      .mockRejectedValueOnce(new Error('connection refused'))
      .mockResolvedValue({ ok: true, status: 200, json: async () => snapshot() })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    const { result } = renderHook(() => useSnapshot())
    await waitFor(() => expect(result.current.offline).toBe(true))
    expect(fetchMock).toHaveBeenCalledTimes(1)

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000)
    })
    await waitFor(() => expect(result.current.offline).toBe(false))
    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(result.current.snapshot?.revision).toBe(7)
  })

  it('does not apply or reschedule a pending poll after unmount', async () => {
    const pending = deferred<{ ok: true; status: number; json: () => Promise<unknown> }>()
    const fetchMock = vi.fn(() => pending.promise)
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    const { result, unmount } = renderHook(() => useSnapshot())
    expect(fetchMock).toHaveBeenCalledTimes(1)
    unmount()

    await act(async () => {
      pending.resolve({ ok: true, status: 200, json: async () => snapshot() })
      await vi.advanceTimersByTimeAsync(6000)
    })
    expect(result.current.snapshot).toBeNull()
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('does not let a StrictMode stale generation schedule after remount', async () => {
    const queue: Array<
      ReturnType<typeof deferred<{ ok: true; status: number; json: () => Promise<unknown> }>>
    > = []
    const fetchMock = vi.fn(() => {
      const item = deferred<{ ok: true; status: number; json: () => Promise<unknown> }>()
      queue.push(item)
      return item.promise
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    renderHook(() => useSnapshot(), { wrapper: StrictMode })
    expect(queue.length).toBeGreaterThanOrEqual(1)
    const started = fetchMock.mock.calls.length
    const stale = queue[0]

    await act(async () => {
      stale.resolve({
        ok: true,
        status: 200,
        json: async () => snapshot({ revision: 1 }),
      })
      await vi.advanceTimersByTimeAsync(6000)
    })
    expect(fetchMock.mock.calls.length).toBe(started)
  })
})
