import { StrictMode, type ReactNode } from 'react'
import { act, renderHook, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { ControllerEventsProvider } from '@/hooks/ControllerEventsContext'
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

  it('treats a pending snapshot as loading, not offline', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: false,
      status: 503,
      json: async () => ({ error: { code: 'DASHBOARD_SNAPSHOT_PENDING', message: 'pending' } }),
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    const { result } = renderHook(() => useSnapshot())
    await waitFor(() => expect(fetchMock).toHaveBeenCalled())
    expect(result.current.offline).toBe(false)
    expect(result.current.snapshot).toBeNull()
    expect(result.current.error).toBeNull()
  })

  it('keeps generated_at_millis when the collection is stale on HTTP 200', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        json: async () =>
          snapshot({
            generated_at_millis: 3_000,
            collection: { freshness: 'stale', errors: [] },
          }),
      }),
    )
    const { result } = renderHook(() => useSnapshot())
    await waitFor(() => expect(result.current.snapshot).not.toBeNull())
    expect(result.current.offline).toBe(false)
    expect(result.current.snapshot?.collection.freshness).toBe('stale')
    expect(result.current.snapshot?.generated_at_millis).toBe(3_000)
  })
})

class StreamSource {
  closed = false
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

  close() {
    this.closed = true
  }

  emit(name: string, data: string, lastEventId = '') {
    if (this.closed) return
    const event = new MessageEvent(name, { data, lastEventId })
    this.listeners.get(name)?.forEach((listener) => listener(event))
  }

  error() {
    if (this.closed) return
    this.listeners.get('error')?.forEach((listener) => listener(new Event('error')))
  }
}

const JOURNAL = '614dc3be-668f-4922-bd31-b1d7a0056790'
const TASK = 'a'.repeat(32)

function streamHarness() {
  const sources: StreamSource[] = []
  const makeSource = (url: string) => {
    const source = new StreamSource(url)
    sources.push(source)
    return source as unknown as EventSource
  }
  const wrapper = ({ children }: { children: ReactNode }) => (
    <ControllerEventsProvider makeSource={makeSource}>{children}</ControllerEventsProvider>
  )
  return { sources, wrapper }
}

function lifecycleEvent(seq: string) {
  return JSON.stringify({
    schema_version: 1,
    journal_id: JOURNAL,
    seq,
    time_millis: 10,
    kind: 'turn.finished',
    data: { task_id: TASK, outcome: 'needs_input' },
  })
}

describe('snapshot invalidation', () => {
  it('stretches polling to 15s while the stream is healthy', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue({ ok: true, status: 200, json: async () => snapshot() })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    const { sources, wrapper } = streamHarness()
    renderHook(() => useSnapshot(), { wrapper })
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1))

    await act(async () => {
      sources[0].emit('heartbeat', '{}')
    })
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2_000)
    })
    expect(fetchMock).toHaveBeenCalledTimes(1)

    await act(async () => {
      await vi.advanceTimersByTimeAsync(13_000)
    })
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('tunnel_disconnect_immediate_polling', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue({ ok: true, status: 200, json: async () => snapshot() })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    const { sources, wrapper } = streamHarness()
    renderHook(() => useSnapshot(), { wrapper })
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1))
    await act(async () => {
      sources[0].emit('heartbeat', '{}')
    })

    await act(async () => {
      sources[0].error()
    })
    await waitFor(() => expect(fetchMock.mock.calls.length).toBeGreaterThan(1))
    const afterDisconnect = fetchMock.mock.calls.length

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2_000)
    })
    expect(fetchMock.mock.calls.length).toBeGreaterThan(afterDisconnect)

    await act(async () => {
      await vi.advanceTimersByTimeAsync(13_000)
    })
    const paced = fetchMock.mock.calls.length
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2_000)
    })
    expect(fetchMock.mock.calls.length).toBeGreaterThan(paced)
  })

  it('event_then_publication_refetch', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue({ ok: true, status: 200, json: async () => snapshot() })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    const { sources, wrapper } = streamHarness()
    const { result } = renderHook(() => useSnapshot(), { wrapper })
    await waitFor(() => expect(result.current.snapshot?.revision).toBe(7))
    expect(fetchMock).toHaveBeenCalledTimes(1)

    await act(async () => {
      sources[0].emit('controller.event', lifecycleEvent('4'), `${JOURNAL}:4`)
      await vi.advanceTimersByTimeAsync(100)
    })
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))

    await act(async () => {
      sources[0].emit('snapshot.ready', JSON.stringify({ revision: 42 }))
      await vi.advanceTimersByTimeAsync(100)
    })
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3))
    expect(result.current.snapshot?.revision).toBe(7)
    expect(JSON.stringify(result.current.snapshot)).not.toContain('needs_input')
  })

  it('visibility_repairs', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue({ ok: true, status: 200, json: async () => snapshot() })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    const { wrapper } = streamHarness()
    renderHook(() => useSnapshot(), { wrapper })
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1))

    const descriptor = Object.getOwnPropertyDescriptor(document, 'visibilityState')
    Object.defineProperty(document, 'visibilityState', {
      configurable: true,
      get: () => 'hidden',
    })
    await act(async () => {
      document.dispatchEvent(new Event('visibilitychange'))
    })
    expect(fetchMock).toHaveBeenCalledTimes(1)

    Object.defineProperty(document, 'visibilityState', {
      configurable: true,
      get: () => 'visible',
    })
    await act(async () => {
      document.dispatchEvent(new Event('visibilitychange'))
    })
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))
    if (descriptor) Object.defineProperty(document, 'visibilityState', descriptor)
  })
})
