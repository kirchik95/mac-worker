import { act, renderHook, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { snapshot } from '@/test/fixtures'
import { useSnapshot } from './useSnapshot'

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
})
