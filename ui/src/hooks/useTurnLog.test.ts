import { act, renderHook, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { useTurnLog } from './useTurnLog'

const b64 = (bytes: number[]) => btoa(String.fromCharCode(...bytes))

afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
})

describe('useTurnLog', () => {
  it('advances the byte cursor and appends what the host returned', async () => {
    const chunks = [
      { stream: 'stdout', offset: 0, next_offset: 5, data: btoa('hello') },
      { stream: 'stdout', offset: 5, next_offset: 11, data: btoa(' world') },
    ]
    const fetchMock = vi.fn(async (url: string) => ({
      ok: true,
      status: 200,
      json: async () => (url.includes('offset=0') ? chunks[0] : chunks[1]),
    }))
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    const { result } = renderHook(() => useTurnLog('task', 'turn', 'stdout', true))
    await waitFor(() => expect(result.current.text).toBe('hello'))

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1100)
    })
    await waitFor(() => expect(result.current.text).toBe('hello world'))
    expect(result.current.offset).toBe(11)
    expect(fetchMock.mock.calls[1][0]).toContain('offset=5')
  })

  it('joins a character split across two chunks instead of corrupting it', async () => {
    // "é" is 0xC3 0xA9; the host may end a chunk between the two bytes.
    const chunks = [
      { stream: 'stdout', offset: 0, next_offset: 1, data: b64([0xc3]) },
      { stream: 'stdout', offset: 1, next_offset: 2, data: b64([0xa9]) },
    ]
    vi.stubGlobal(
      'fetch',
      vi.fn(async (url: string) => ({
        ok: true,
        status: 200,
        json: async () => (url.includes('offset=0') ? chunks[0] : chunks[1]),
      })),
    )
    vi.useFakeTimers({ shouldAdvanceTime: true })

    const { result } = renderHook(() => useTurnLog('task', 'turn', 'stdout', true))
    await waitFor(() => expect(result.current.offset).toBe(1))
    // The first half alone must not render as a replacement character.
    expect(result.current.text).toBe('')

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1100)
    })
    await waitFor(() => expect(result.current.text).toBe('é'))
  })

  it('asks for a bounded window and stops polling when the turn is not live', async () => {
    const fetchMock = vi.fn(async (_url: string) => ({
      ok: true,
      status: 200,
      json: async () => ({ stream: 'stdout', offset: 0, next_offset: 0, data: '' }),
    }))
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    renderHook(() => useTurnLog('task', 'turn', 'stdout', false))
    await waitFor(() => expect(fetchMock).toHaveBeenCalled())
    expect(fetchMock.mock.calls[0][0]).toContain('limit=65536')

    const calls = fetchMock.mock.calls.length
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000)
    })
    expect(fetchMock.mock.calls.length).toBe(calls)
  })

  it('reports a read failure without discarding what it already showed', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({ stream: 'stdout', offset: 0, next_offset: 4, data: btoa('done') }),
      })
      .mockResolvedValue({ ok: false, status: 503, json: async () => ({}) })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    const { result } = renderHook(() => useTurnLog('task', 'turn', 'stdout', true))
    await waitFor(() => expect(result.current.text).toBe('done'))

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1100)
    })
    await waitFor(() => expect(result.current.error).toContain('503'))
    expect(result.current.text).toBe('done')
  })
})
