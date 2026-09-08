import { act, renderHook, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { MAX_LOG_CHUNK_BYTES, type LogStream } from '@/lib/api'
import { useTurnLog } from './useTurnLog'

const b64 = (bytes: number[]) => btoa(String.fromCharCode(...bytes))
const byteLength = (text: string) => new TextEncoder().encode(text).byteLength

function response(body: {
  stream: LogStream
  offset: number
  next_offset: number
  data: string
}) {
  return { ok: true, status: 200, json: async () => body }
}

function textChunk(stream: LogStream, offset: number, text: string) {
  return response({
    stream,
    offset,
    next_offset: offset + byteLength(text),
    data: btoa(text),
  })
}

function emptyChunk(stream: LogStream, offset: number) {
  return response({ stream, offset, next_offset: offset, data: '' })
}

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

const requestOffset = (url: string) =>
  Number(new URL(url, window.location.origin).searchParams.get('offset'))

afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
})

describe('useTurnLog', () => {
  it('drains every bounded block of a completed stream to its current end', async () => {
    const blocks = [
      'a'.repeat(MAX_LOG_CHUNK_BYTES),
      'b'.repeat(MAX_LOG_CHUNK_BYTES),
      'tail',
    ]
    const completeText = blocks.join('')
    const requestedOffsets: number[] = []
    const fetchMock = vi.fn(async (url: string) => {
      const offset = requestOffset(url)
      requestedOffsets.push(offset)
      if (offset === 0) return textChunk('stdout', offset, blocks[0])
      if (offset === MAX_LOG_CHUNK_BYTES) return textChunk('stdout', offset, blocks[1])
      if (offset === 2 * MAX_LOG_CHUNK_BYTES) return textChunk('stdout', offset, blocks[2])
      return emptyChunk('stdout', offset)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    const { result } = renderHook(() => useTurnLog('task', 'turn', 'stdout', false))
    await waitFor(() => expect(result.current.text).toBe(completeText))

    expect(result.current.offset).toBe(2 * MAX_LOG_CHUNK_BYTES + 4)
    expect(requestedOffsets).toEqual([
      0,
      MAX_LOG_CHUNK_BYTES,
      2 * MAX_LOG_CHUNK_BYTES,
      2 * MAX_LOG_CHUNK_BYTES + 4,
    ])
  })

  it('preserves one active request and its decoder when live becomes completed', async () => {
    const prefix = 'prefix'
    const tail = deferred<ReturnType<typeof textChunk>>()
    const requestedOffsets: number[] = []
    const fetchMock = vi.fn((url: string) => {
      const offset = requestOffset(url)
      requestedOffsets.push(offset)
      if (offset === 0) return Promise.resolve(textChunk('stdout', offset, prefix))
      if (offset === byteLength(prefix)) return tail.promise
      return Promise.resolve(emptyChunk('stdout', offset))
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    const { result, rerender } = renderHook(
      ({ live }) => useTurnLog('task', 'turn', 'stdout', live),
      { initialProps: { live: true } },
    )
    await waitFor(() => expect(result.current.text).toBe(prefix))

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1000)
    })
    expect(fetchMock).toHaveBeenCalledTimes(2)

    rerender({ live: false })
    expect(fetchMock).toHaveBeenCalledTimes(2)

    tail.resolve(textChunk('stdout', byteLength(prefix), 'TAIL'))
    await waitFor(() => expect(result.current.text).toBe('prefixTAIL'))
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3))
    expect(requestedOffsets).toEqual([0, byteLength(prefix), byteLength('prefixTAIL')])
  })

  it('resumes a finalized non-live stream with its text, cursor, and split UTF-8 intact', async () => {
    const prefix = 'prefix'
    const prefixBytes = byteLength(prefix)
    const requestedOffsets: number[] = []
    const fetchMock = vi.fn(async (url: string) => {
      const offset = requestOffset(url)
      requestedOffsets.push(offset)
      switch (requestedOffsets.length) {
        case 1:
          return textChunk('stdout', offset, prefix)
        case 2:
          return emptyChunk('stdout', offset)
        case 3:
          return response({
            stream: 'stdout',
            offset,
            next_offset: offset + 1,
            data: b64([0xc3]),
          })
        default:
          return response({
            stream: 'stdout',
            offset,
            next_offset: offset + 1,
            data: b64([0xa9]),
          })
      }
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    const { result, rerender } = renderHook(
      ({ live }) => useTurnLog('task', 'turn', 'stdout', live),
      { initialProps: { live: false } },
    )
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))
    expect(result.current.text).toBe(prefix)
    expect(result.current.offset).toBe(prefixBytes)

    rerender({ live: true })
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3))
    await waitFor(() => expect(result.current.offset).toBe(prefixBytes + 1))
    expect(result.current.text).toBe(prefix)
    expect(result.current.text).not.toContain('\uFFFD')

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1000)
    })
    await waitFor(() => expect(result.current.text).toBe(`${prefix}é`))
    expect(result.current.offset).toBe(prefixBytes + 2)
    expect(requestedOffsets).toEqual([0, prefixBytes, prefixBytes, prefixBytes + 1])
  })

  it('joins a UTF-8 scalar split across completed chunks before finalizing once', async () => {
    const fetchMock = vi.fn(async (url: string) => {
      const offset = requestOffset(url)
      if (offset === 0) {
        return response({ stream: 'stdout', offset, next_offset: 1, data: b64([0xc3]) })
      }
      if (offset === 1) {
        return response({ stream: 'stdout', offset, next_offset: 2, data: b64([0xa9]) })
      }
      return emptyChunk('stdout', offset)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    const { result } = renderHook(() => useTurnLog('task', 'turn', 'stdout', false))
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3))

    expect(result.current.text).toBe('é')
    expect(result.current.text).not.toContain('\uFFFD')
    expect(result.current.offset).toBe(2)
  })

  it('never overlaps reads when a live request remains blocked across timer ticks', async () => {
    const first = deferred<ReturnType<typeof emptyChunk>>()
    const fetchMock = vi.fn(() => first.promise)
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    renderHook(() => useTurnLog('task', 'turn', 'stdout', true))
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1))

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000)
    })
    expect(fetchMock).toHaveBeenCalledTimes(1)

    first.resolve(emptyChunk('stdout', 0))
    await act(async () => {
      await Promise.resolve()
    })
  })

  it.each([
    ['task', { taskId: 'task-2', turnId: 'turn-1', stream: 'stdout' as const }],
    ['turn', { taskId: 'task-1', turnId: 'turn-2', stream: 'stdout' as const }],
    ['stream', { taskId: 'task-1', turnId: 'turn-1', stream: 'stderr' as const }],
  ])('isolates a stale partial decoder when %s identity changes', async (_label, next) => {
    const oldTail = deferred<ReturnType<typeof response>>()
    const requests: string[] = []
    const fetchMock = vi.fn((url: string) => {
      requests.push(url)
      const offset = requestOffset(url)
      const isOld =
        url.includes('/tasks/task-1/') &&
        url.includes('/turns/turn-1/') &&
        url.includes('stream=stdout')
      if (isOld && offset === 0) {
        return Promise.resolve(
          response({ stream: 'stdout', offset, next_offset: 1, data: b64([0xc3]) }),
        )
      }
      if (isOld) return oldTail.promise
      const stream = new URL(url, window.location.origin).searchParams.get('stream') as LogStream
      if (offset === 0) return Promise.resolve(textChunk(stream, offset, 'new'))
      return Promise.resolve(emptyChunk(stream, offset))
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    const initialIdentity: { taskId: string; turnId: string; stream: LogStream } = {
      taskId: 'task-1',
      turnId: 'turn-1',
      stream: 'stdout',
    }
    const { result, rerender } = renderHook(
      (props: { taskId: string; turnId: string; stream: LogStream }) =>
        useTurnLog(props.taskId, props.turnId, props.stream, false),
      { initialProps: initialIdentity },
    )
    await waitFor(() => expect(result.current.offset).toBe(1))

    rerender(next)
    await waitFor(() => expect(result.current.text).toBe('new'))
    expect(result.current.offset).toBe(3)
    expect(result.current.text).not.toContain('\uFFFD')
    expect(requests.at(-2)).toContain('offset=0')

    oldTail.resolve(
      response({ stream: 'stdout', offset: 1, next_offset: 2, data: b64([0xa9]) }),
    )
    await act(async () => {
      await Promise.resolve()
    })
    expect(result.current.text).toBe('new')
  })

  it('keeps stdout and stderr text and byte offsets independent', async () => {
    const requested = { stdout: [] as number[], stderr: [] as number[] }
    const fetchMock = vi.fn(async (url: string) => {
      const parsed = new URL(url, window.location.origin)
      const stream = parsed.searchParams.get('stream') as LogStream
      const offset = requestOffset(url)
      requested[stream].push(offset)
      if (offset === 0) {
        return textChunk(stream, offset, stream === 'stdout' ? 'out' : 'error')
      }
      if (offset === (stream === 'stdout' ? 3 : 5)) return textChunk(stream, offset, '!')
      return emptyChunk(stream, offset)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    const { result } = renderHook(() => ({
      stdout: useTurnLog('task', 'turn', 'stdout', false),
      stderr: useTurnLog('task', 'turn', 'stderr', false),
    }))
    await waitFor(() => {
      expect(result.current.stdout.text).toBe('out!')
      expect(result.current.stderr.text).toBe('error!')
    })

    expect(result.current.stdout.offset).toBe(4)
    expect(result.current.stderr.offset).toBe(6)
    expect(requested.stdout).toEqual([0, 3, 4])
    expect(requested.stderr).toEqual([0, 5, 6])
  })

  it('retries a failed completed read and clears the error on no-progress success', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(textChunk('stdout', 0, 'kept bytes'))
      .mockResolvedValueOnce({ ok: false, status: 503, json: async () => ({}) })
      .mockResolvedValueOnce(emptyChunk('stdout', byteLength('kept bytes')))
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers()

    const { result } = renderHook(() => useTurnLog('task', 'turn', 'stdout', false))
    await act(async () => {
      for (let index = 0; index < 10; index += 1) await Promise.resolve()
    })
    expect(result.current.error).toContain('503')
    expect(result.current.text).toBe('kept bytes')
    expect(fetchMock).toHaveBeenCalledTimes(2)

    await act(async () => {
      await vi.advanceTimersByTimeAsync(999)
    })
    expect(fetchMock).toHaveBeenCalledTimes(2)

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1)
      for (let index = 0; index < 5; index += 1) await Promise.resolve()
    })
    expect(result.current.error).toBeNull()
    expect(result.current.text).toBe('kept bytes')
    expect(fetchMock).toHaveBeenCalledTimes(3)
  })

  it('requests the bounded host window', async () => {
    const fetchMock = vi.fn(async (_url: string) => emptyChunk('stdout', 0))
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    renderHook(() => useTurnLog('task', 'turn', 'stdout', false))
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1))
    expect(fetchMock.mock.calls[0][0]).toContain(`limit=${MAX_LOG_CHUNK_BYTES}`)
  })
})
