import { useEffect, useRef, useState } from 'react'

import { decodeBase64, fetchTurnLog, type LogStream } from '@/lib/api'

const POLL_INTERVAL_MS = 1000

export interface TurnLog {
  text: string
  offset: number
  error: string | null
}

/**
 * Follows one stream of one turn with a byte cursor, exactly as the host serves
 * it. The decoder is kept across polls so a multi-byte character split across a
 * chunk boundary is joined rather than rendered as replacement characters, and
 * it is flushed once when following stops.
 */
export function useTurnLog(
  taskId: string | null,
  turnId: string | null,
  stream: LogStream,
  live: boolean,
): TurnLog {
  const [log, setLog] = useState<TurnLog>({ text: '', offset: 0, error: null })
  const offset = useRef(0)
  const decoder = useRef<TextDecoder | null>(null)

  useEffect(() => {
    offset.current = 0
    decoder.current = new TextDecoder('utf-8')
    setLog({ text: '', offset: 0, error: null })

    if (taskId == null || turnId == null) return
    let cancelled = false
    const controller = new AbortController()

    const poll = async () => {
      try {
        const chunk = await fetchTurnLog(taskId, turnId, stream, offset.current, controller.signal)
        if (cancelled) return
        if (chunk.next_offset > offset.current) {
          const bytes = decodeBase64(chunk.data)
          const text = decoder.current?.decode(bytes, { stream: true }) ?? ''
          offset.current = chunk.next_offset
          setLog((current) => ({
            text: current.text + text,
            offset: chunk.next_offset,
            error: null,
          }))
        }
      } catch (error) {
        if (cancelled || controller.signal.aborted) return
        setLog((current) => ({
          ...current,
          error: error instanceof Error ? error.message : String(error),
        }))
      }
    }

    void poll()
    const timer = live ? setInterval(() => void poll(), POLL_INTERVAL_MS) : null

    return () => {
      cancelled = true
      controller.abort()
      if (timer) clearInterval(timer)
      // Flush whatever partial character the stream ended on, exactly once.
      const tail = decoder.current?.decode() ?? ''
      if (tail) setLog((current) => ({ ...current, text: current.text + tail }))
    }
  }, [taskId, turnId, stream, live])

  return log
}
