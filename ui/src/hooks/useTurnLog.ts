import { useEffect, useRef, useState } from 'react'

import { decodeBase64, fetchTurnLog, type LogStream } from '@/lib/api'

const POLL_INTERVAL_MS = 1000

export interface TurnLog {
  text: string
  offset: number
  error: string | null
}

type ReadOutcome = 'advanced' | 'idle' | 'failed'

/**
 * Follows one stream of one turn with a byte cursor, exactly as the host serves
 * it. The decoder is kept across polls so a multi-byte character split across a
 * chunk boundary is joined rather than rendered as replacement characters, and
 * it is flushed once a completed stream reaches its current end.
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
  const generation = useRef(0)
  const activeRequest = useRef<Promise<void> | null>(null)
  const activeController = useRef<AbortController | null>(null)
  const retryTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  const finalized = useRef(false)
  const liveRef = useRef(live)

  useEffect(() => {
    liveRef.current = live
  }, [live])

  useEffect(() => {
    const identityGeneration = generation.current + 1
    generation.current = identityGeneration
    offset.current = 0
    decoder.current = new TextDecoder('utf-8')
    finalized.current = false
    setLog({ text: '', offset: 0, error: null })

    return () => {
      if (generation.current === identityGeneration) generation.current += 1
      activeController.current?.abort()
      activeController.current = null
      activeRequest.current = null
      if (retryTimer.current !== null) {
        clearTimeout(retryTimer.current)
        retryTimer.current = null
      }
      decoder.current = null
      finalized.current = false
    }
  }, [taskId, turnId, stream])

  useEffect(() => {
    if (taskId == null || turnId == null) return

    const identityGeneration = generation.current
    if (live && finalized.current) {
      finalized.current = false
      decoder.current = new TextDecoder('utf-8')
    }

    const finalizeDecoder = () => {
      if (generation.current !== identityGeneration || finalized.current) return
      finalized.current = true
      const tail = decoder.current?.decode() ?? ''
      decoder.current = null
      if (tail) setLog((current) => ({ ...current, text: current.text + tail }))
    }

    let failDelay = POLL_INTERVAL_MS

    const scheduleRead = (delay: number) => {
      if (generation.current !== identityGeneration || finalized.current) return
      retryTimer.current = setTimeout(() => {
        retryTimer.current = null
        startRead()
      }, delay)
    }

    const handleOutcome = (outcome: ReadOutcome) => {
      if (generation.current !== identityGeneration) return
      if (outcome !== 'failed') failDelay = POLL_INTERVAL_MS
      if (liveRef.current) {
        scheduleRead(POLL_INTERVAL_MS)
      } else if (outcome === 'advanced') {
        startRead()
      } else if (outcome === 'failed') {
        scheduleRead(failDelay)
        failDelay = Math.min(failDelay * 2, 8_000)
      } else {
        finalizeDecoder()
      }
    }

    const startRead = () => {
      if (
        generation.current !== identityGeneration ||
        finalized.current ||
        activeRequest.current !== null
      ) {
        return
      }

      const controller = new AbortController()
      activeController.current = controller
      const request = (async () => {
        let outcome: ReadOutcome
        try {
          const chunk = await fetchTurnLog(
            taskId,
            turnId,
            stream,
            offset.current,
            controller.signal,
          )
          if (controller.signal.aborted || generation.current !== identityGeneration) return

          setLog((current) =>
            current.error === null ? current : { ...current, error: null },
          )
          if (chunk.next_offset > offset.current) {
            if (decoder.current === null) throw new Error('Log decoder is unavailable')
            const bytes = decodeBase64(chunk.data)
            const text = decoder.current.decode(bytes, { stream: true })
            offset.current = chunk.next_offset
            setLog((current) => ({
              text: current.text + text,
              offset: chunk.next_offset,
              error: null,
            }))
            outcome = 'advanced'
          } else {
            outcome = 'idle'
          }
        } catch (error) {
          if (controller.signal.aborted || generation.current !== identityGeneration) return
          setLog((current) => ({
            ...current,
            error: error instanceof Error ? error.message : String(error),
          }))
          outcome = 'failed'
        } finally {
          if (activeController.current === controller) {
            activeController.current = null
            activeRequest.current = null
          }
        }

        handleOutcome(outcome)
      })()
      activeRequest.current = request
    }

    if (retryTimer.current !== null) {
      clearTimeout(retryTimer.current)
      retryTimer.current = null
    }
    startRead()
  }, [taskId, turnId, stream, live])

  return log
}
