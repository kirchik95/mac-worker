import { useEffect, useRef, useState } from 'react'

import { useControllerEvents } from '@/hooks/ControllerEventsContext'
import { fetchTaskDetail, type Question } from '@/lib/api'

/** A guard on the fan-out: at most this many detail reads run at once. */
export const MAX_ATTENTION_FETCHES = 6

/**
 * Reads the questions of the tasks that are waiting on an answer. The snapshot
 * does not carry them — they live on the task record — so the few tasks that
 * need input are fetched once. The same waiting ids are read again when a lifecycle
 * event bumps that task's revision. At most six reads run at once.
 */
export function useAttentionQuestions(
  taskIds: string[],
): Record<string, (string | Question)[] | undefined> {
  const events = useControllerEvents()
  const [questions, setQuestions] = useState<Record<string, (string | Question)[] | undefined>>(
    {},
  )
  const idsKey = taskIds.join(',')
  const revisionKey = taskIds.map((id) => String(events.taskEpoch(id))).join(',')
  const previousIds = useRef<string | null>(null)

  useEffect(() => {
    const ids = idsKey ? idsKey.split(',') : []
    if (previousIds.current !== idsKey) {
      previousIds.current = idsKey
      setQuestions({})
    }
    if (ids.length === 0) return

    let cancelled = false
    const controller = new AbortController()
    let nextIndex = 0

    const worker = async () => {
      while (!cancelled) {
        const index = nextIndex
        nextIndex += 1
        if (index >= ids.length || cancelled) return
        const id = ids[index]
        try {
          const detail = await fetchTaskDetail(id, controller.signal)
          if (!cancelled) {
            setQuestions((current) => ({
              ...current,
              [id]: detail.questions,
            }))
          }
        } catch {
          // Keep this ID undefined. Do not publish [] for failure.
        }
      }
    }

    const workers = Math.min(MAX_ATTENTION_FETCHES, ids.length)
    for (let started = 0; started < workers; started += 1) void worker()

    return () => {
      cancelled = true
      controller.abort()
    }
  }, [idsKey, revisionKey])

  return questions
}
