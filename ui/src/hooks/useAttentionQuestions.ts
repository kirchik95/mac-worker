import { useEffect, useState } from 'react'

import { fetchTaskDetail, type Question } from '@/lib/api'

/** A guard on the fan-out: at most this many detail reads run at once. */
export const MAX_ATTENTION_FETCHES = 6

/**
 * Reads the questions of the tasks that are waiting on an answer. The snapshot
 * does not carry them — they live on the task record — so the few tasks that
 * need input are fetched once. A question does not change while the task waits,
 * so this refetches only when the set of waiting tasks does, not on every poll.
 */
export function useAttentionQuestions(
  taskIds: string[],
): Record<string, (string | Question)[] | undefined> {
  const [questions, setQuestions] = useState<Record<string, (string | Question)[] | undefined>>(
    {},
  )
  const key = taskIds.join(',')

  useEffect(() => {
    const ids = key ? key.split(',') : []
    setQuestions({})
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
  }, [key])

  return questions
}
