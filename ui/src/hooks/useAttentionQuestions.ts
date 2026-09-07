import { useEffect, useState } from 'react'

import { fetchTaskDetail, type Question } from '@/lib/api'

/** A guard on the fan-out: the list is one request per task that needs input. */
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
  const [questions, setQuestions] = useState<Record<string, (string | Question)[]>>({})
  const key = taskIds.join(',')

  useEffect(() => {
    const ids = key ? key.split(',').slice(0, MAX_ATTENTION_FETCHES) : []
    if (ids.length === 0) {
      setQuestions({})
      return
    }

    let cancelled = false
    const controller = new AbortController()

    void (async () => {
      const entries = await Promise.all(
        ids.map(async (id) => {
          try {
            const detail = await fetchTaskDetail(id, controller.signal)
            return [id, detail.questions] as const
          } catch {
            // A task that cannot be read keeps its row; the caller distinguishes
            // "not answered yet" from "answered with nothing" by the empty array.
            return [id, []] as const
          }
        }),
      )
      if (!cancelled) setQuestions(Object.fromEntries(entries))
    })()

    return () => {
      cancelled = true
      controller.abort()
    }
  }, [key])

  return questions
}
