import { useEffect, useState } from 'react'
import { useControllerEvents } from '@/hooks/ControllerEventsContext'
import { fetchTaskDetail, type TaskDetail, type TaskRow } from '@/lib/api'
import { taskEventKey } from '@/lib/taskPresentation'
export type TaskPreviews = Record<string, TaskDetail | null | undefined>
/** Bounded detail reads; task revisions refresh questions even if an intermediate turn was missed. */
export function useTaskPreviews(tasks: TaskRow[], enabled = true): TaskPreviews {
  const [result, setResult] = useState<{ key: string; previews: TaskPreviews }>({
    key: '',
    previews: {},
  })
  const events = useControllerEvents()
  const key = enabled
    ? tasks.map((task) => `${taskEventKey(task)}:${events.taskEpoch(task.task_id)}`).join(',')
    : ''
  useEffect(() => {
    if (!key) return
    const ids = key.split(',').map((entry) => entry.split(':')[0])
    const controller = new AbortController()
    let next = 0
    const read = async () => {
      while (!controller.signal.aborted && next < ids.length) {
        const id = ids[next++]
        try {
          const detail = await fetchTaskDetail(id, controller.signal)
          if (!controller.signal.aborted)
            setResult((current) => ({
              key,
              previews: { ...(current.key === key ? current.previews : {}), [id]: detail },
            }))
        } catch {
          if (!controller.signal.aborted)
            setResult((current) => ({
              key,
              previews: { ...(current.key === key ? current.previews : {}), [id]: null },
            }))
        }
      }
    }
    for (let i = 0; i < Math.min(6, ids.length); i++) void read()
    return () => controller.abort()
  }, [key])
  return result.key === key ? result.previews : {}
}
