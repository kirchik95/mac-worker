import { useEffect, useRef } from 'react'
import { Bell, GitBranch, MessageSquareText } from 'lucide-react'
import { toast } from 'sonner'
import type { Snapshot, TaskRow } from '@/lib/api'
import { needsAnswer, readyForReview } from '@/lib/taskPresentation'

const TOAST_ID = 'mac-worker:attention'
const eventKey = (task: TaskRow) =>
  `${task.task_id}:${task.turn_count}:${needsAnswer(task) ? 'question' : 'review'}`

/** Announce new actionable turns, not snapshot refreshes or initial history. */
export function useAttentionToasts({
  snapshot,
  offline,
  activeTaskId,
  panelOpen,
  onSelectTask,
  onOpenNotifications,
}: {
  snapshot: Snapshot | null
  offline: boolean
  activeTaskId?: string
  panelOpen: boolean
  onSelectTask: (id: string) => void
  onOpenNotifications: () => void
}) {
  const seen = useRef<Set<string> | null>(null)
  const unavailable = useRef(false)
  const staleTaskIds = useRef(new Set<string>())
  const announced = useRef<string[]>([])

  useEffect(() => {
    const available = !offline && snapshot?.collection.freshness === 'current'
    const tasks = snapshot?.tasks.filter((task) => needsAnswer(task) || readyForReview(task)) ?? []
    const eligible = tasks.filter(
      (task) =>
        task.freshness === 'current' &&
        task.task_id !== activeTaskId &&
        !staleTaskIds.current.has(task.task_id),
    )
    if (
      announced.current.length &&
      (!available ||
        panelOpen ||
        !eligible.some((task) => announced.current.includes(eventKey(task))))
    ) {
      toast.dismiss(TOAST_ID)
      announced.current = []
    }
    if (!snapshot) {
      unavailable.current = true
      return
    }

    const fresh = eligible.filter((task) => !seen.current?.has(eventKey(task)))
    const silent = !seen.current || unavailable.current || !available || panelOpen
    const history = seen.current ?? new Set<string>()
    tasks.forEach((task) => history.add(eventKey(task)))
    // Collection freshness and task freshness are independent. Seed a recovered
    // task silently even if its first current observation contains a new turn.
    staleTaskIds.current = new Set(
      snapshot.tasks.filter((task) => task.freshness !== 'current').map((task) => task.task_id),
    )
    // Retain current events even when a very large pool exceeds the history cap.
    seen.current = new Set([...Array.from(history).slice(-500), ...tasks.map(eventKey)])
    unavailable.current = !available
    if (silent || !fresh.length) return

    announced.current = fresh.map(eventKey)
    const first = fresh[0]
    const multiple = fresh.length > 1
    const question = needsAnswer(first)
    toast(
      multiple
        ? `${fresh.length} tasks need your attention`
        : question
          ? 'Task needs your answer'
          : 'Ready for review',
      {
        id: TOAST_ID,
        duration: 6000,
        description: multiple
          ? 'New questions and results are in the notification center.'
          : first.title,
        icon: multiple ? (
          <Bell size={18} />
        ) : question ? (
          <MessageSquareText size={18} className="text-warning" />
        ) : (
          <GitBranch size={18} className="text-success" />
        ),
        action: {
          label: multiple ? 'View notifications' : question ? 'Answer' : 'Review changes',
          onClick: multiple ? onOpenNotifications : () => onSelectTask(first.task_id),
        },
      },
    )
  }, [snapshot, offline, activeTaskId, panelOpen, onSelectTask, onOpenNotifications])

  useEffect(
    () => () => {
      toast.dismiss(TOAST_ID)
    },
    [],
  )
}
