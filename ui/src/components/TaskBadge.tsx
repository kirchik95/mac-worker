import { Check, Clock3, GitBranch, ListTodo, MessageSquareText, TriangleAlert } from 'lucide-react'
import type { TaskRow } from '@/lib/api'
import { taskPresentation } from '@/lib/taskPresentation'
const ICONS = {
  closed: Check,
  running: Clock3,
  queued: ListTodo,
  question: MessageSquareText,
  review: GitBranch,
  error: TriangleAlert,
}
export function TaskBadge({ task }: { task: TaskRow }) {
  const state = taskPresentation(task)
  const Icon = ICONS[state.kind as keyof typeof ICONS]
  return (
    <span className="mw-badge" data-tone={state.tone}>
      <Icon size={13} strokeWidth={1.5} aria-hidden="true" />
      {state.label}
    </span>
  )
}
