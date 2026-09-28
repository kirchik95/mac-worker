import { Check, CircleX, Clock3, GitBranch, ListTodo, MessageSquareText, TriangleAlert } from 'lucide-react'
import { Icon as PaperIcon, type IconName } from '@/components/Icon'
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
const PILL_ICONS: Partial<Record<string, IconName>> = {
  running: 'clockOpen',
  queued: 'queue',
}

export function TaskBadge({ task, variant = 'default' }: { task: TaskRow; variant?: 'default' | 'pill' }) {
  const state = taskPresentation(task)
  const abandoned = task.state === 'abandoned'
  const Icon = abandoned ? CircleX : ICONS[state.kind as keyof typeof ICONS]
  const paperIcon = variant === 'pill' ? (abandoned ? 'xCircle' : PILL_ICONS[state.kind]) : undefined
  return (
    <span className="mw-badge mw-task-badge" data-tone={state.tone} data-kind={state.kind} data-variant={variant}>
      {paperIcon ? <PaperIcon name={paperIcon} size={14} /> : (
        <Icon size={variant === 'pill' ? 14 : 13} strokeWidth={1.5} aria-hidden="true" />
      )}
      {state.label}
    </span>
  )
}
