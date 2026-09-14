import type { TaskRow } from '@/lib/api'
export type TaskTone = 'neutral' | 'warning' | 'success' | 'error'
export const needsAnswer = (task: TaskRow) =>
  task.state === 'open' &&
  (task.review_state === 'waiting_on_you' || task.last_outcome?.kind === 'needs_input')
export const readyForReview = (task: TaskRow) =>
  task.state === 'open' && task.review_state === 'ready_for_review'
export function taskPresentation(task: TaskRow): {
  label: string
  tone: TaskTone
  kind: string
  action: string
} {
  if (task.state === 'closed')
    return { label: 'Closed', tone: 'neutral', kind: 'closed', action: 'Open task' }
  if (task.state === 'active')
    return { label: 'Running', tone: 'neutral', kind: 'running', action: 'Open task' }
  if (task.state === 'queued')
    return { label: 'Queued', tone: 'neutral', kind: 'queued', action: 'Open task' }
  if (needsAnswer(task))
    return { label: 'Needs answer', tone: 'warning', kind: 'question', action: 'Answer' }
  if (readyForReview(task))
    return { label: 'Ready for review', tone: 'success', kind: 'review', action: 'Review changes' }
  if (task.state === 'abandoned')
    return { label: 'Abandoned', tone: 'neutral', kind: 'closed', action: 'Open task' }
  if (task.review_state === 'close_pending')
    return { label: 'Closing', tone: 'neutral', kind: 'running', action: 'Open task' }
  if (
    task.state === 'lost' ||
    ['failed', 'blocked', 'timed_out', 'lost'].includes(task.last_outcome?.kind ?? '')
  )
    return { label: 'Needs follow-up', tone: 'error', kind: 'error', action: 'Open task' }
  return { label: 'Open', tone: 'neutral', kind: 'review', action: 'Open task' }
}
export const taskEventKey = (task: TaskRow) =>
  [task.task_id, task.state, task.review_state, task.turn_count, task.updated_at_millis].join(':')

/** Both admitted and parked task rows can report missing capabilities. */
export function setupAgent(task: TaskRow): string | null {
  if (task.state !== 'queued' || !task.blocking_code) return null
  const match = /^(?:NO_WORKER_OFFERS|CAPABILITY_MISSING):(.*)$/.exec(task.blocking_code)
  if (!match) return null
  const agents = match[1]
    .split(',')
    .map((capability) => capability.trim())
    .filter((capability) => capability.startsWith('agent:'))
    .map((capability) => capability.slice(6))
  return agents.includes(task.agent) ? task.agent : (agents[0] ?? null)
}
export function waitingReason(task: TaskRow): string {
  const code = task.blocking_code
  if (!code) return task.state === 'active' ? 'Agent turn in progress' : 'Open the task for details'
  if (code === 'RUN_MAX_PARALLEL') return 'Run concurrency limit reached'
  if (code === 'PINNED_WORKER_BUSY') return 'Waiting for ' + (task.worker ?? 'the pinned Mac')
  if (['WAITING_FOR_DISPATCH', 'CAPACITY_BUSY', 'NO_COMPATIBLE_IDLE_WORKER'].includes(code))
    return 'Waiting for a compatible Mac slot'
  const match = /^(?:NO_WORKER_OFFERS|CAPABILITY_MISSING):(.*)$/.exec(code)
  if (match) return 'Missing capability: ' + match[1].replaceAll('agent:', '')
  return code
}
