import type { Snapshot, TaskRow } from '@/lib/api'

/** Outcomes that end a turn without ending the work. */
const ATTENTION_OUTCOMES = ['blocked', 'failed', 'timed_out', 'lost', 'needs_input']

/** A task the operator has to look at: still open, or ended a turn badly. */
export const needsAttention = (task: TaskRow) => {
  if (task.integration?.state === 'integrated') return false
  if (task.integration?.state === 'blocked' || (task.integration && task.workflow_state === 'needs_you')) return true
  if (task.integration && ['queued', 'running', 'integrating', 'done'].includes(task.workflow_state ?? '')) return false
  if (task.integration && !['armed', 'revoked'].includes(task.integration.state)) return false
  return task.state === 'open' ||
    (!['closed', 'abandoned'].includes(task.state) &&
      task.last_outcome != null &&
      ATTENTION_OUTCOMES.includes(task.last_outcome.kind))
}

export const attentionCount = (snapshot: Snapshot) => snapshot.tasks.filter(needsAttention).length
