import type { Snapshot, TaskRow } from '@/lib/api'
import { currentIntegration } from './taskPresentation'

/** Outcomes that end a turn without ending the work. */
const ATTENTION_OUTCOMES = ['blocked', 'failed', 'timed_out', 'lost', 'needs_input']

/** A task the operator has to look at: still open, or ended a turn badly. */
export const needsAttention = (task: TaskRow) => {
  const integration = currentIntegration(task)
  if (integration?.state === 'integrated') return false
  if (integration?.state === 'blocked' || (integration && task.workflow_state === 'needs_you')) return true
  if (integration && ['queued', 'running', 'integrating', 'done'].includes(task.workflow_state ?? '')) return false
  if (integration && !['armed', 'revoked'].includes(integration.state)) return false
  return task.state === 'open' ||
    (!['closed', 'abandoned'].includes(task.state) &&
      task.last_outcome != null &&
      ATTENTION_OUTCOMES.includes(task.last_outcome.kind))
}

export const attentionCount = (snapshot: Snapshot) => snapshot.tasks.filter(needsAttention).length
