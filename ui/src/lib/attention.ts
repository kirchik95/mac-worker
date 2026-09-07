import type { Snapshot, TaskRow } from '@/lib/api'

/** Outcomes that end a turn without ending the work. */
const ATTENTION_OUTCOMES = ['blocked', 'failed', 'timed_out', 'lost', 'needs_input']

/** A task the operator has to look at: still open, or ended a turn badly. */
export const needsAttention = (task: TaskRow) =>
  task.state === 'open' ||
  (task.last_outcome != null && ATTENTION_OUTCOMES.includes(task.last_outcome.kind))

export const attentionCount = (snapshot: Snapshot) => snapshot.tasks.filter(needsAttention).length
