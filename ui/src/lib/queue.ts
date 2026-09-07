import type { IconName } from '@/components/Icon'
import type { Snapshot, Worker } from '@/lib/api'

/**
 * Agent capabilities are derived from facts that expire; see FACTS_TTL in
 * src/agent_facts.rs. A worker whose facts have lapsed still answers the probe
 * but advertises no agent, which is why the pool can go quiet with every
 * machine up.
 */
export const FACTS_TTL_MILLIS = 15 * 60 * 1000

export type BlockingTone = 'attention' | 'muted' | 'bad'

interface Blocking {
  icon: IconName
  tone: BlockingTone
  explain: string
}

/**
 * The codes the host reports; see queue_blocking_code and
 * task_row_blocking_code in src/task_view.rs. A code that is not listed is
 * shown verbatim with no explanation rather than guessed at.
 */
const BLOCKING: Record<string, Blocking> = {
  NO_COMPATIBLE_IDLE_WORKER: {
    icon: 'broadcastOff',
    tone: 'attention',
    explain: 'No idle worker advertises what this entry needs.',
  },
  CAPABILITY_MISSING: {
    icon: 'linkOff',
    tone: 'bad',
    explain: 'No configured worker offers a required capability.',
  },
  PINNED_WORKER_BUSY: {
    icon: 'hourglass',
    tone: 'muted',
    explain: 'The worker this entry is pinned to has one slot and is using it.',
  },
  RUN_MAX_PARALLEL: {
    icon: 'pause',
    tone: 'muted',
    explain: 'Its run already has as many turns in flight as it allows.',
  },
  WAITING_FOR_DISPATCH: {
    icon: 'clock',
    tone: 'muted',
    explain: 'Admitted and waiting for a free slot.',
  },
}

export function blockingDetail(code: string): Blocking & { label: string } {
  const [name, detail] = code.split(/:(.*)/s)
  const known = BLOCKING[name]
  if (known) {
    return {
      ...known,
      label: name,
      explain: detail ? `${known.explain} Missing: ${detail}.` : known.explain,
    }
  }
  return { icon: 'clock', tone: 'muted', label: code, explain: '' }
}

export const TONE_TEXT: Record<BlockingTone, string> = {
  attention: 'text-primary',
  muted: 'text-muted-foreground',
  bad: 'text-destructive',
}

/** Milliseconds since a worker's agent facts were collected, when it reported any. */
export function factsAge(worker: Worker, now: number): number | null {
  const collected = worker.agent_facts?.collected_at_millis
  return collected == null ? null : Math.max(0, now - collected)
}

export const factsLapsed = (worker: Worker, now: number) => {
  const age = factsAge(worker, now)
  return age != null && age > FACTS_TTL_MILLIS
}

/** The agents a worker advertised when its facts were collected. */
export const advertisedAgents = (worker: Worker) =>
  worker.agent_facts?.agents.map((agent) => agent.name) ?? []

export interface Stall {
  waiting: number
  oldestWaitMillis: number
  lapsed: Worker[]
}

/**
 * Describes a queue that is not moving: entries are waiting, no slot is
 * running, and at least one worker's facts have lapsed. Anything short of that
 * is ordinary queueing and gets no banner.
 */
export function stall(snapshot: Snapshot, now: number): Stall | null {
  if (snapshot.queue.length === 0) return null
  if (snapshot.workers.some((worker) => worker.slot.state !== 'idle')) return null
  const lapsed = snapshot.workers.filter((worker) => factsLapsed(worker, now))
  if (lapsed.length === 0) return null
  const oldest = Math.min(...snapshot.queue.map((entry) => entry.created_at_millis))
  return {
    waiting: snapshot.queue.length,
    oldestWaitMillis: Math.max(0, now - oldest),
    lapsed,
  }
}

/** Whole minutes, the unit the artboard prints stall and facts ages in. */
export const minutes = (millis: number) => Math.floor(millis / 60_000)
