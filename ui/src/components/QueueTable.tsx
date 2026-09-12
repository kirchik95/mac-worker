import { Icon } from '@/components/Icon'
import { duration, pad2, shortId } from '@/lib/format'
import { TONE_TEXT, blockingDetail } from '@/lib/queue'
import { cn } from '@/lib/utils'
import { slotBusy, type QueueEntry, type Snapshot } from '@/lib/api'

const KIND_LABEL: Record<string, string> = { batch: 'task', task_turn: 'follow-up' }

function Row({
  entry,
  snapshot,
  now,
}: {
  entry: QueueEntry
  snapshot: Snapshot
  now: number
}) {
  const task = snapshot.tasks.find((row) => row.task_id === entry.task_id)
  const run = snapshot.runs.find((row) => row.run_id === entry.run_id)
  const blocking = blockingDetail(entry.blocking_code)
  const attention = blocking.tone !== 'muted'

  return (
    <tr className={cn('border-t align-top', attention && 'bg-observatory-highlight')}>
      <td className="w-12 py-3.5 pl-5.5 font-mono text-sm">{pad2(entry.position)}</td>
      <td className="min-w-0 py-3.5 pr-5">
        <div className="truncate text-sm">
          {task?.title ?? entry.project_label ?? 'Untitled entry'}
        </div>
        <div className="mt-1 font-mono text-[11px] text-observatory-hollow">
          {shortId(entry.task_id ?? entry.job_id, 12)} ·{' '}
          {KIND_LABEL[entry.entry_kind] ?? entry.entry_kind}
        </div>
      </td>
      <td className="w-49 py-3.5 pr-5">
        {run ? (
          <>
            <div className="truncate font-mono text-xs text-observatory-accent-soft">
              {run.name ?? shortId(run.run_id)}
            </div>
            <div className="mt-1 font-mono text-[11px] text-observatory-hollow">
              {entry.run_max_parallel != null
                ? `max ${entry.run_max_parallel} at once`
                : 'no parallel cap'}
            </div>
          </>
        ) : (
          <>
            <div className="font-mono text-xs text-observatory-hollow">—</div>
            <div className="mt-1 font-mono text-[11px] text-observatory-hollow">
              standalone task
            </div>
          </>
        )}
      </td>
      <td className="w-54 py-3.5 pr-5">
        <div className="truncate font-mono text-xs">
          {entry.requirements[0] ?? (task ? `agent:${task.agent}` : '—')}
        </div>
        <div className="mt-1 truncate font-mono text-[11px] text-observatory-hollow">
          {entry.pinned_worker ? (
            <span className="text-observatory-accent-soft">pinned to {entry.pinned_worker}</span>
          ) : (
            (entry.requirements.slice(1).join(' · ') || 'no other requirement')
          )}
        </div>
      </td>
      <td className="w-85 py-3.5 pr-5">
        <div className={cn('flex items-center gap-2', TONE_TEXT[blocking.tone])}>
          <Icon name={blocking.icon} />
          <span className="font-mono text-[11px] tracking-[0.05em]">{blocking.label}</span>
        </div>
        {blocking.explain ? (
          <p className="mt-1 text-xs text-muted-foreground">{blocking.explain}</p>
        ) : null}
      </td>
      <td
        className={cn(
          'w-20 py-3.5 pr-5.5 text-right font-mono text-xs',
          attention ? 'text-primary' : 'text-muted-foreground',
        )}
      >
        {duration(now - entry.created_at_millis)}
      </td>
    </tr>
  )
}

/**
 * The admission queue as the host projects it: position, what each entry is
 * waiting for, and the code that says why. The pool chooses the worker, so the
 * table never offers one.
 */
export function QueueTable({ snapshot, now = Date.now() }: { snapshot: Snapshot; now?: number }) {
  const running = snapshot.workers.reduce((total, worker) => total + slotBusy(worker.slot), 0)

  return (
    <section className="overflow-hidden rounded-[10px] border bg-card">
      <div className="flex flex-wrap items-baseline gap-x-3.5 gap-y-1 border-b px-5.5 py-3.5">
        <span className="flex items-center gap-2 text-muted-foreground">
          <Icon name="queue" />
          <span className="font-mono text-[11px] tracking-[0.1em]">QUEUE</span>
        </span>
        <span className="text-xs text-observatory-hollow">
          in admission order · the pool picks the worker, never you
        </span>
        <span className="ml-auto font-mono text-[11px] tracking-[0.04em] text-observatory-hollow uppercase">
          {snapshot.queue.length} waiting · {running} running
        </span>
      </div>

      {snapshot.queue.length === 0 ? (
        <p className="px-5.5 py-8 text-center text-sm text-muted-foreground">
          The queue is empty.
        </p>
      ) : (
        <table className="w-full table-fixed border-collapse">
          <thead>
            <tr className="bg-muted/60 text-left font-mono text-[10px] tracking-[0.06em] text-observatory-hollow">
              <th className="w-12 py-2.5 pl-5.5 font-normal">POS</th>
              <th className="py-2.5 pr-5 font-normal">ENTRY</th>
              <th className="w-49 py-2.5 pr-5 font-normal">RUN</th>
              <th className="w-54 py-2.5 pr-5 font-normal">REQUIRES</th>
              <th className="w-85 py-2.5 pr-5 font-normal">WHY IT IS WAITING</th>
              <th className="w-20 py-2.5 pr-5.5 text-right font-normal">WAITED</th>
            </tr>
          </thead>
          <tbody>
            {snapshot.queue.map((entry) => (
              <Row key={entry.job_id} entry={entry} snapshot={snapshot} now={now} />
            ))}
          </tbody>
        </table>
      )}
    </section>
  )
}
