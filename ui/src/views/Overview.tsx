import { ActiveTurn } from '@/components/ActiveTurn'
import { AttentionStrip } from '@/components/AttentionStrip'
import { Capabilities } from '@/components/Capabilities'
import { QueueTable } from '@/components/QueueTable'
import { RunProgress } from '@/components/RunProgress'
import { StalledBanner } from '@/components/StalledBanner'
import { WorkerCard } from '@/components/WorkerCard'
import { stall } from '@/lib/queue'
import type { Snapshot } from '@/lib/api'

export function Overview({
  snapshot,
  now = Date.now(),
  onShowAttention,
}: {
  snapshot: Snapshot
  now?: number
  onShowAttention?: () => void
}) {
  const busy = snapshot.workers.filter((worker) => worker.active_task != null)
  const runs = snapshot.runs.filter((run) => run.progress.total > 0)
  const stalled = stall(snapshot, now)
  const unreachable = snapshot.workers
    .filter((worker) => worker.health === 'unavailable')
    .map((worker) => worker.name)

  return (
    <div className="space-y-5">
      {stalled ? (
        <StalledBanner stall={stalled} unreachable={unreachable} now={now} />
      ) : null}

      {snapshot.workers.length > 0 ? (
        <div className="grid gap-5 md:grid-cols-2 xl:grid-cols-3">
          {snapshot.workers.map((worker) => (
            <WorkerCard key={worker.name} worker={worker} now={now} />
          ))}
        </div>
      ) : (
        <p className="text-sm text-muted-foreground">No workers are configured.</p>
      )}

      {busy.length > 0 ? (
        <div className="space-y-5">
          {busy.map((worker) => {
            const row = snapshot.tasks.find((task) => task.task_id === worker.active_task?.task_id)
            return (
              <ActiveTurn
                key={worker.name}
                worker={worker}
                row={row}
                run={snapshot.runs.find((entry) => entry.run_id === row?.run_id)}
              />
            )
          })}
        </div>
      ) : (
        <section className="flex min-h-32 items-center justify-center rounded-[10px] border bg-card">
          <p className="text-sm text-muted-foreground">No turn is running.</p>
        </section>
      )}

      <div>
        <QueueTable snapshot={snapshot} now={now} />
        <AttentionStrip snapshot={snapshot} onShowAttention={onShowAttention} />
      </div>

      {runs.length > 0 ? (
        <div className="grid gap-5 md:grid-cols-2 xl:grid-cols-3">
          {runs.map((run) => (
            <RunProgress key={run.run_id} run={run} />
          ))}
        </div>
      ) : null}

      <Capabilities snapshot={snapshot} now={now} />
    </div>
  )
}
