import { ChevronRight } from 'lucide-react'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { ActiveTurn } from '@/components/ActiveTurn'
import { AttentionCards } from '@/components/AttentionCards'
import { Capabilities } from '@/components/Capabilities'
import { QueueTable } from '@/components/QueueTable'
import { StalledBanner } from '@/components/StalledBanner'
import { WorkerCard } from '@/components/WorkerCard'
import { TaskTable } from '@/components/TaskTable'
import { stall } from '@/lib/queue'
import { slotBusy, type Snapshot } from '@/lib/api'
import { needsAnswer, readyForReview } from '@/lib/taskPresentation'
import type { TaskPreviews } from '@/hooks/useTaskPreviews'

export function Overview({
  snapshot,
  now = Date.now(),
  onShowAttention,
  onSelectTask,
  onSetup,
  onShowRun,
  previews,
}: {
  snapshot: Snapshot
  now?: number
  onShowAttention?: () => void
  onSelectTask?: (id: string) => void
  onSetup?: (worker?: string, agent?: string) => void
  onShowRun?: (id: string) => void
  previews?: TaskPreviews
}) {
  const stalled = stall(snapshot, now)
  const known = snapshot.workers.filter(
    (worker) =>
      worker.freshness === 'current' &&
      worker.health !== 'unavailable' &&
      worker.slot.state !== 'unknown',
  )
  const busy = known.reduce((total, worker) => total + slotBusy(worker.slot), 0)
  const capacity = known.reduce((total, worker) => total + worker.slot.capacity, 0)
  const other = snapshot.tasks.filter(
    (task) => !needsAnswer(task) && !readyForReview(task) && task.state !== 'closed',
  )
  const active = snapshot.workers.filter((worker) => worker.active_task)
  const select = onSelectTask ?? (() => onShowAttention?.())
  return (
    <div className="mw-page">
      <AttentionCards snapshot={snapshot} onSelect={select} previews={previews} now={now} />
      <section>
        <div className="mw-section-heading">
          <h2 className="mw-section-title">Your Macs</h2>
          <p className="text-[13px] text-muted-foreground">
            {busy} of {capacity} slots busy
            {known.length !== snapshot.workers.length ? ' · some capacity unknown' : ''}
          </p>
        </div>
        {snapshot.workers.length ? (
          <div className="mw-card-grid">
            {snapshot.workers.map((worker) => (
              <WorkerCard
                key={worker.name}
                worker={worker}
                now={now}
                onSetup={onSetup}
                onSelectTask={select}
              />
            ))}
          </div>
        ) : (
          <div className="mw-panel mw-empty">
            <h3 className="mw-section-title">No Macs connected</h3>
            <p className="text-muted-foreground">No workers are configured.</p>
            <p className="text-sm">Add a Mac to your worker configuration to start your pool.</p>
          </div>
        )}
      </section>
      <section className="mt-1 border-t pt-6">
        {other.length ? (
          <TaskTable
            tasks={other}
            onSelect={select}
            onSetup={onSetup}
            onShowRun={onShowRun}
            now={now}
            compact
            header={
              <>
                <h2 className="text-base font-semibold">Other tasks</h2>
                <p className="text-[13px] text-muted-foreground">
                  {other.filter((task) => task.state === 'active').length} running ·{' '}
                  {other.filter((task) => task.state === 'queued').length} queued
                </p>
              </>
            }
          />
        ) : (
          <div className="mw-panel p-6 text-sm text-muted-foreground">
            <h2 className="mw-section-title mb-3">Other tasks</h2>
            No other tasks are waiting or running.
          </div>
        )}
      </section>
      {stalled ? (
        <StalledBanner
          stall={stalled}
          unreachable={snapshot.workers
            .filter((worker) => worker.health === 'unavailable')
            .map((worker) => worker.name)}
          now={now}
        />
      ) : null}
      <Collapsible className="mw-panel p-5">
        <CollapsibleTrigger className="mw-link w-full font-medium">
          <ChevronRight className="mw-disclosure-chevron" size={14} />
          Execution details
        </CollapsibleTrigger>
        <CollapsibleContent>
          <div className="mt-5 space-y-5">
            {active.length ? (
              active.map((worker) => {
                const row = snapshot.tasks.find(
                  (task) => task.task_id === worker.active_task?.task_id,
                )
                return (
                  <ActiveTurn
                    key={worker.name}
                    worker={worker}
                    row={row}
                    run={snapshot.runs.find((run) => run.run_id === row?.run_id)}
                  />
                )
              })
            ) : (
              <p className="text-sm text-muted-foreground">No turn is running.</p>
            )}
            <QueueTable snapshot={snapshot} now={now} />
            <Capabilities snapshot={snapshot} now={now} />
          </div>
        </CollapsibleContent>
      </Collapsible>
    </div>
  )
}
