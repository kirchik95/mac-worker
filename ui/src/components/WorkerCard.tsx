import { HerdrChip } from '@/components/HerdrChip'
import { Metric } from '@/components/Metric'
import { WorkerIcon } from '@/components/WorkerIcon'
import { bytes, humanize, relativeTime, shortId } from '@/lib/format'
import { cn } from '@/lib/utils'
import { activeJobIds, describeError, slotBusy, type Worker } from '@/lib/api'

type Presence = 'available' | 'running' | 'stale' | 'offline'

function presenceOf(worker: Worker): Presence {
  if (worker.health === 'unavailable') return 'offline'
  if (worker.freshness !== 'current') return 'stale'
  return slotBusy(worker.slot) > 0 ? 'running' : 'available'
}

const DOT: Record<Presence, string> = {
  available: 'bg-observatory-green',
  running: 'bg-observatory-accent-soft',
  // A stale reading is drawn hollow: the pool is not claiming to know.
  stale: 'border border-observatory-hollow',
  offline: 'bg-destructive',
}

/** Elapsed time of the running turn, in the artboard's mm:ss. */
function elapsed(sinceMillis: number | null | undefined, now: number): string {
  if (sinceMillis == null) return '--:--'
  const seconds = Math.max(0, Math.round((now - sinceMillis) / 1000))
  return `${String(Math.floor(seconds / 60)).padStart(2, '0')}:${String(seconds % 60).padStart(2, '0')}`
}

function ErrorCodeChip({ code }: { code: string }) {
  return (
    <span className="shrink-0 rounded-full border px-2 py-0.5 font-mono text-[10px] tracking-[0.06em] text-destructive">
      {code}
    </span>
  )
}

export function WorkerCard({ worker, now }: { worker: Worker; now: number }) {
  const presence = presenceOf(worker)
  const task = worker.active_task
  const running = presence === 'running'
  const busy = slotBusy(worker.slot)
  const jobIds = activeJobIds(worker.slot)
  const described = describeError(worker.error)
  const extraJobs =
    jobIds.length > 1 ? (
      <p className="mt-1 truncate font-mono text-xs text-muted-foreground">
        {jobIds.map((id) => shortId(id, 8)).join(' · ')}
      </p>
    ) : null

  return (
    <article
      className={cn(
        'flex flex-col rounded-[10px] border p-5',
        running
          ? 'border-observatory-highlight-line bg-observatory-highlight'
          : 'border-border bg-card',
      )}
    >
      <div className="flex items-center gap-3">
        <span className="text-muted-foreground">
          <WorkerIcon />
        </span>
        <h2 className="min-w-0 flex-1 truncate text-[22px] leading-7 font-medium tracking-[-0.025em]">
          {worker.name}
        </h2>
        <HerdrChip herdr={worker.herdr} />
        <span className="flex items-center gap-[7px]">
          <span className={cn('size-[5px] shrink-0 rounded-full', DOT[presence])} aria-hidden="true" />
          <span className="font-mono text-[10px] tracking-[0.06em] text-muted-foreground uppercase">
            {presence}
          </span>
        </span>
      </div>

      <div className="mt-5 grid grid-cols-3 gap-6">
        <Metric
          label={presence === 'stale' ? 'LAST CPU' : 'CPU'}
          value={
            worker.system.cpu_busy_percent == null
              ? '—'
              : `${worker.system.cpu_busy_percent.toFixed(1)}%`
          }
          size="display"
        />
        <Metric
          label="MEMORY"
          value={humanize(worker.system.memory_pressure)}
          tone={
            worker.system.memory_pressure != null && worker.system.memory_pressure !== 'normal'
              ? 'accent'
              : undefined
          }
        />
        <Metric label="DISK FREE" value={bytes(worker.system.free_disk_bytes)} />
      </div>

      <div className="mt-5 border-t pt-4">
        {running && task ? (
          <>
            <div className="flex items-baseline gap-3">
              <p className="min-w-0 flex-1 truncate text-sm text-primary">{task.title}</p>
              <span className="shrink-0 font-mono text-xs text-muted-foreground">
                {elapsed(worker.observed_at_millis, now)} · {busy} / {worker.slot.capacity} slots
              </span>
            </div>
            {extraJobs}
            <div className="mt-4 grid grid-cols-3 gap-6">
              <Metric label="AGENT" value={humanize(task.agent)} />
              <Metric label="MODEL" value={task.model ?? 'Agent default'} />
              <Metric label="EFFORT" value={task.effort ? humanize(task.effort) : 'Not reported'} />
            </div>
          </>
        ) : (
          <>
            {presence === 'offline' ? (
              <p
                className="flex min-w-0 items-center gap-2 text-sm"
                title={described?.code ?? undefined}
              >
                {described?.code && described.code !== described.message ? (
                  <ErrorCodeChip code={described.code} />
                ) : null}
                <span className="min-w-0 truncate">
                  {described?.message ?? 'The worker did not answer'}
                </span>
              </p>
            ) : (
              <p className="truncate text-sm">
                {presence === 'stale' ? 'Observation is out of date' : 'Ready for the next task'}
              </p>
            )}
            {extraJobs}
            <div className="mt-2.5 flex items-baseline gap-3">
              <span className="min-w-0 flex-1 truncate text-xs text-muted-foreground">
                {presence === 'offline' || presence === 'stale'
                  ? `Last seen ${relativeTime(worker.observed_at_millis, now)}`
                  : `${busy} / ${worker.slot.capacity} slots occupied`}
              </span>
              <span className="shrink-0 text-xs text-muted-foreground">
                {presence === 'offline' || presence === 'stale' ? 'Cached metrics' : 'Current'}
              </span>
            </div>
          </>
        )}
      </div>
    </article>
  )
}
