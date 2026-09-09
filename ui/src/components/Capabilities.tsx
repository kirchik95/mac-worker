import { HerdrChip } from '@/components/HerdrChip'
import { Icon, type IconName } from '@/components/Icon'
import { duration, shortId } from '@/lib/format'
import { FACTS_TTL_MILLIS, advertisedAgents, factsAge, factsLapsed } from '@/lib/queue'
import { cn } from '@/lib/utils'
import { describeError, type Snapshot, type Worker } from '@/lib/api'

function glyph(worker: Worker): IconName {
  if (worker.health === 'unavailable') return 'cpuOff'
  return worker.slot.state === 'idle' ? 'cpu' : 'cpuBusy'
}

function Facts({ worker, now }: { worker: Worker; now: number }) {
  const age = factsAge(worker, now)
  const lapsed = factsLapsed(worker, now)
  const fill = age == null ? 0 : Math.min(1, age / FACTS_TTL_MILLIS)

  return (
    <div className="flex w-105 shrink-0 items-center gap-3">
      <span
        className={cn(
          'flex w-30 shrink-0 items-center gap-1.5 font-mono text-[11px] whitespace-nowrap text-observatory-hollow',
        )}
      >
        <Icon
          name="clock"
          size={12}
          className={lapsed ? 'text-primary' : age == null ? 'text-border' : 'text-observatory-green'}
        />
        {age == null ? 'NO FACTS' : `FACTS ${duration(age)}`}
      </span>
      <span className="h-1 w-35 shrink-0 overflow-hidden rounded-full border bg-muted">
        <span
          className={cn('block h-full', lapsed ? 'bg-primary' : 'bg-observatory-green')}
          style={{ width: `${Math.round(fill * 100)}%` }}
        />
      </span>
      <span
        className={cn(
          'shrink-0 font-mono text-[11px] tracking-[0.06em] whitespace-nowrap',
          lapsed ? 'text-primary' : age == null ? 'text-observatory-hollow' : 'text-muted-foreground',
        )}
      >
        {age == null ? 'UNREACHABLE' : lapsed ? 'EXPIRED' : 'FRESH'}
      </span>
    </div>
  )
}

function trailing(worker: Worker, now: number): string {
  if (worker.active_task) {
    const since = worker.observed_at_millis == null ? '' : ` · ${duration(now - worker.observed_at_millis)} in`
    return `turn ${worker.active_task.turn_number} of ${shortId(worker.active_task.task_id, 12)}${since}`
  }
  if (worker.health === 'unavailable') {
    return worker.observed_at_millis == null
      ? 'never observed'
      : `last seen ${duration(now - worker.observed_at_millis)} ago`
  }
  return 'slot free'
}

/**
 * What each worker currently advertises. Agent capabilities come from facts
 * that expire, so a machine can be up, idle, and still take no work; this is
 * the panel that says so before the queue stops making sense.
 */
export function Capabilities({ snapshot, now = Date.now() }: { snapshot: Snapshot; now?: number }) {
  const usable = snapshot.workers.filter(
    (worker) => worker.health !== 'unavailable' && !factsLapsed(worker, now),
  ).length

  return (
    <section className="overflow-hidden rounded-[10px] border bg-card">
      <div className="flex flex-wrap items-baseline gap-x-3.5 gap-y-1 border-b px-5.5 py-3.5">
        <span className="flex items-center gap-2 text-muted-foreground">
          <Icon name="broadcast" />
          <span className="font-mono text-[11px] tracking-[0.1em]">WHAT EACH WORKER ADVERTISES</span>
        </span>
        <span className="text-xs text-observatory-hollow">
          agent facts expire {duration(FACTS_TTL_MILLIS)} after they are collected
        </span>
        <span
          className={cn(
            'ml-auto font-mono text-[11px] uppercase',
            usable === 0 ? 'text-primary' : 'text-observatory-hollow',
          )}
        >
          {usable} of {snapshot.workers.length} workers can take work
        </span>
      </div>

      <ul className="divide-y">
        {snapshot.workers.map((worker) => {
          const lapsed = factsLapsed(worker, now)
          const agents = advertisedAgents(worker)
          const described = describeError(worker.error)
          return (
            <li key={worker.name} className="flex flex-wrap items-center gap-y-2 px-5.5 py-4">
              <span className="flex w-37.5 shrink-0 items-center gap-2.5">
                <Icon
                  name={glyph(worker)}
                  size={15}
                  className={
                    worker.health === 'unavailable'
                      ? 'text-destructive'
                      : worker.slot.state === 'idle'
                        ? 'text-observatory-green'
                        : 'text-primary'
                  }
                />
                <span className="text-[15px]">{worker.name}</span>
                <span
                  className={cn(
                    'font-mono text-[11px] uppercase',
                    worker.health === 'unavailable' ? 'text-destructive' : 'text-muted-foreground',
                  )}
                >
                  {worker.health === 'unavailable'
                    ? 'offline'
                    : worker.slot.state === 'idle'
                      ? 'ready'
                      : 'running'}
                </span>
              </span>

              <Facts worker={worker} now={now} />

              <span className="flex min-w-0 flex-1 flex-wrap items-center gap-2.5">
                <HerdrChip herdr={worker.herdr} className="px-2.5 py-1 text-[11px]" />
                {described ? (
                  <span
                    className="flex min-w-0 items-center gap-2"
                    title={described.code ?? undefined}
                  >
                    {described.code && described.code !== described.message ? (
                      <span className="rounded-full border px-2.5 py-1 font-mono text-[11px] text-destructive">
                        {described.code}
                      </span>
                    ) : null}
                    <span className="min-w-0 truncate font-mono text-[11px] text-destructive">
                      {described.message}
                    </span>
                  </span>
                ) : agents.length === 0 ? (
                  <span className="font-mono text-[11px] text-observatory-hollow">
                    no agent reported
                  </span>
                ) : (
                  agents.map((agent) => (
                    <span
                      key={agent}
                      className={cn(
                        'rounded-full border px-2.5 py-1 font-mono text-[11px]',
                        lapsed ? 'text-observatory-hollow' : 'text-muted-foreground',
                      )}
                    >
                      {agent}
                      {lapsed ? ' · lapsed' : ''}
                    </span>
                  ))
                )}
              </span>

              <span className="shrink-0 text-right text-xs text-observatory-hollow">
                {trailing(worker, now)}
              </span>
            </li>
          )
        })}
      </ul>
    </section>
  )
}
