import { useState } from 'react'

import { Metric } from '@/components/Metric'
import { ScrollArea } from '@/components/ui/scroll-area'
import { useTurnLog, } from '@/hooks/useTurnLog'
import { wantsExample } from '@/hooks/useSnapshot'
import { EXAMPLE_LOG } from '@/lib/exampleSnapshot'
import { humanize, shortId } from '@/lib/format'
import { parseLogEvents } from '@/lib/logEvents'
import { cn } from '@/lib/utils'
import type { LogStream, RunRow, TaskRow, Worker } from '@/lib/api'

const STREAMS: [LogStream, string][] = [
  ['stdout', 'Standard output'],
  ['stderr', 'Standard error'],
]

type Panel = LogStream | 'details'

export function ActiveTurn({
  worker,
  row,
  run,
}: {
  worker: Worker
  row?: TaskRow
  run?: RunRow
}) {
  const [panel, setPanel] = useState<Panel>('stdout')
  const stream: LogStream = panel === 'details' ? 'stdout' : panel
  const task = worker.active_task
  const turnId = worker.slot.active_job_id
  const example = wantsExample()
  const log = useTurnLog(example ? null : (task?.task_id ?? null), turnId, stream, !example)
  const events = example ? EXAMPLE_LOG : parseLogEvents(log.text)

  if (!task) return null

  return (
    <section className="flex flex-col rounded-[10px] border bg-card">
      <div className="flex flex-wrap items-start gap-6 px-5 pt-5 pb-4">
        <div className="min-w-0 flex-1">
          <span className="flex items-center gap-[7px]">
            <span
              className="size-[5px] shrink-0 rounded-full bg-observatory-accent-soft"
              aria-hidden="true"
            />
            <span className="font-mono text-[10px] tracking-[0.06em] text-primary uppercase">
              Active turn · {worker.name} · turn {task.turn_number}
            </span>
          </span>
          <h2 className="mt-2 truncate text-[22px] leading-7 font-medium tracking-[-0.025em]">
            {task.title}
          </h2>
        </div>
        <div className="flex shrink-0 gap-8">
          <Metric label="AGENT" value={humanize(task.agent)} />
          <Metric label="MODEL" value={task.model ?? 'Agent default'} />
          <Metric label="EFFORT" value={task.effort ? humanize(task.effort) : 'Not reported'} />
        </div>
      </div>

      <div className="flex items-center gap-6 border-y px-5">
        {[...STREAMS, ['details', 'Task details'] as const].map(([value, label]) => (
          <button
            key={value}
            type="button"
            onClick={() => setPanel(value as Panel)}
            className={cn(
              'border-b-2 py-3 text-sm',
              panel === value
                ? 'border-primary text-foreground'
                : 'border-transparent text-muted-foreground',
            )}
          >
            {label}
          </button>
        ))}
        <span className="ml-auto flex items-center gap-[7px]">
          <span className="size-[5px] rounded-full bg-observatory-green" aria-hidden="true" />
          <span className="font-mono text-[10px] tracking-[0.06em] text-muted-foreground uppercase">
            Runner alive · live
          </span>
        </span>
      </div>

      <ScrollArea className="h-64">
        <div className="px-5 py-4 font-mono text-xs leading-6">
          {panel === 'details' ? (
            <dl className="grid grid-cols-2 gap-x-8 gap-y-3 font-sans">
              {[
                ['Task', row?.task_id ?? task.task_id],
                ['Branch', row?.branch ?? '—'],
                ['State', row?.state ?? 'active'],
                ['Permissions', row?.permissions ?? '—'],
                ['Environment profile', row?.env_profile ?? '—'],
                ['Run', run?.name ?? '—'],
              ].map(([label, value]) => (
                <div key={label} className="min-w-0">
                  <dt className="text-[11px] text-muted-foreground">{label}</dt>
                  <dd className="truncate text-sm">{value}</dd>
                </div>
              ))}
            </dl>
          ) : log.error ? (
            <p className="text-destructive">Cannot read the log: {log.error}</p>
          ) : events.length === 0 ? (
            <p className="text-muted-foreground">Waiting for output…</p>
          ) : (
            events.map((event, index) => (
              <div key={index} className="flex gap-6">
                <span className="w-[68px] shrink-0 text-muted-foreground">{event.time ?? ''}</span>
                <span className="w-[76px] shrink-0 text-muted-foreground">{event.kind ?? ''}</span>
                <span className="min-w-0 flex-1 break-all whitespace-pre-wrap">
                  {event.message}
                </span>
              </div>
            ))
          )}
        </div>
      </ScrollArea>

      <div className="flex items-center gap-3 border-t px-5 py-4">
        <svg width="14" height="14" viewBox="0 0 24 24" aria-hidden="true" className="shrink-0">
          <path
            d="M6 4v10 M6 20a2 2 0 1 0 0-4 2 2 0 0 0 0 4Z M18 8a2 2 0 1 0 0-4 2 2 0 0 0 0 4Z M18 8v2a4 4 0 0 1-4 4H6"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.6"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
        <span className="flex-1 font-mono text-xs text-muted-foreground">
          {row?.branch ?? `task/${shortId(task.task_id)}`}
        </span>
        {run ? <span className="text-xs text-muted-foreground">{run.name}</span> : null}
      </div>
    </section>
  )
}
