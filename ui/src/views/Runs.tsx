import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { relativeTime, shortId } from '@/lib/format'
import type { Progress, RunRow, Snapshot } from '@/lib/api'

const SEGMENTS: [keyof Progress, string, string][] = [
  ['closed', 'Closed', 'bg-emerald-500'],
  ['active', 'Active', 'bg-sky-500'],
  ['open', 'Open', 'bg-amber-500'],
  ['queued', 'Queued', 'bg-muted-foreground/40'],
  ['failed_like', 'Failed', 'bg-red-500'],
]

function ProgressBar({ progress }: { progress: Progress }) {
  const total = Math.max(progress.total, 1)
  return (
    <div className="space-y-2">
      <div className="flex h-2 overflow-hidden rounded-full bg-muted">
        {SEGMENTS.map(([key, , tone]) =>
          progress[key] > 0 ? (
            <div key={key} className={tone} style={{ width: `${(progress[key] / total) * 100}%` }} />
          ) : null,
        )}
      </div>
      <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
        {SEGMENTS.map(([key, label]) => (
          <span key={key}>
            {label} <span className="tabular-nums text-foreground">{progress[key]}</span>
          </span>
        ))}
      </div>
    </div>
  )
}

function RunCard({ run }: { run: RunRow }) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <CardTitle className="truncate text-base">{run.name ?? 'Unnamed run'}</CardTitle>
            <p className="font-mono text-xs text-muted-foreground">{shortId(run.run_id, 12)}</p>
          </div>
          <div className="shrink-0 text-right text-xs text-muted-foreground">
            <div>max parallel {run.max_parallel}</div>
            <div>{relativeTime(run.created_at_millis)}</div>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <ProgressBar progress={run.progress} />
      </CardContent>
    </Card>
  )
}

export function Runs({ snapshot }: { snapshot: Snapshot }) {
  if (snapshot.runs.length === 0) {
    return <p className="text-sm text-muted-foreground">No runs have been recorded yet.</p>
  }
  return (
    <div className="grid gap-4 md:grid-cols-2">
      {snapshot.runs.map((run) => (
        <RunCard key={run.run_id} run={run} />
      ))}
    </div>
  )
}
