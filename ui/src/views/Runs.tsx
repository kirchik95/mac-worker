import { ArrowRight, Layers, Info } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { relativeTime, shortId } from '@/lib/format'
import type { Snapshot, Progress } from '@/lib/api'

const SEGMENTS: [keyof Progress, string, string][] = [
  ['closed', 'Closed', '#12B76A'],
  ['active', 'Running', '#475467'],
  ['open', 'Open', '#F79009'],
  ['queued', 'Queued', '#D0D5DD'],
  ['failed_like', 'Failed', '#F04438'],
]
export function Runs({
  snapshot,
  onShowRun,
}: {
  snapshot: Snapshot
  onShowRun?: (id: string) => void
}) {
  return (
    <div className="mw-page">
      <header>
        <h1 className="mw-page-title">Runs</h1>
        <p className="mw-page-description">
          Related tasks, their progress and how many can run at once.
        </p>
      </header>
      {snapshot.runs.length ? (
        <div className="mw-table-wrap">
          <table className="mw-table">
            <colgroup>
              <col style={{ width: '27%' }} />
              <col />
              <col style={{ width: 140 }} />
              <col style={{ width: 140 }} />
              <col style={{ width: 158 }} />
            </colgroup>
            <thead>
              <tr>
                <th>Run</th>
                <th>Progress</th>
                <th>Max parallel</th>
                <th>Created</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {snapshot.runs.map((run) => (
                <tr key={run.run_id}>
                  <td>
                    <button
                      className="mw-task-title font-medium"
                      onClick={() => onShowRun?.(run.run_id)}
                    >
                      {run.name ?? 'Unnamed run'}
                    </button>
                    <span className="mw-task-id">
                      {shortId(run.run_id, 12)} · {run.progress.total}{' '}
                      {run.progress.total === 1 ? 'task' : 'tasks'}
                    </span>
                  </td>
                  <td>
                    <div
                      className="mb-2 flex h-1.5 overflow-hidden rounded-full bg-muted"
                      role="img"
                      aria-label={SEGMENTS.map(
                        ([key, label]) => label + ': ' + run.progress[key],
                      ).join(', ')}
                    >
                      {SEGMENTS.map(([key, , color]) => (
                        <span
                          key={key}
                          style={{
                            width:
                              (100 * run.progress[key]) / Math.max(run.progress.total, 1) + '%',
                            background: color,
                          }}
                        />
                      ))}
                    </div>
                    <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs">
                      {SEGMENTS.filter(([key]) => run.progress[key] > 0).map(([key, label]) => (
                        <span key={key}>
                          {run.progress[key]} {label.toLowerCase()}
                        </span>
                      ))}
                    </div>
                  </td>
                  <td>
                    <span className="mw-badge">{run.max_parallel}</span>
                  </td>
                  <td>{relativeTime(run.created_at_millis)}</td>
                  <td>
                    <Button variant="outline" onClick={() => onShowRun?.(run.run_id)}>
                      View tasks
                      <ArrowRight size={16} aria-hidden="true" />
                    </Button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <section className="mw-panel mw-empty">
          <Layers size={28} aria-hidden="true" />
          <h2 className="mw-section-title">No runs yet</h2>
          <p className="text-muted-foreground">
            Runs group related tasks under one concurrency limit.
          </p>
        </section>
      )}
      <p className="flex items-start gap-2 text-xs text-muted-foreground">
        <Info size={15} className="shrink-0 mt-0.5" aria-hidden="true" />A run’s limit is separate
        from Mac slots. Queued tasks start when both a run slot and a compatible Mac slot are
        available.
      </p>
    </div>
  )
}
