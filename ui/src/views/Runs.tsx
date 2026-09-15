import { Clock3, FileText, Layers } from 'lucide-react'
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
        <p className="mw-page-description">Task groups and their execution limits.</p>
      </header>
      {snapshot.runs.length ? (
        <div className="mw-table-wrap mw-runs-table">
          <table className="mw-table">
            <colgroup>
              <col style={{ width: '292px' }} />
              <col />
              <col style={{ width: 168 }} />
              <col style={{ width: 162 }} />
              <col style={{ width: 184 }} />
            </colgroup>
            <thead>
              <tr>
                <th>Run</th>
                <th>Task progress</th>
                <th>Parallel limit</th>
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
                    <span className="mw-task-id">{shortId(run.run_id, 12)}</span>
                  </td>
                  <td>
                    <p className="mb-2 text-sm text-foreground">
                      {run.progress.total} {run.progress.total === 1 ? 'task' : 'tasks'}
                    </p>
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
                    {snapshot.collection.freshness === 'current' &&
                    snapshot.tasks.some(
                      (task) =>
                        task.run_id === run.run_id &&
                        task.state === 'queued' &&
                        task.freshness === 'current' &&
                        task.blocking_code === 'RUN_MAX_PARALLEL',
                    ) ? (
                      <p className="mt-4 flex items-center gap-2 text-xs text-warning">
                        <Clock3 size={13} aria-hidden="true" />
                        Parallel limit reached
                      </p>
                    ) : null}
                  </td>
                  <td>{run.max_parallel} at a time</td>
                  <td>{relativeTime(run.created_at_millis)}</td>
                  <td>
                    <Button variant="outline" onClick={() => onShowRun?.(run.run_id)}>
                      <FileText size={16} aria-hidden="true" />
                      View tasks
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
      {snapshot.runs.length ? (
        <p className="text-[13px] text-muted-foreground">
          {snapshot.runs.length} runs ·{' '}
          {snapshot.runs.reduce((sum, run) => sum + run.progress.total, 0)} tasks
        </p>
      ) : null}
      <div className="flex items-start gap-3 border-t py-6">
        <Layers size={20} className="shrink-0 text-muted-foreground" aria-hidden="true" />
        <div>
          <h2 className="mb-2 font-semibold">Run limits and Mac slots</h2>
          <p className="text-sm text-muted-foreground">
            A run can wait at its parallel limit even when a Mac has a free slot.
          </p>
          <p className="mt-1 text-[13px] text-muted-foreground">
            View its tasks to see which items are waiting for capacity, input or agent setup.
          </p>
        </div>
      </div>
    </div>
  )
}
