import { useEffect, useState } from 'react'
import { Tabs } from '@base-ui/react/tabs'
import { LayoutGrid, Layers, ListFilter, SlidersHorizontal } from 'lucide-react'
import { Wordmark } from '@/components/Wordmark'
import { Skeleton } from '@/components/Skeleton'
import { Notifications } from '@/components/Notifications'
import { Overview } from '@/views/Overview'
import { Runs } from '@/views/Runs'
import { Settings } from '@/views/Settings'
import { TaskDetail } from '@/views/TaskDetail'
import { Tasks } from '@/views/Tasks'
import { useSnapshot } from '@/hooks/useSnapshot'
import { useTaskPreviews } from '@/hooks/useTaskPreviews'
import { useInputModality } from '@/hooks/useInputModality'
import { attentionCount } from '@/lib/attention'
import { needsAnswer, readyForReview } from '@/lib/taskPresentation'
import { relativeTime } from '@/lib/format'

export const documentTitle = (attention: number) =>
  (attention > 0 ? '(' + attention + ') ' : '') + 'mac-worker — pool'
const TASK_ID = /^[0-9a-f]{32}$/
export type TaskHash = { status: 'none' } | { status: 'task'; id: string } | { status: 'invalid' }
export function parseTaskHash(hash: string): TaskHash {
  const match = hash.split('?')[0].match(/^#\/tasks\/([^/]*)$/)
  if (!match) return { status: 'none' }
  let decoded: string
  try {
    decoded = decodeURIComponent(match[1])
  } catch {
    return { status: 'invalid' }
  }
  return TASK_ID.test(decoded) ? { status: 'task', id: decoded } : { status: 'invalid' }
}
type View = 'overview' | 'tasks' | 'runs' | 'settings'
type Route = {
  view: View
  taskId?: string
  invalid?: boolean
  run?: string
  worker?: string
  agent?: string
}
function currentRoute(): Route {
  const [path, search] = window.location.hash.replace(/^#\//, '').split('?')
  const query = new URLSearchParams(search)
  const context = {
    run: query.get('run') ?? undefined,
    worker: query.get('worker') ?? undefined,
    agent: query.get('agent') ?? undefined,
  }
  const task = parseTaskHash(window.location.hash)
  if (task.status === 'task') return { view: 'tasks', taskId: task.id, ...context }
  if (task.status === 'invalid') return { view: 'tasks', invalid: true, ...context }
  return {
    view: ['tasks', 'runs', 'settings'].includes(path) ? (path as View) : 'overview',
    ...context,
  }
}
const VIEWS = [
  ['overview', 'The shelf', LayoutGrid],
  ['tasks', 'Tasks', ListFilter],
  ['runs', 'Runs', Layers],
  ['settings', 'Settings', SlidersHorizontal],
] as const

export default function App() {
  useInputModality()
  const { snapshot, error, offline, example } = useSnapshot()
  const [route, setRoute] = useState(currentRoute)
  const attention = snapshot ? attentionCount(snapshot) : 0
  const previews = useTaskPreviews(
    snapshot?.tasks.filter((task) => needsAnswer(task) || readyForReview(task)) ?? [],
  )
  useEffect(() => {
    document.title = documentTitle(attention)
  }, [attention])
  useEffect(() => {
    const apply = () => setRoute(currentRoute())
    window.addEventListener('hashchange', apply)
    return () => window.removeEventListener('hashchange', apply)
  }, [])
  const navigate = (next: Route) => {
    const query = new URLSearchParams()
    for (const key of ['run', 'worker', 'agent'] as const) if (next[key]) query.set(key, next[key]!)
    const path = next.taskId
      ? 'tasks/' + next.taskId
      : next.view === 'overview'
        ? 'shelf'
        : next.view
    const hash = '#/' + path + (query.size ? '?' + query.toString() : '')
    setRoute(next)
    if (window.location.hash !== hash) window.location.hash = hash
  }
  const selectTask = (id: string) =>
    navigate({ view: 'tasks', taskId: id, run: route.view === 'tasks' ? route.run : undefined })
  const showRun = (id: string) => navigate({ view: 'tasks', run: id })
  const setup = (worker?: string, agent?: string) => navigate({ view: 'settings', worker, agent })
  const collectionStale = snapshot?.collection.freshness === 'stale'
  const displayed =
    snapshot && (offline || collectionStale)
      ? {
          ...snapshot,
          workers: snapshot.workers.map((worker) => ({ ...worker, freshness: 'stale' as const })),
        }
      : snapshot

  return (
    <Tabs.Root
      value={route.view}
      onValueChange={(value) => navigate({ view: value as View })}
      className="flex min-h-screen flex-col"
    >
      <header className="mw-header">
        <button
          type="button"
          onClick={() => navigate({ view: 'overview' })}
          aria-label="mac-worker home"
          className="rounded"
        >
          <Wordmark />
        </button>
        <Tabs.List className="mw-navigation" aria-label="Main navigation">
          {VIEWS.map(([value, label, Icon]) => (
            <Tabs.Tab
              key={value}
              value={value}
              onClick={() => {
                if (route.taskId && value === 'tasks') navigate({ view: 'tasks' })
              }}
              className="mw-nav-link"
            >
              <Icon size={16} strokeWidth={1.4} aria-hidden="true" />
              {label}
            </Tabs.Tab>
          ))}
        </Tabs.List>
        <p className="mw-header-status">
          {offline
            ? 'Showing last snapshot'
            : collectionStale
              ? 'Snapshot is stale'
              : snapshot
                ? 'Dashboard refreshed ' + relativeTime(snapshot.generated_at_millis)
                : 'Connecting to your Macs…'}
        </p>
        <Notifications snapshot={snapshot} previews={previews} onSelectTask={selectTask} />
      </header>
      <main className="mw-main flex-1">
        {displayed ? (
          <>
            {offline ? (
              <div className="mw-banner mb-5" data-tone="error">
                <span>The dashboard API stopped answering</span>
                <span className="ml-auto text-xs">
                  Showing the last snapshot · retrying every 2s
                </span>
              </div>
            ) : null}
            {snapshot?.laptop?.binary_outdated ? (
              <div className="mw-banner mb-5">worker was updated, restart the dashboard</div>
            ) : null}
            <Tabs.Panel value="overview">
              <Overview
                snapshot={displayed}
                onSelectTask={selectTask}
                onShowAttention={() => navigate({ view: 'tasks' })}
                onSetup={setup}
                onShowRun={showRun}
                previews={previews}
              />
            </Tabs.Panel>
            <Tabs.Panel value="tasks">
              {route.invalid ? (
                <p className="mb-5 text-sm text-destructive">That task link is not valid.</p>
              ) : null}
              {route.taskId ? (
                <TaskDetail
                  runName={
                    displayed.runs.find(
                      (run) =>
                        run.run_id ===
                        displayed.tasks.find((task) => task.task_id === route.taskId)?.run_id,
                    )?.name ?? undefined
                  }
                  taskId={route.taskId}
                  onBack={() => navigate({ view: 'tasks', run: route.run })}
                />
              ) : (
                <Tasks
                  snapshot={displayed}
                  onSelect={selectTask}
                  initialRun={route.run}
                  onRunChange={(run) => navigate({ view: 'tasks', run })}
                  onSetup={setup}
                />
              )}
            </Tabs.Panel>
            <Tabs.Panel value="runs">
              <Runs snapshot={displayed} onShowRun={showRun} />
            </Tabs.Panel>
            <Tabs.Panel value="settings">
              <Settings
                snapshot={displayed}
                initialWorker={route.worker}
                initialAgent={route.agent}
              />
            </Tabs.Panel>
          </>
        ) : error ? (
          <div className="mw-panel mw-empty">
            <h1 className="mw-section-title">Cannot reach the dashboard</h1>
            <p className="text-sm text-destructive">Cannot reach the dashboard API: {error}</p>
            <p className="text-sm text-muted-foreground">Retrying automatically.</p>
          </div>
        ) : (
          <Skeleton />
        )}
      </main>
      <footer className="flex justify-between gap-4 border-t px-10 py-3 text-xs text-muted-foreground">
        <span>
          {example
            ? 'Example snapshot · '
            : !snapshot
              ? 'Waiting for the first snapshot · '
              : offline
                ? 'Last known snapshot · '
                : ''}
          Local observation
        </span>
        <span>mac-worker</span>
      </footer>
    </Tabs.Root>
  )
}
