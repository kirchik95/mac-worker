import { useEffect, useState } from 'react'

import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Wordmark } from '@/components/Wordmark'
import { Skeleton } from '@/components/Skeleton'
import { Overview } from '@/views/Overview'
import { Runs } from '@/views/Runs'
import { Settings } from '@/views/Settings'
import { TaskDetail } from '@/views/TaskDetail'
import { Tasks } from '@/views/Tasks'
import { useSnapshot } from '@/hooks/useSnapshot'
import { attentionCount } from '@/lib/attention'
import { clockTime, pad2 } from '@/lib/format'
import type { Snapshot } from '@/lib/api'

/** The tab carries the count so a question is noticed in a background tab. */
export const documentTitle = (attention: number) =>
  `${attention > 0 ? `(${attention}) ` : ''}mac-worker — pool`

const VIEWS = [
  ['overview', 'Overview'],
  ['tasks', 'Tasks'],
  ['runs', 'Run history'],
  ['settings', 'Settings'],
] as const

function fleetLine(snapshot: Snapshot): string {
  const current = snapshot.workers.filter((worker) => worker.freshness === 'current').length
  const stale = snapshot.workers.length - current
  const parts = [`${current} current ${current === 1 ? 'worker' : 'workers'}`]
  if (stale > 0) parts.push(`${stale} stale ${stale === 1 ? 'observation' : 'observations'}`)
  return `Your personal pool · ${parts.join(' · ')}`
}

function Stat({ value, label, accent }: { value: string; label: string; accent?: boolean }) {
  return (
    <div className="flex items-center gap-2">
      <span
        className={`font-mono text-[18px] leading-6 tracking-[-0.02em] ${
          accent ? 'text-primary' : 'text-foreground'
        }`}
      >
        {value}
      </span>
      <span className="text-xs text-muted-foreground">{label}</span>
    </div>
  )
}

const TASK_ID = /^[0-9a-f]{32}$/

export type TaskHash =
  | { status: 'none' }
  | { status: 'task'; id: string }
  | { status: 'invalid' }

/** Hash deep links are `#/tasks/<lowercase simple UUID>`. Decode failures stay off the detail pane. */
export function parseTaskHash(hash: string): TaskHash {
  const match = hash.match(/^#\/tasks\/([^/]*)$/)
  if (!match) return { status: 'none' }
  let decoded: string
  try {
    decoded = decodeURIComponent(match[1])
  } catch {
    return { status: 'invalid' }
  }
  if (!TASK_ID.test(decoded)) return { status: 'invalid' }
  return { status: 'task', id: decoded }
}

export default function App() {
  const { snapshot, error, offline, example } = useSnapshot()
  const [selectedTask, setSelectedTask] = useState<string | null>(() => {
    const parsed = parseTaskHash(window.location.hash)
    return parsed.status === 'task' ? parsed.id : null
  })
  const [invalidTaskLink, setInvalidTaskLink] = useState(
    () => parseTaskHash(window.location.hash).status === 'invalid',
  )
  const [view, setView] = useState(() =>
    parseTaskHash(window.location.hash).status === 'none' ? 'overview' : 'tasks',
  )

  const busy = snapshot?.workers.filter((worker) => worker.slot.state !== 'idle').length ?? 0
  const capacity =
    snapshot?.workers.reduce((total, worker) => total + worker.slot.capacity, 0) ?? 0
  const attention = snapshot ? attentionCount(snapshot) : 0
  const collectionStale = snapshot?.collection.freshness === 'stale'

  useEffect(() => {
    document.title = documentTitle(attention)
  }, [attention])

  useEffect(() => {
    const apply = () => {
      const parsed = parseTaskHash(window.location.hash)
      if (parsed.status === 'task') {
        setSelectedTask(parsed.id)
        setInvalidTaskLink(false)
        setView('tasks')
        return
      }
      setSelectedTask(null)
      setInvalidTaskLink(parsed.status === 'invalid')
      if (parsed.status === 'invalid') setView('tasks')
    }
    window.addEventListener('hashchange', apply)
    return () => window.removeEventListener('hashchange', apply)
  }, [])

  const selectTask = (id: string | null) => {
    setInvalidTaskLink(false)
    setSelectedTask(id)
    if (id) {
      const next = `#/tasks/${id}`
      if (window.location.hash !== next) window.location.hash = next
      setView('tasks')
      return
    }
    if (window.location.hash.startsWith('#/tasks/')) {
      window.history.pushState(null, '', `${window.location.pathname}${window.location.search}`)
    }
  }

  return (
    <Tabs value={view} onValueChange={(next) => setView(next ?? 'overview')} className="flex min-h-screen flex-col gap-0 bg-background text-foreground">
      <header className="flex h-19 shrink-0 items-center gap-10 border-b bg-card px-10">
        <Wordmark />
        <TabsList className="h-19 gap-10 rounded-none bg-transparent p-0">
          {VIEWS.map(([value, label]) => (
            <TabsTrigger
              key={value}
              value={value}
              className="h-19 rounded-none border-0 border-b-2 border-transparent px-0 text-sm text-muted-foreground shadow-none data-[state=active]:border-primary data-[state=active]:bg-transparent data-[state=active]:text-foreground data-[state=active]:shadow-none"
            >
              {label}
            </TabsTrigger>
          ))}
        </TabsList>
        <div className="ml-auto flex items-center gap-2">
            <span className={`size-1.5 rounded-full ${offline || collectionStale ? 'bg-destructive' : 'bg-primary'}`}
            aria-hidden="true"
          />
          <span className={`font-mono text-xs ${offline || collectionStale ? 'text-destructive' : 'text-muted-foreground'}`}>
            {offline ? 'LAST SNAPSHOT' : collectionStale ? 'STALE SNAPSHOT' : 'SNAPSHOT'}{' '}
            {snapshot ? clockTime(snapshot.generated_at_millis) : '--:--:--'}
          </span>
        </div>
      </header>

      <main className="px-10 pt-6.5 pb-6">
        {snapshot ? (
          <>
            {offline ? (
              <div className="mb-5 flex flex-wrap items-center gap-3 rounded-[10px] border border-destructive bg-destructive/5 px-5 py-3.5">
                <span className="size-1.5 shrink-0 rounded-full bg-destructive" aria-hidden="true" />
                <span className="text-sm">The dashboard API stopped answering</span>
                <span className="ml-auto text-xs text-muted-foreground">
                  Showing the last snapshot · retrying every 2s
                </span>
              </div>
            ) : null}
            <div className="flex items-center pb-5">
              <p className="flex-1 text-sm text-muted-foreground">{fleetLine(snapshot)}</p>
              <div className="flex items-center gap-7">
                <Stat value={`${pad2(busy)} / ${pad2(capacity)}`} label="Slots occupied" accent />
                <Stat value={pad2(snapshot.progress.total)} label="Total tasks" />
              </div>
            </div>

            <TabsContent value="overview">
              <Overview snapshot={snapshot} onShowAttention={() => setView('tasks')} />
            </TabsContent>

            <TabsContent value="tasks">
              {invalidTaskLink ? (
                <p className="mb-5 text-sm text-destructive">That task link is not valid.</p>
              ) : null}
              {selectedTask ? (
                <TaskDetail taskId={selectedTask} onBack={() => selectTask(null)} />
              ) : (
                <Tasks snapshot={snapshot} onSelect={selectTask} />
              )}
            </TabsContent>

            <TabsContent value="runs">
              <Runs snapshot={snapshot} />
            </TabsContent>

            <TabsContent value="settings">
              <Settings snapshot={snapshot} />
            </TabsContent>
          </>
        ) : error ? (
          <p className="text-sm text-destructive">Cannot reach the dashboard API: {error}</p>
        ) : (
          <Skeleton />
        )}
      </main>

      <footer className="mt-auto border-t px-10 py-4">
        <div className="flex flex-wrap gap-x-8 gap-y-1 font-mono text-[10px] tracking-[0.06em] text-muted-foreground uppercase">
          <span>Observatory · light</span>
          <span className="mx-auto">
            {example
              ? 'Example snapshot · '
              : snapshot == null
                ? 'Waiting for the first snapshot · '
                : offline
                  ? 'Last known snapshot · '
                  : ''}
            Local observation
          </span>
          <span>mac-worker</span>
        </div>
      </footer>
    </Tabs>
  )
}
