import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { useEffect, useMemo, useState } from 'react'
import { Search, Terminal, MessageSquareText, ListFilter } from 'lucide-react'
import { CommandList } from '@/components/CommandList'
import { TaskTable } from '@/components/TaskTable'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { humanize, shortId } from '@/lib/format'
import { needsAnswer } from '@/lib/taskPresentation'
import type { Snapshot } from '@/lib/api'

const ANY = 'any'
const STATES = [
  ['any', 'All'],
  ['active', 'Running'],
  ['queued', 'Queued'],
  ['open', 'Open'],
  ['closed', 'Closed'],
  ['abandoned', 'Abandoned'],
  ['lost', 'Lost'],
] as const

/** Local filters never issue a request. */
export function Tasks({
  snapshot,
  now = Date.now(),
  onSelect,
  initialRun,
  onSetup,
  onRunChange,
}: {
  snapshot: Snapshot
  now?: number
  onSelect: (id: string) => void
  onRunChange?: (run?: string) => void
  initialRun?: string
  onSetup?: (worker?: string, agent?: string) => void
}) {
  const [state, setState] = useState(ANY)
  const [outcome, setOutcome] = useState(ANY)
  const [run, setRun] = useState(initialRun ?? ANY)
  const [query, setQuery] = useState('')
  useEffect(() => setRun(initialRun ?? ANY), [initialRun])
  const outcomes = [
    ...new Set(
      snapshot.tasks.flatMap((task) => (task.last_outcome ? [task.last_outcome.kind] : [])),
    ),
  ]
  const rows = useMemo(
    () =>
      snapshot.tasks.filter((task) => {
        if (state !== ANY && task.state !== state) return false
        if (outcome !== ANY && task.last_outcome?.kind !== outcome) return false
        if (run !== ANY && task.run_id !== run) return false
        return (
          !query.trim() ||
          (task.title + ' ' + task.task_id).toLowerCase().includes(query.trim().toLowerCase())
        )
      }),
    [snapshot.tasks, state, outcome, run, query],
  )
  const questions = rows.filter(needsAnswer)
  const filtered = state !== ANY || outcome !== ANY || run !== ANY || !!query.trim()
  const clear = () => {
    setState(ANY)
    setOutcome(ANY)
    setRun(ANY)
    setQuery('')
    onRunChange?.(undefined)
  }
  const command = [
    'worker task list',
    state === ANY ? '' : '--state ' + state,
    outcome === ANY ? '' : '--outcome ' + outcome.replaceAll('_', '-'),
    run === ANY ? '' : '--run ' + run,
  ]
    .filter(Boolean)
    .join(' ')
  return (
    <div className="mw-page">
      <div className="mw-page-heading">
        <div>
          <h1 className="mw-page-title">Tasks</h1>
          <p className="mw-page-description">
            Follow every task, answer questions and review finished work.
          </p>
        </div>
      </div>
      <div className="mw-filters">
        <div className="mw-segmented" role="group" aria-label="State">
          {STATES.filter(
            ([key]) =>
              !['abandoned', 'lost'].includes(key) ||
              snapshot.tasks.some((task) => task.state === key),
          ).map(([key, label]) => (
            <button
              key={key}
              type="button"
              aria-pressed={state === key}
              onClick={() => setState(key)}
            >
              {label}
              <span>
                {key === ANY
                  ? snapshot.tasks.length
                  : snapshot.tasks.filter((task) => task.state === key).length}
              </span>
            </button>
          ))}
        </div>
        <div className="relative min-w-44 flex-1">
          <Search
            size={16}
            aria-hidden="true"
            className="absolute top-3 left-3 text-muted-foreground"
          />
          <Input
            aria-label="Filter tasks"
            placeholder="Search tasks…"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            className="pl-9"
          />
        </div>
        <select
          className="mw-select w-auto"
          aria-label="Outcome"
          value={outcome}
          onChange={(event) => setOutcome(event.target.value)}
        >
          <option value={ANY}>Any outcome</option>
          {outcomes.map((kind) => (
            <option key={kind} value={kind}>
              {humanize(kind)}
            </option>
          ))}
        </select>
        <select
          className="mw-select w-auto max-w-48"
          aria-label="Run"
          value={run}
          onChange={(event) => {
            setRun(event.target.value)
            onRunChange?.(event.target.value === ANY ? undefined : event.target.value)
          }}
        >
          <option value={ANY}>All runs</option>
          {run !== ANY && !snapshot.runs.some((entry) => entry.run_id === run) ? (
            <option value={run}>{shortId(run)}</option>
          ) : null}
          {snapshot.runs.map((entry) => (
            <option value={entry.run_id} key={entry.run_id}>
              {entry.name ?? shortId(entry.run_id)}
            </option>
          ))}
        </select>
      </div>
      {questions.length ? (
        <div className="mw-banner">
          <MessageSquareText size={16} aria-hidden="true" />
          <span>
            {questions.length} {questions.length === 1 ? 'task needs' : 'tasks need'} your answer
          </span>
          <span className="hidden sm:inline">·</span>
          {questions.map((task) => (
            <button
              key={task.task_id}
              type="button"
              className="mw-link"
              onClick={() => onSelect(task.task_id)}
            >
              {task.title}
            </button>
          ))}
        </div>
      ) : null}
      {snapshot.tasks.length === 0 ? (
        <section className="mw-panel mw-empty">
          <ListFilter size={28} strokeWidth={1.3} aria-hidden="true" />
          <h2 className="mw-section-title">Your first task starts in the terminal</h2>
          <p className="text-muted-foreground">
            Send work to an agent. It will appear here when the dashboard refreshes.
          </p>
          <CommandList commands={['worker task submit --help']} />
        </section>
      ) : rows.length ? (
        <TaskTable tasks={rows} onSelect={onSelect} onSetup={onSetup} now={now} />
      ) : (
        <section className="mw-panel mw-empty">
          <Search size={28} aria-hidden="true" />
          <h2 className="mw-section-title">No task matches these filters.</h2>
          <Button variant="outline" onClick={clear}>
            Clear filters
          </Button>
        </section>
      )}
      <div className="flex flex-wrap items-start justify-between gap-4 text-xs text-muted-foreground">
        <div className="flex items-center gap-4">
          <span>
            {rows.length} of {snapshot.tasks.length}
          </span>
          {filtered ? (
            <>
              <span>
                {snapshot.tasks.length - rows.length} of {snapshot.tasks.length} tasks do not match
              </span>
              {rows.length ? (
                <button className="mw-link" onClick={clear}>
                  Clear filters
                </button>
              ) : null}
            </>
          ) : null}
        </div>
        <Collapsible className="max-w-full">
          <CollapsibleTrigger className="mw-link">
            <Terminal size={14} aria-hidden="true" />
            CLI equivalent
          </CollapsibleTrigger>
          <CollapsibleContent keepMounted>
            <div className="mt-3">
              <CommandList commands={[command]} />
            </div>
          </CollapsibleContent>
        </Collapsible>
      </div>
    </div>
  )
}
