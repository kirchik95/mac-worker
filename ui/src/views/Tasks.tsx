import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { useEffect, useMemo, useState } from 'react'
import {
  ArrowRight,
  ChevronDown,
  Search,
  Terminal,
  MessageSquareText,
  ListFilter,
  X,
} from 'lucide-react'
import { CommandList } from '@/components/CommandList'
import { TaskTable } from '@/components/TaskTable'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
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
  const [questionsOnly, setQuestionsOnly] = useState(false)
  useEffect(() => setRun(initialRun ?? ANY), [initialRun])
  const outcomes = [
    ...new Set(
      snapshot.tasks.flatMap((task) => (task.last_outcome ? [task.last_outcome.kind] : [])),
    ),
  ]
  const rows = useMemo(
    () =>
      snapshot.tasks.filter((task) => {
        if (questionsOnly && !needsAnswer(task)) return false
        if (state !== ANY && task.state !== state) return false
        if (outcome !== ANY && task.last_outcome?.kind !== outcome) return false
        if (run !== ANY && task.run_id !== run) return false
        return (
          !query.trim() ||
          (task.title + ' ' + task.task_id).toLowerCase().includes(query.trim().toLowerCase())
        )
      }),
    [snapshot.tasks, state, outcome, run, query, questionsOnly],
  )
  const questions = snapshot.tasks.filter(needsAnswer)
  const filtered =
    questionsOnly || state !== ANY || outcome !== ANY || run !== ANY || !!query.trim()
  const clear = () => {
    setState(ANY)
    setOutcome(ANY)
    setRun(ANY)
    setQuery('')
    setQuestionsOnly(false)
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
    <div className="mw-page mw-tasks-page">
      <div className="mw-page-heading">
        <div>
          <h1 className="mw-page-title">Tasks</h1>
          <p className="mw-page-description">
            Follow every task, answer questions and review finished work.
          </p>
        </div>
      </div>
      <Collapsible>
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
                aria-pressed={!questionsOnly && state === key}
                onClick={() => {
                  setState(key)
                  setQuestionsOnly(false)
                }}
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
              placeholder="Search by title or task ID"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              className="pl-9"
            />
          </div>
          <Select
            value={outcome}
            onValueChange={(value) => {
              setOutcome(value ?? ANY)
              setQuestionsOnly(false)
            }}
          >
            <SelectTrigger aria-label="Outcome" className="w-[150px] shrink-0">
              <SelectValue className="min-w-0">
                {(value) => (
                  <span className="truncate">
                    {value === ANY ? 'Outcome: Any' : humanize(String(value))}
                  </span>
                )}
              </SelectValue>
            </SelectTrigger>
            <SelectContent alignItemWithTrigger={false}>
              <SelectItem value={ANY}>Any outcome</SelectItem>
              {outcomes.map((kind) => (
                <SelectItem key={kind} value={kind}>
                  {humanize(kind)}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select
            value={run}
            onValueChange={(value) => {
              setRun(value ?? ANY)
              setQuestionsOnly(false)
              onRunChange?.(!value || value === ANY ? undefined : value)
            }}
          >
            <SelectTrigger aria-label="Run" className="w-[148px] shrink-0">
              <SelectValue className="min-w-0 overflow-hidden">
                <span className="truncate">
                  Run:{' '}
                  {run === ANY
                    ? 'All runs'
                    : (snapshot.runs.find((entry) => entry.run_id === run)?.name ?? shortId(run))}
                </span>
              </SelectValue>
            </SelectTrigger>
            <SelectContent alignItemWithTrigger={false}>
              <SelectItem value={ANY}>All runs</SelectItem>
              {run !== ANY && !snapshot.runs.some((entry) => entry.run_id === run) ? (
                <SelectItem value={run}>{shortId(run)}</SelectItem>
              ) : null}
              {snapshot.runs.map((entry) => (
                <SelectItem value={entry.run_id} key={entry.run_id}>
                  {entry.name ?? shortId(entry.run_id)}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <CollapsibleTrigger className="mw-link shrink-0">
            <Terminal size={14} aria-hidden="true" />
            CLI equivalent
            <ChevronDown size={12} aria-hidden="true" />
          </CollapsibleTrigger>
        </div>
        <CollapsibleContent keepMounted>
          <div className="mt-3">
            <CommandList commands={[command]} />
            {query || questionsOnly ? (
              <p className="mw-help mt-2">
                This command includes state, outcome and run filters. Text search and the questions
                shortcut apply in the dashboard.
              </p>
            ) : null}
          </div>
        </CollapsibleContent>
      </Collapsible>
      {questions.length ? (
        <div className="mw-banner">
          <MessageSquareText size={16} aria-hidden="true" />
          <span>
            {questions.length} {questions.length === 1 ? 'task needs' : 'tasks need'} your answer
          </span>
          <button
            type="button"
            className="mw-link ml-auto !text-warning"
            aria-pressed={questionsOnly}
            onClick={() => {
              clear()
              setQuestionsOnly(!questionsOnly)
            }}
          >
            {questionsOnly ? 'Show all tasks' : 'Show questions'}
            <ArrowRight size={15} aria-hidden="true" />
          </button>
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
      ) : (
        <TaskTable
          tasks={rows}
          onSelect={onSelect}
          onSetup={onSetup}
          now={now}
          empty={
            <div className="mw-empty">
              <Search size={28} aria-hidden="true" />
              <h2 className="mw-section-title">
                {query.trim()
                  ? `No tasks match “${query.trim()}”`
                  : 'No task matches these filters.'}
              </h2>
              <p>Try a different title or task ID, or clear your filters.</p>
              <Button variant="outline" onClick={clear}>
                <X size={14} aria-hidden="true" />
                Clear filters
              </Button>
            </div>
          }
        />
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
      </div>
    </div>
  )
}
