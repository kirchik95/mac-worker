import { useMemo, useState } from 'react'

import { CommandList } from '@/components/CommandList'
import { Icon } from '@/components/Icon'
import { Input } from '@/components/ui/input'
import { StatusMark } from '@/components/StatusMark'
import { useAttentionQuestions } from '@/hooks/useAttentionQuestions'
import { duration, relativeTime, shortId } from '@/lib/format'
import { blockingDetail } from '@/lib/queue'
import { cn } from '@/lib/utils'
import { questionOptions, questionText, type Snapshot, type TaskRow } from '@/lib/api'

const ANY = 'any'

/** A turn that ended with a question stops the task until it is answered. */
export const needsInput = (task: TaskRow) => task.last_outcome?.kind === 'needs_input'

const outcomeOf = (task: TaskRow) => task.last_outcome?.kind ?? null

function countBy(tasks: TaskRow[], of: (task: TaskRow) => string | null): Map<string, number> {
  const counts = new Map<string, number>()
  for (const task of tasks) {
    const key = of(task)
    if (key) counts.set(key, (counts.get(key) ?? 0) + 1)
  }
  return counts
}

function Segmented({
  value,
  options,
  onChange,
  label,
}: {
  value: string
  options: string[]
  onChange: (next: string) => void
  label: string
}) {
  return (
    <div className="flex overflow-hidden rounded-md border bg-card" role="group" aria-label={label}>
      {[ANY, ...options].map((option) => (
        <button
          key={option}
          type="button"
          onClick={() => onChange(option)}
          aria-pressed={value === option}
          className={cn(
            'border-l px-3.5 py-1.5 font-mono text-xs first:border-l-0',
            value === option
              ? 'bg-foreground text-background'
              : 'text-muted-foreground hover:text-foreground',
          )}
        >
          {option === ANY ? 'all' : option}
        </button>
      ))}
    </div>
  )
}

function Chip({
  active,
  onClick,
  children,
}: {
  active: boolean
  onClick: () => void
  children: React.ReactNode
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={active}
      className={cn(
        'flex items-center gap-2 rounded-full border px-3.5 py-1.5 font-mono text-xs',
        active
          ? 'border-observatory-highlight-line bg-observatory-highlight text-primary'
          : 'text-muted-foreground hover:text-foreground',
      )}
    >
      {children}
    </button>
  )
}

function Attention({
  tasks,
  snapshot,
  now,
  onSelect,
}: {
  tasks: TaskRow[]
  snapshot: Snapshot
  now: number
  onSelect: (taskId: string) => void
}) {
  const questions = useAttentionQuestions(tasks.map((task) => task.task_id))
  const oldest = Math.max(...tasks.map((task) => now - task.updated_at_millis))

  return (
    <section className="space-y-3">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
        <span className="flex items-center gap-2.5 text-primary">
          <Icon name="bell" size={15} />
          <span className="font-mono text-[11px] tracking-[0.1em] uppercase">
            Waiting on you · {tasks.length}
          </span>
        </span>
        <span className="text-xs text-observatory-hollow">
          a turn ended with a question and the next turn cannot start until you answer
        </span>
        <span className="ml-auto font-mono text-[11px] tracking-[0.04em] text-observatory-hollow">
          OLDEST HAS WAITED {duration(oldest)}
        </span>
      </div>

      {tasks.map((task) => {
        const run = snapshot.runs.find((entry) => entry.run_id === task.run_id)
        const asked = questions[task.task_id] ?? []
        return (
          <article
            key={task.task_id}
            className="overflow-hidden rounded-[10px] border border-observatory-highlight-line bg-observatory-highlight"
          >
            <button
              type="button"
              onClick={() => onSelect(task.task_id)}
              className="flex w-full flex-wrap items-center gap-x-4 gap-y-1.5 px-5.5 py-3.5 text-left"
            >
              <span className="flex w-47.5 shrink-0 flex-wrap items-center gap-x-2 gap-y-1">
                <StatusMark value={task.state} />
                <StatusMark value="needs_input" />
              </span>
              <span className="min-w-0 flex-1 truncate text-base">{task.title}</span>
              <span className="w-37.5 shrink-0 truncate font-mono text-xs text-observatory-accent-soft">
                {run ? (run.name ?? shortId(run.run_id)) : '—'}
              </span>
              <span className="w-57.5 shrink-0 truncate font-mono text-xs text-muted-foreground">
                {[task.agent, task.model, task.effort].filter(Boolean).join(' · ')}
              </span>
              <span className="flex w-26.5 shrink-0 items-center gap-1.5 font-mono text-xs text-muted-foreground">
                <Icon name="cpu" size={12} className="text-observatory-hollow" />
                {task.worker ?? '—'}
              </span>
              <span className="flex w-23.5 shrink-0 items-center gap-1.5 font-mono text-xs text-muted-foreground">
                <Icon name="repeat" size={12} className="text-observatory-hollow" />
                {task.turn_count} {task.turn_count === 1 ? 'turn' : 'turns'}
              </span>
              <span className="w-15 shrink-0 text-right font-mono text-xs text-primary">
                {duration(now - task.updated_at_millis)}
              </span>
            </button>

            <div className="flex flex-wrap items-start gap-x-6 gap-y-4 border-t border-observatory-highlight-line px-5.5 py-4 pl-57">
              <div className="min-w-90 flex-1 space-y-2.5">
                {asked == null ? (
                  <p className="text-sm text-observatory-hollow">Reading the question…</p>
                ) : asked.length === 0 ? (
                  <p className="text-sm text-observatory-hollow">
                    The task record carries no question — open the task to read its last turn.
                  </p>
                ) : (
                  asked.map((question, index) => (
                    <div key={index} className="space-y-2.5">
                      <p className="max-w-170 text-[15px] leading-5.5">{questionText(question)}</p>
                      {questionOptions(question).length > 0 ? (
                        <div className="flex flex-wrap items-center gap-2.5">
                          {questionOptions(question).map((option) => (
                            <span
                              key={option}
                              className="rounded-full border border-observatory-highlight-line bg-card px-3.5 py-1.5 font-mono text-xs text-primary"
                            >
                              {option}
                            </span>
                          ))}
                          <span className="text-xs text-observatory-hollow">
                            answer with one of these, verbatim
                          </span>
                        </div>
                      ) : (
                        <p className="text-xs text-observatory-hollow">
                          the agent offered no options — this one needs a written answer
                        </p>
                      )}
                    </div>
                  ))
                )}
              </div>

              <div className="w-full max-w-115 shrink-0 space-y-2">
                <CommandList commands={[`worker task say ${task.task_id} --message-file reply.md`]} />
                <p className="text-right font-mono text-[11px] text-observatory-hollow">
                  your answer starts turn {task.turn_count + 1}
                </p>
              </div>
            </div>
          </article>
        )
      })}
    </section>
  )
}

/** Filters are local to the browser, as in the Rust client: they issue no requests. */
export function Tasks({
  snapshot,
  now = Date.now(),
  onSelect,
}: {
  snapshot: Snapshot
  now?: number
  onSelect: (taskId: string) => void
}) {
  const [state, setState] = useState(ANY)
  const [outcome, setOutcome] = useState(ANY)
  const [run, setRun] = useState(ANY)
  const [query, setQuery] = useState('')

  const states = useMemo(
    () => [...new Set(snapshot.tasks.map((task) => task.state))].sort(),
    [snapshot.tasks],
  )
  const outcomes = useMemo(() => countBy(snapshot.tasks, outcomeOf), [snapshot.tasks])

  const rows = useMemo(() => {
    const needle = query.trim().toLowerCase()
    return snapshot.tasks.filter((task) => {
      if (state !== ANY && task.state !== state) return false
      if (outcome !== ANY && outcomeOf(task) !== outcome) return false
      if (run !== ANY && task.run_id !== run) return false
      if (!needle) return true
      return (
        task.title.toLowerCase().includes(needle) || task.task_id.toLowerCase().includes(needle)
      )
    })
  }, [snapshot.tasks, state, outcome, run, query])

  const attention = useMemo(() => snapshot.tasks.filter(needsInput), [snapshot.tasks])
  const hidden = snapshot.tasks.length - rows.length
  const filtered = state !== ANY || outcome !== ANY || run !== ANY || query.trim() !== ''

  const command = [
    'worker task list',
    state === ANY ? '' : `--state ${state}`,
    outcome === ANY ? '' : `--outcome ${outcome.replace(/_/g, '-')}`,
    run === ANY ? '' : `--run ${run}`,
  ]
    .filter(Boolean)
    .join(' ')

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-end justify-between gap-x-10 gap-y-4">
        <div className="space-y-3.5">
          <div className="flex items-center gap-2.5">
            <span className="flex w-18.5 shrink-0 items-center gap-1.5 font-mono text-[10px] tracking-[0.06em] text-observatory-hollow">
              <Icon name="sliders" size={12} />
              STATE
            </span>
            <Segmented value={state} options={states} onChange={setState} label="State" />
          </div>
          <div className="flex flex-wrap items-center gap-2.5">
            <span className="flex w-18.5 shrink-0 items-center gap-1.5 font-mono text-[10px] tracking-[0.06em] text-observatory-hollow">
              <Icon name="funnel" size={12} />
              OUTCOME
            </span>
            <Chip active={outcome === ANY} onClick={() => setOutcome(ANY)}>
              any
            </Chip>
            {[...outcomes.entries()].map(([kind, count]) => (
              <Chip
                key={kind}
                active={outcome === kind}
                onClick={() => setOutcome(outcome === kind ? ANY : kind)}
              >
                <StatusMark value={kind} size={12} className="text-[12px] normal-case" />
                <span className={outcome === kind ? 'text-observatory-accent-soft' : 'text-observatory-hollow'}>
                  {count}
                </span>
              </Chip>
            ))}
          </div>
          <div className="flex flex-wrap items-center gap-2.5">
            <span className="flex w-18.5 shrink-0 items-center gap-1.5 font-mono text-[10px] tracking-[0.06em] text-observatory-hollow">
              <Icon name="list" size={12} />
              SEARCH
            </span>
            <Input
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Filter by title or id"
              className="h-8 w-65 font-mono text-xs"
              aria-label="Filter tasks"
            />
            <span className="font-mono text-xs text-observatory-hollow">
              {rows.length} of {snapshot.tasks.length}
            </span>
          </div>
        </div>

        <div className="space-y-2.5">
          <p className="flex items-center gap-1.5 font-mono text-[10px] tracking-[0.06em] text-observatory-hollow">
            <Icon name="terminal" size={12} />
            THE SAME VIEW FROM THE TERMINAL
          </p>
          <CommandList commands={[command]} />
        </div>
      </div>

      {attention.length > 0 ? (
        <Attention tasks={attention} snapshot={snapshot} now={now} onSelect={onSelect} />
      ) : null}

      {filtered ? (
        <div className="flex flex-wrap items-center gap-x-5 gap-y-2 rounded-[10px] border bg-card px-5.5 py-4">
          <span className="flex items-center gap-2 text-muted-foreground">
            <Icon name="eyeOff" />
            <span className="font-mono text-[11px] tracking-[0.1em]">HIDDEN BY THE FILTER</span>
          </span>
          <span className="flex-1 text-[13px]">
            {hidden} of {snapshot.tasks.length} tasks do not match
          </span>
          <button
            type="button"
            onClick={() => {
              setState(ANY)
              setOutcome(ANY)
              setRun(ANY)
              setQuery('')
            }}
            className="flex items-center gap-2 rounded-md border px-4 py-1.5 font-mono text-[11px] tracking-[0.06em]"
          >
            <Icon name="x" size={12} />
            CLEAR THE FILTER
          </button>
        </div>
      ) : null}

      {snapshot.runs.length > 0 ? (
        <div className="flex flex-wrap items-center gap-x-5 gap-y-2.5 rounded-[10px] border bg-card px-5.5 py-3.5">
          <span className="flex items-center gap-2 text-observatory-hollow">
            <Icon name="grid" size={12} />
            <span className="font-mono text-[11px] tracking-[0.1em]">BY RUN</span>
          </span>
          {snapshot.runs.map((entry) => (
            <Chip
              key={entry.run_id}
              active={run === entry.run_id}
              onClick={() => setRun(run === entry.run_id ? ANY : entry.run_id)}
            >
              <span className={run === entry.run_id ? '' : 'text-observatory-accent-soft'}>
                {entry.name ?? shortId(entry.run_id)}
              </span>
              <span className="text-[11px] text-observatory-hollow">
                {entry.progress.total} {entry.progress.total === 1 ? 'task' : 'tasks'}
                {entry.progress.queued > 0 ? ` · ${entry.progress.queued} waiting` : ''}
              </span>
            </Chip>
          ))}
          <span className="text-xs text-observatory-hollow">pick one to see only its tasks</span>
        </div>
      ) : null}

      <section className="overflow-hidden rounded-[10px] border bg-card">
        <table className="w-full table-fixed border-collapse">
          <thead>
            <tr className="bg-muted/60 text-left font-mono text-[10px] tracking-[0.06em] text-observatory-hollow">
              <th className="w-47.5 py-2.5 pl-5.5 font-normal">STATE</th>
              <th className="py-2.5 pr-5 font-normal">TASK</th>
              <th className="w-57.5 py-2.5 pr-5 font-normal">AGENT</th>
              <th className="w-26.5 py-2.5 pr-5 font-normal">WORKER</th>
              <th className="w-23.5 py-2.5 pr-5 font-normal">TURNS</th>
              <th className="w-24 py-2.5 pr-5.5 text-right font-normal">UPDATED</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((task) => (
              <tr
                key={task.task_id}
                onClick={() => onSelect(task.task_id)}
                className="cursor-pointer border-t align-top hover:bg-muted/40"
              >
                <td className="py-3.5 pl-5.5">
                  <span className="flex flex-wrap items-center gap-x-2 gap-y-1">
                    <StatusMark value={task.state} />
                    {task.last_outcome ? <StatusMark value={task.last_outcome.kind} /> : null}
                  </span>
                </td>
                <td className="min-w-0 py-3.5 pr-5">
                  <div className="truncate text-sm">{task.title}</div>
                  <div className="mt-1 truncate font-mono text-[11px] text-observatory-hollow">
                    {shortId(task.task_id, 12)}
                    {task.blocking_code
                      ? ` · ${blockingDetail(task.blocking_code).label}`
                      : ''}
                  </div>
                </td>
                <td className="py-3.5 pr-5 font-mono text-xs text-muted-foreground">
                  <div className="truncate">
                    {[task.agent, task.model, task.effort].filter(Boolean).join(' · ')}
                  </div>
                </td>
                <td className="py-3.5 pr-5 font-mono text-xs text-muted-foreground">
                  {task.worker ?? '—'}
                </td>
                <td className="py-3.5 pr-5 font-mono text-xs text-muted-foreground tabular-nums">
                  {task.turn_count}
                </td>
                <td className="py-3.5 pr-5.5 text-right text-xs text-muted-foreground">
                  {relativeTime(task.updated_at_millis, now)}
                </td>
              </tr>
            ))}
            {rows.length === 0 ? (
              <tr className="border-t">
                <td colSpan={6} className="py-10 text-center text-sm text-muted-foreground">
                  No task matches these filters.
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </section>
    </div>
  )
}
