import { useEffect, useRef, useState } from 'react'

import { CommandList } from '@/components/CommandList'
import { Icon, type IconName } from '@/components/Icon'
import { StatusMark } from '@/components/StatusMark'
import { TurnLogPanel } from '@/components/TurnLogPanel'
import { duration, humanize, relativeTime, shortId } from '@/lib/format'
import { cn } from '@/lib/utils'
import {
  acceptTask,
  ApiError,
  fetchTaskDetail,
  questionOptions,
  questionText,
  replyToTask,
  type ReportedCheck,
  type TaskDetail as TaskDetailPayload,
  type TaskMutation,
  type TurnRow,
} from '@/lib/api'

const POLL_INTERVAL_MS = 2000

function Fact({ icon, label, value }: { icon: IconName; label: string; value: string }) {
  return (
    <div className="min-w-0 flex-1 border-l pl-8.5 first:border-l-0 first:pl-0">
      <dt className="flex items-center gap-1.5 font-mono text-[10px] tracking-[0.06em] text-observatory-hollow">
        <Icon name={icon} size={11} />
        {label}
      </dt>
      <dd className="mt-1.5 truncate font-mono text-[13px]">{value}</dd>
    </div>
  )
}

function Head({ icon, label }: { icon: IconName; label: string }) {
  return (
    <span className="flex items-center gap-2 text-muted-foreground">
      <Icon name={icon} />
      <span className="font-mono text-[11px] tracking-[0.1em]">{label}</span>
    </span>
  )
}

function fileIcon(path: string): IconName {
  if (/(^|\/)(tests?|spec)\//.test(path) || /\.(test|spec)\./.test(path)) return 'fileCheck'
  if (/\.(md|txt|rst|adoc)$/.test(path)) return 'fileText'
  if (/\.[a-z0-9]+$/i.test(path)) return 'fileCode'
  return 'file'
}

function checkMark(status: ReportedCheck['status']): string {
  if (status === 'pass') return 'PASS'
  if (status === 'fail') return 'FAIL'
  if (status === 'error') return 'ERROR'
  return 'NOT RUN'
}

function mutationBody(detail: TaskDetailPayload, message?: string): TaskMutation {
  const turns = detail.timeline.length > 0 ? detail.timeline : detail.turns
  return {
    message,
    expected_task_id: detail.task.task_id,
    expected_turn_id: turns.at(-1)?.turn_id ?? null,
    expected_turn_count: detail.task.turn_count,
    expected_head_oid: detail.head_oid,
    expected_updated_at_millis: detail.task.updated_at_millis,
    expected_state: detail.task.state,
  }
}

function turnSpan(turn: TurnRow): string {
  if (turn.started_at_millis == null) return 'not started'
  if (turn.ended_at_millis == null) return `started ${relativeTime(turn.started_at_millis)}`
  return duration(turn.ended_at_millis - turn.started_at_millis)
}

function Turn({ taskId, turn, open, onToggle }: {
  taskId: string
  turn: TurnRow
  open: boolean
  onToggle: () => void
}) {
  const live = turn.ended_at_millis == null && turn.started_at_millis != null
  const waitingToStart = turn.started_at_millis == null && turn.ended_at_millis == null

  return (
    <div className="overflow-hidden rounded-[10px] border bg-card">
      <button
        type="button"
        onClick={onToggle}
        aria-expanded={open}
        className="flex w-full flex-wrap items-center gap-x-4 gap-y-1 px-5 py-3.5 text-left"
      >
        <span className="w-6 shrink-0 font-mono text-[15px]">
          {String(turn.turn_number).padStart(2, '0')}
        </span>
        <StatusMark value={turn.outcome?.kind ?? 'unknown'} size={14} />
        {turn.terminal && turn.terminal !== turn.outcome?.kind ? (
          <StatusMark value={turn.terminal} size={14} />
        ) : null}
        <span className="flex items-center gap-1.5 font-mono text-xs text-muted-foreground">
          <Icon name="clock" size={12} />
          {turnSpan(turn)}
        </span>
        <span className="flex-1 text-xs text-muted-foreground">
          {turn.started_at_millis == null
            ? shortId(turn.turn_id, 12)
            : `started ${relativeTime(turn.started_at_millis)}`}
        </span>
        {turn.log_truncated ? (
          <span className="font-mono text-[11px] text-primary">LOG TRUNCATED</span>
        ) : null}
        <Icon name={open ? 'chevronDown' : 'chevronRight'} size={14} className="text-muted-foreground" />
      </button>

      {open ? (
        waitingToStart ? (
          <p className="px-5 py-3.5 text-sm text-muted-foreground">
            Waiting for this turn to start…
          </p>
        ) : (
          <TurnLogPanel
            taskId={taskId}
            turnId={turn.turn_id}
            turnNumber={turn.turn_number}
            live={live}
            truncated={turn.log_truncated}
          />
        )
      ) : null}
    </div>
  )
}

export function TaskDetail({ taskId, onBack }: { taskId: string; onBack?: () => void }) {
  const [detail, setDetail] = useState<TaskDetailPayload | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [openTurn, setOpenTurn] = useState<string | null>(null)
  const [draft, setDraft] = useState('')
  const [actionError, setActionError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const mutationGeneration = useRef(0)
  const mutationController = useRef<AbortController | null>(null)
  const mutationInFlight = useRef(false)
  const detailEpoch = useRef(0)
  const pollController = useRef<AbortController | null>(null)
  const resumePoll = useRef<() => void>(() => {})

  useEffect(() => {
    let cancelled = false
    let timer: ReturnType<typeof setTimeout> | null = null
    mutationGeneration.current += 1
    mutationController.current?.abort()
    mutationController.current = null
    mutationInFlight.current = false
    detailEpoch.current += 1
    pollController.current?.abort()
    pollController.current = null
    setDetail(null)
    setError(null)
    setActionError(null)
    setDraft('')
    setBusy(false)
    setOpenTurn(null)

    const schedule = () => {
      if (cancelled || timer !== null) return
      timer = setTimeout(() => {
        timer = null
        void poll()
      }, POLL_INTERVAL_MS)
    }

    const poll = async () => {
      if (cancelled) return
      if (mutationInFlight.current) {
        schedule()
        return
      }
      pollController.current?.abort()
      const controller = new AbortController()
      pollController.current = controller
      const epoch = detailEpoch.current
      try {
        const payload = await fetchTaskDetail(taskId, controller.signal)
        if (
          !cancelled &&
          !controller.signal.aborted &&
          detailEpoch.current === epoch &&
          !mutationInFlight.current
        ) {
          setDetail(payload)
          setError(null)
        }
      } catch (cause) {
        if (
          !cancelled &&
          !controller.signal.aborted &&
          detailEpoch.current === epoch
        ) {
          setError(cause instanceof Error ? cause.message : String(cause))
        }
      }
      if (!cancelled && pollController.current === controller) schedule()
    }

    resumePoll.current = schedule
    void poll()
    return () => {
      cancelled = true
      resumePoll.current = () => {}
      pollController.current?.abort()
      pollController.current = null
      mutationController.current?.abort()
      mutationController.current = null
      if (timer !== null) clearTimeout(timer)
    }
  }, [taskId])

  const runMutation = async (kind: 'reply' | 'accept', message?: string) => {
    if (!detail || mutationInFlight.current) return
    const generation = mutationGeneration.current
    mutationController.current?.abort()
    const controller = new AbortController()
    mutationController.current = controller
    mutationInFlight.current = true
    detailEpoch.current += 1
    pollController.current?.abort()
    pollController.current = null
    setBusy(true)
    setActionError(null)
    try {
      const next =
        kind === 'reply'
          ? await replyToTask(taskId, mutationBody(detail, message), controller.signal)
          : await acceptTask(taskId, mutationBody(detail), controller.signal)
      if (mutationGeneration.current !== generation || controller.signal.aborted) return
      detailEpoch.current += 1
      setDetail(next)
      if (kind === 'reply') setDraft('')
    } catch (cause) {
      if (controller.signal.aborted || mutationGeneration.current !== generation) return
      setActionError(cause instanceof ApiError ? cause.message : String(cause))
    } finally {
      if (
        mutationGeneration.current === generation &&
        mutationController.current === controller
      ) {
        mutationInFlight.current = false
        setBusy(false)
        resumePoll.current()
      }
    }
  }

  if (error && detail == null) return <p className="text-sm text-destructive">{error}</p>
  if (!detail) return <p className="text-sm text-muted-foreground">Loading task…</p>

  const { task } = detail
  const turns = detail.timeline.length > 0 ? detail.timeline : detail.turns

  return (
    <div className="space-y-5">
      {error ? <p className="text-sm text-destructive">{error}</p> : null}
      <nav className="flex items-center gap-2.5 text-xs">
        <button
          type="button"
          onClick={onBack}
          className="flex items-center gap-2.5 text-muted-foreground hover:text-foreground"
        >
          <Icon name="list" size={14} className="text-observatory-hollow" />
          Tasks
        </button>
        <Icon name="chevronRight" size={11} className="text-border" />
        <span className="truncate">{task.title}</span>
      </nav>

      <header className="flex flex-wrap items-start gap-x-10 gap-y-4">
        <div className="min-w-0 flex-1 space-y-2.5">
          <h1 className="text-[34px] leading-10 font-medium tracking-[-0.03em]">{task.title}</h1>
          <div className="flex flex-wrap items-center gap-x-4.5 gap-y-1.5">
            <StatusMark value={task.state} />
            {task.last_outcome ? <StatusMark value={task.last_outcome.kind} /> : null}
            <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
              <Icon name="cpu" /> {task.worker ?? 'unassigned'}
            </span>
            <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
              <Icon name="repeat" /> {task.turn_count} {task.turn_count === 1 ? 'turn' : 'turns'}
            </span>
            <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
              <Icon name="clock" /> updated {relativeTime(task.updated_at_millis)}
            </span>
          </div>
        </div>
        <dl className="flex shrink-0 gap-8.5 pt-1.5">
          {(
            [
              ['spark', 'AGENT', humanize(task.agent)],
              ['stack', 'MODEL', task.model ?? 'Agent default'],
              ['gauge', 'EFFORT', task.effort ? humanize(task.effort) : 'Not set'],
            ] as [IconName, string, string][]
          ).map(([icon, label, value]) => (
            <div key={label}>
              <dt className="flex items-center gap-1.5 font-mono text-[11px] tracking-[0.06em] text-muted-foreground">
                <Icon name={icon} size={11} />
                {label}
              </dt>
              <dd
                className={cn(
                  'mt-2 text-lg leading-5.5',
                  label === 'MODEL' && 'max-w-80 truncate font-mono',
                )}
                title={value}
              >
                {value}
              </dd>
            </div>
          ))}
        </dl>
      </header>

      <dl className="flex flex-wrap gap-y-4 border-y py-3.5">
        <Fact icon="hash" label="TASK" value={shortId(task.task_id, 12)} />
        <Fact
          icon="grid"
          label="RUN"
          value={
            task.run_id
              ? `${shortId(task.run_id, 12)}${task.run_position ? ` · ${task.run_position}` : ''}`
              : 'standalone'
          }
        />
        <Fact icon="branch" label="BRANCH" value={task.branch ?? 'not published'} />
        <Fact icon="commit" label="BASE" value={shortId(detail.base_oid, 12)} />
        <Fact icon="commit" label="HEAD" value={shortId(detail.head_oid, 12)} />
      </dl>

      <section className="overflow-hidden rounded-[10px] border bg-card">
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1 border-b px-5.5 py-3.5">
          <Head icon="fileCheck" label="RESULT" />
          <span className="ml-auto font-mono text-[11px] tracking-[0.04em] text-observatory-hollow uppercase">
            {turns.length} {turns.length === 1 ? 'turn' : 'turns'}
            {detail.session_present ? ' · session on the worker' : ''}
          </span>
        </div>

        <div className="flex flex-wrap items-stretch">
          <div className="min-w-90 flex-1 space-y-4 border-r px-5.5 py-5">
            <p className="max-w-150 text-sm leading-6 whitespace-pre-wrap">
              {detail.summary ?? 'The agent reported no summary.'}
            </p>

            {detail.questions.length > 0 ? (
              <div className="space-y-2.5 border-l-2 pl-4">
                <p className="flex items-center gap-1.5 font-mono text-[10px] tracking-[0.06em] text-observatory-hollow">
                  <Icon name="messageQuestion" size={12} />
                  WAITING ON YOU
                </p>
                {detail.questions.map((question, index) => (
                  <div key={index} className="space-y-2">
                    <p className="max-w-150 text-sm leading-5.5">{questionText(question)}</p>
                    {questionOptions(question).length > 0 ? (
                      <div className="flex flex-wrap gap-2">
                        {questionOptions(question).map((option) => (
                          <button
                            key={option}
                            type="button"
                            onClick={() => setDraft(option)}
                            className="rounded-full border border-observatory-highlight-line bg-observatory-highlight px-3 py-1.5 font-mono text-xs text-primary"
                          >
                            {option}
                          </button>
                        ))}
                      </div>
                    ) : (
                      <p className="text-xs text-observatory-hollow">
                        the agent offered no options — this one needs a written answer
                      </p>
                    )}
                  </div>
                ))}
              </div>
            ) : null}

            {detail.reported_checks.length > 0 ? (
              <div className="space-y-2">
                <p className="flex items-center gap-1.5 font-mono text-[10px] tracking-[0.06em] text-observatory-hollow">
                  <Icon name="shield" size={12} />
                  AGENT REPORTED
                </p>
                <ul className="space-y-2">
                  {detail.reported_checks.map((check) => (
                    <li key={`${check.name}:${check.command}`} className="space-y-1">
                      <div className="flex flex-wrap items-baseline gap-2">
                        <span className="font-mono text-[11px] tracking-[0.06em] text-observatory-hollow">
                          {checkMark(check.status)}
                        </span>
                        <span className="text-sm">{check.name}</span>
                      </div>
                      {check.command ? (
                        <p className="font-mono text-[11px] text-muted-foreground">{check.command}</p>
                      ) : null}
                      {check.detail ? (
                        <p className="text-xs text-muted-foreground">{check.detail}</p>
                      ) : null}
                    </li>
                  ))}
                </ul>
              </div>
            ) : (
              <p className="font-mono text-[11px] tracking-[0.06em] text-observatory-hollow">
                AGENT REPORTED · not reported
              </p>
            )}
          </div>

          <div className="w-full max-w-101 shrink-0 space-y-3.5 px-5.5 py-5">
            <div className="space-y-1.5">
              <span className="flex items-center gap-2 font-mono text-[10px] tracking-[0.06em] text-observatory-hollow">
                <Icon name="fileDiff" size={12} />
                CHANGED FILES · {detail.files_changed.length}
              </span>
              {detail.diff_stat ? (
                <p className="font-mono text-[11px] break-words text-muted-foreground">
                  {detail.diff_stat}
                </p>
              ) : null}
            </div>
            {detail.files_changed.length === 0 ? (
              <p className="text-xs text-observatory-hollow">No file changed.</p>
            ) : (
              <ul className="space-y-2.5">
                {detail.files_changed.map((file) => (
                  <li key={file} className="flex items-center gap-3">
                    <Icon name={fileIcon(file)} className="text-observatory-hollow" />
                    <span className="truncate font-mono text-xs">{file}</span>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>
      </section>

      {['waiting_on_you', 'ready_for_review', 'ready_for_follow_up', 'close_pending'].includes(
        detail.review_state,
      ) ? (
        <section className="space-y-3 rounded-[10px] border bg-card px-5.5 py-5">
          <div className="flex flex-wrap items-center gap-x-4 gap-y-1">
            <Head icon="reply" label="REVIEW" />
            <span className="ml-auto font-mono text-[11px] tracking-[0.04em] text-observatory-hollow uppercase">
              {detail.review_state.replaceAll('_', ' ')}
            </span>
          </div>
          {detail.review_state === 'close_pending' ? (
            <p className="text-sm text-muted-foreground">
              Close is in progress on this task. Retry when you are ready; it will not double-close.
            </p>
          ) : (
            <textarea
              aria-label="Follow-up"
              value={draft}
              onChange={(event) => setDraft(event.target.value)}
              rows={3}
              className="w-full resize-y rounded-md border bg-background px-3 py-2 text-sm"
            />
          )}
          {actionError ? <p className="text-sm text-destructive">{actionError}</p> : null}
          <div className="flex flex-wrap gap-2">
            {detail.review_state !== 'close_pending' ? (
              <button
                type="button"
                disabled={busy || draft.trim() === ''}
                aria-busy={busy}
                onClick={() => void runMutation('reply', draft)}
                className="rounded-md border px-3.5 py-1.5 font-mono text-xs disabled:opacity-50"
              >
                Reply
              </button>
            ) : null}
            {detail.review_state === 'ready_for_review' ||
            detail.review_state === 'close_pending' ? (
              <button
                type="button"
                disabled={busy}
                aria-busy={busy}
                onClick={() => void runMutation('accept')}
                className="rounded-md border border-observatory-highlight-line bg-observatory-highlight px-3.5 py-1.5 font-mono text-xs text-primary disabled:opacity-50"
              >
                {detail.review_state === 'close_pending' ? 'Retry accept' : 'Accept'}
              </button>
            ) : null}
          </div>
        </section>
      ) : null}

      <section className="space-y-3">
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1">
          <Head icon="terminal" label="TAKE IT LOCALLY" />
          {task.branch ? (
            <span className="ml-auto flex items-center gap-2 font-mono text-[11px]">
              <Icon name="branch" size={12} className="text-observatory-hollow" />
              <span className="text-observatory-hollow">RESULT BRANCH</span>
              <span className="text-observatory-accent-soft">{task.branch}</span>
            </span>
          ) : null}
        </div>
        <CommandList
          commands={
            detail.review_commands.length > 0
              ? detail.review_commands
              : [detail.fetch_command, `worker task diff ${task.task_id} --stat`]
          }
        />
      </section>

      <section className="space-y-3">
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1">
          <Head icon="history" label="TURNS" />
          <span className="ml-auto font-mono text-[11px] tracking-[0.04em] text-observatory-hollow uppercase">
            {turns.length} recorded · open one to read its log
          </span>
        </div>
        {turns.length === 0 ? (
          <p className="rounded-[10px] border bg-card px-5 py-8 text-center text-sm text-muted-foreground">
            This task has no recorded turns.
          </p>
        ) : (
          <div className="space-y-3">
            {turns.map((turn) => (
              <Turn
                key={turn.turn_id}
                taskId={task.task_id}
                turn={turn}
                open={openTurn === turn.turn_id}
                onToggle={() => setOpenTurn(openTurn === turn.turn_id ? null : turn.turn_id)}
              />
            ))}
          </div>
        )}
      </section>
    </div>
  )
}
