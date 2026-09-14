import { useEffect, useRef, useState } from 'react'

import { CommandList } from '@/components/CommandList'
import { DeliveryChip } from '@/components/DeliveryChip'
import { Icon, type IconName } from '@/components/Icon'
import { StatusMark } from '@/components/StatusMark'
import { TurnLogPanel } from '@/components/TurnLogPanel'
import { duration, humanize, relativeTime, shortId } from '@/lib/format'
import { Check, ChevronRight, History, Monitor, Reply, Terminal } from 'lucide-react'
import { AgentMark } from '@/components/AgentMark'
import { TaskBadge } from '@/components/TaskBadge'
import { Button } from '@/components/ui/button'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
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

function Turn({
  taskId,
  turn,
  open,
  onToggle,
}: {
  taskId: string
  turn: TurnRow
  open: boolean
  onToggle: () => void
}) {
  const live = turn.ended_at_millis == null && turn.started_at_millis != null
  const waitingToStart = turn.started_at_millis == null && turn.ended_at_millis == null

  return (
    <Collapsible
      open={open}
      onOpenChange={onToggle}
      className="overflow-hidden border-b last:border-b-0 bg-card"
    >
      <CollapsibleTrigger
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
        {turn.log_truncated ? <span className="text-xs text-warning">LOG TRUNCATED</span> : null}
        <Icon
          name="chevronRight"
          size={14}
          className="mw-disclosure-chevron text-muted-foreground"
        />
      </CollapsibleTrigger>

      <CollapsibleContent>
        {waitingToStart ? (
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
        )}
      </CollapsibleContent>
    </Collapsible>
  )
}

export function TaskDetail({
  taskId,
  onBack,
  runName,
}: {
  taskId: string
  onBack?: () => void
  runName?: string
}) {
  const [detail, setDetail] = useState<TaskDetailPayload | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [openTurn, setOpenTurn] = useState<string | null>(null)
  const [draft, setDraft] = useState('')
  const replyInput = useRef<HTMLTextAreaElement>(null)
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
        if (!cancelled && !controller.signal.aborted && detailEpoch.current === epoch) {
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
      if (mutationGeneration.current === generation && mutationController.current === controller) {
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

  const waiting = detail.review_state === 'waiting_on_you'
  const reviewable = detail.review_state === 'ready_for_review'
  const closing = detail.review_state === 'close_pending'
  const replyable = waiting || reviewable || detail.review_state === 'ready_for_follow_up'
  const activeTurn =
    task.state === 'active'
      ? (turns.find((turn) => turn.turn_id === task.active_turn_id) ??
        turns.findLast((turn) => turn.started_at_millis != null && turn.ended_at_millis == null))
      : null
  const history = activeTurn ? turns.filter((turn) => turn.turn_id !== activeTurn.turn_id) : turns
  const replyForm = (
    <div className="space-y-3">
      <label
        htmlFor="task-reply"
        className={waiting ? 'text-[13px] text-warning' : 'text-[13px] text-muted-foreground'}
      >
        {waiting ? 'Your reply' : 'Follow-up'}
      </label>
      <textarea
        ref={replyInput}
        id="task-reply"
        aria-label="Follow-up"
        placeholder={
          waiting ? 'Write your answer…' : 'Describe what you’d like the agent to change…'
        }
        value={draft}
        disabled={busy}
        onChange={(event) => setDraft(event.target.value)}
        rows={4}
        className="mw-textarea block"
      />
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className={'text-[13px] ' + (waiting ? 'text-warning' : 'text-muted-foreground')}>
          Starts a new turn for this task.
        </p>
        <Button
          disabled={busy || !draft.trim()}
          aria-busy={busy}
          onClick={() => void runMutation('reply', draft)}
        >
          <Reply size={16} aria-hidden="true" />
          Send reply
        </Button>
      </div>
    </div>
  )
  return (
    <div className="mw-page">
      <nav className="flex items-center gap-3 text-[13px]" aria-label="Breadcrumb">
        <button className="mw-link" onClick={onBack}>
          Tasks
        </button>
        <ChevronRight size={13} className="text-muted-foreground" aria-hidden="true" />
        <span className="text-muted-foreground">Task details</span>
      </nav>
      {error ? (
        <p role="alert" className="mw-banner" data-tone="error">
          {error}
        </p>
      ) : null}
      <header className="mw-page-heading">
        <div>
          <h1 className="mw-page-title">{task.title}</h1>
          <div className="mt-3 flex flex-wrap items-center gap-x-5 gap-y-2 text-xs text-muted-foreground">
            <TaskBadge task={{ ...task, review_state: detail.review_state }} />
            <DeliveryChip task={detail} freshness={task.freshness} />
            <span className="inline-flex items-center gap-2">
              <Monitor size={14} aria-hidden="true" />
              {task.worker ?? 'Unassigned'}
            </span>
            <span className="inline-flex items-center gap-2">
              <History size={14} aria-hidden="true" />
              {task.turn_count} {task.turn_count === 1 ? 'turn' : 'turns'}
            </span>
            <span>Updated {relativeTime(task.updated_at_millis)}</span>
          </div>
        </div>
        {reviewable || closing ? (
          <div className="flex gap-3">
            {reviewable ? (
              <Button
                variant="outline"
                onClick={() => {
                  replyInput.current?.scrollIntoView({ block: 'center', behavior: 'smooth' })
                  replyInput.current?.focus()
                }}
              >
                <Reply size={16} aria-hidden="true" />
                Write follow-up
              </Button>
            ) : null}
            <Button disabled={busy} aria-busy={busy} onClick={() => void runMutation('accept')}>
              <Check size={16} aria-hidden="true" />
              {closing ? 'Retry accept' : 'Accept task'}
            </Button>
          </div>
        ) : null}
      </header>
      {actionError ? (
        <p role="alert" className="mw-banner" data-tone="error">
          {actionError}
        </p>
      ) : null}
      {closing ? (
        <p className="mw-banner">
          Close is in progress on this task. Retry when you are ready; it will not double-close.
        </p>
      ) : null}
      <div className="mw-detail-grid">
        <div className="min-w-0 space-y-5">
          {waiting ? (
            <section className="mw-panel mw-panel-pad border-[#FEF0C7] bg-warning-soft">
              <div className="mb-6 space-y-5">
                {detail.questions.length ? (
                  detail.questions.map((question, index) => (
                    <div key={index}>
                      <h2 className="text-xl leading-7 font-semibold">{questionText(question)}</h2>
                      <p className="mt-3 text-[13px] text-warning">
                        {questionOptions(question).length
                          ? 'Choose an option or write your own answer.'
                          : 'The agent offered no options. Write your answer below.'}
                      </p>
                      {questionOptions(question).length ? (
                        <div className="mt-4 flex flex-wrap gap-2">
                          {questionOptions(question).map((option) => (
                            <Button
                              key={option}
                              variant="outline"
                              aria-pressed={draft === option}
                              className={draft === option ? '!border-warning !text-warning' : ''}
                              disabled={busy}
                              onClick={() => {
                                setDraft(option)
                                replyInput.current?.focus()
                              }}
                            >
                              {draft === option ? <Check size={16} aria-hidden="true" /> : null}
                              {option}
                            </Button>
                          ))}
                        </div>
                      ) : null}
                    </div>
                  ))
                ) : (
                  <>
                    <h2 className="text-xl font-semibold">This task needs your answer</h2>
                    <p>Read the last turn for context, then reply below.</p>
                  </>
                )}
              </div>
              {replyForm}
            </section>
          ) : null}
          {activeTurn ? (
            <section className="mw-panel overflow-hidden">
              <div className="flex items-center justify-between gap-3 border-b p-6">
                <h2 className="mw-section-title flex items-center gap-3">
                  <AgentMark agent={task.agent} size={22} />
                  Turn {activeTurn.turn_number}
                </h2>
                <span className="text-xs text-muted-foreground">{turnSpan(activeTurn)}</span>
              </div>
              <TurnLogPanel
                taskId={task.task_id}
                turnId={activeTurn.turn_id}
                turnNumber={activeTurn.turn_number}
                live={true}
                truncated={activeTurn.log_truncated}
              />
            </section>
          ) : (
            <>
              <section className="mw-panel mw-panel-pad">
                <h2 className="mb-4 text-base font-semibold">
                  {waiting ? 'Agent context' : 'Result'}
                </h2>
                <p className="text-sm leading-6 whitespace-pre-wrap">
                  {detail.summary ??
                    (task.state === 'active'
                      ? 'Waiting for the current turn to report a result.'
                      : 'The agent reported no summary.')}
                </p>
                {detail.questions.length > 0 && !waiting ? (
                  <div className="mt-5 space-y-2">
                    {detail.questions.map((question, index) => (
                      <div key={index}>
                        <p>{questionText(question)}</p>
                        {questionOptions(question).length ? (
                          <p className="mt-1 text-xs text-muted-foreground">
                            {questionOptions(question).map((option) => (
                              <span className="mw-badge mr-2" key={option}>
                                {option}
                              </span>
                            ))}
                          </p>
                        ) : null}
                      </div>
                    ))}
                  </div>
                ) : null}
                {!waiting ? (
                  <div className="mt-6 border-t pt-5">
                    <div className="mb-4 flex items-center justify-between">
                      <h3 className="font-semibold">Checks</h3>
                      <span className="mw-help">Agent reported</span>
                    </div>
                    {detail.reported_checks.length ? (
                      <ul className="space-y-4">
                        {detail.reported_checks.map((check) => (
                          <li key={check.name + ':' + check.command} className="flex gap-3">
                            <span
                              className="mw-badge self-start"
                              data-tone={
                                check.status === 'pass'
                                  ? 'success'
                                  : check.status === 'fail' || check.status === 'error'
                                    ? 'error'
                                    : 'neutral'
                              }
                            >
                              {checkMark(check.status)}
                            </span>
                            <div className="min-w-0">
                              <p>{check.name}</p>
                              {check.command ? (
                                <p className="mt-1 break-all font-mono text-xs text-muted-foreground">
                                  {check.command}
                                </p>
                              ) : null}
                              {check.detail ? (
                                <p className="mt-1 text-xs text-muted-foreground">{check.detail}</p>
                              ) : null}
                            </div>
                          </li>
                        ))}
                      </ul>
                    ) : (
                      <p className="text-xs text-muted-foreground">AGENT REPORTED · not reported</p>
                    )}
                  </div>
                ) : null}
              </section>
              {waiting ? (
                <Collapsible className="mw-panel mw-panel-pad">
                  <CollapsibleTrigger className="mw-link w-full font-semibold">
                    <ChevronRight className="mw-disclosure-chevron" size={14} />
                    Partial result and terminal commands
                  </CollapsibleTrigger>
                  <CollapsibleContent keepMounted>
                    <div className="mt-5 space-y-5">
                      {detail.reported_checks.length ? (
                        <div>
                          <h3 className="mb-3 font-medium">
                            Checks{' '}
                            <span className="ml-2 text-xs font-normal text-muted-foreground">
                              Agent reported
                            </span>
                          </h3>
                          <ul className="space-y-3">
                            {detail.reported_checks.map((check) => (
                              <li key={check.name + check.command}>
                                <span
                                  className="mw-badge mr-2"
                                  data-tone={check.status === 'pass' ? 'success' : 'neutral'}
                                >
                                  {checkMark(check.status)}
                                </span>
                                {check.name}
                                {check.command ? (
                                  <p className="mt-1 break-all font-mono text-xs text-muted-foreground">
                                    {check.command}
                                  </p>
                                ) : null}
                                {check.detail ? (
                                  <p className="mt-1 text-xs text-muted-foreground">{check.detail}</p>
                                ) : null}
                              </li>
                            ))}
                          </ul>
                        </div>
                      ) : null}
                      <div>
                        <h3 className="mb-3 font-medium">
                          Changed files · {detail.files_changed.length}
                        </h3>
                        {detail.diff_stat ? (
                          <p className="mb-3 text-xs text-muted-foreground">{detail.diff_stat}</p>
                        ) : null}
                        {detail.files_changed.length ? (
                          <ul className="space-y-2">
                            {detail.files_changed.map((file) => (
                              <li className="break-all font-mono text-xs" key={file}>
                                {file}
                              </li>
                            ))}
                          </ul>
                        ) : (
                          <p className="text-xs text-muted-foreground">No file changed.</p>
                        )}
                      </div>
                      <CommandList
                        commands={[
                          ...(detail.review_commands.length
                            ? detail.review_commands
                            : [detail.fetch_command]
                          ).filter(Boolean),
                          `worker task say ${task.task_id} --message-file reply.md`,
                        ]}
                      />
                    </div>
                  </CollapsibleContent>
                </Collapsible>
              ) : null}
              {!waiting ? (
                <section className="mw-panel overflow-hidden">
                  <div className="flex flex-wrap items-center justify-between gap-3 border-b px-6 py-4">
                    <h2 className="font-semibold">
                      Changed files{' '}
                      <span className="ml-2 text-xs font-normal text-muted-foreground">
                        {detail.files_changed.length}
                      </span>
                    </h2>
                    {detail.diff_stat ? (
                      <span className="text-xs text-muted-foreground">{detail.diff_stat}</span>
                    ) : null}
                  </div>
                  {detail.files_changed.length ? (
                    <ul className="divide-y">
                      {detail.files_changed.map((file) => (
                        <li key={file} className="flex items-start gap-3 px-6 py-3">
                          <Icon
                            name={fileIcon(file)}
                            className="mt-0.5 shrink-0 text-muted-foreground"
                          />
                          <span className="break-all font-mono text-xs leading-5">{file}</span>
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <p className="px-6 py-4 text-xs text-muted-foreground">No file changed.</p>
                  )}
                </section>
              ) : null}
              {!waiting ? (
                <section className="mw-panel mw-panel-pad">
                  <h2 className="mb-4 flex items-center gap-2 font-semibold">
                    <Terminal size={16} aria-hidden="true" />
                    Take it locally
                  </h2>
                  <CommandList
                    commands={
                      detail.review_commands.length
                        ? detail.review_commands
                        : [detail.fetch_command, `worker task diff ${task.task_id} --stat`].filter(
                            Boolean,
                          )
                    }
                  />
                </section>
              ) : null}
              {replyable && !waiting ? (
                <section className="mw-panel mw-panel-pad">
                  <h2 className="mb-4 font-semibold">Ask for a change</h2>
                  {replyForm}
                </section>
              ) : null}
            </>
          )}
        </div>
        <aside className="mw-panel mw-panel-pad self-start">
          <h2 className="mw-section-title mb-6">Task details</h2>
          <dl className="mw-facts">
            <dt>Agent</dt>
            <dd>
              <AgentMark agent={task.agent} label />
            </dd>
            <dt>Model</dt>
            <dd>{task.model ?? 'Agent default'}</dd>
            <dt>Effort</dt>
            <dd>{task.effort ? humanize(task.effort) : 'Not reported'}</dd>
            <dt>Mac</dt>
            <dd>{task.worker ?? 'Unassigned'}</dd>
            <dt>Run</dt>
            <dd>{task.run_id ? (runName ?? shortId(task.run_id, 12)) : 'Standalone'}</dd>
            <dt>Task</dt>
            <dd className="font-mono text-xs" title={task.task_id}>
              {task.task_id}
            </dd>
            <dt>Branch</dt>
            <dd className="font-mono text-xs">{task.branch ?? 'Not published'}</dd>
            <dt>Base</dt>
            <dd className="font-mono text-xs" title={detail.base_oid ?? undefined}>
              {shortId(detail.base_oid, 12)}
            </dd>
            <dt>Head</dt>
            <dd className="font-mono text-xs" title={detail.head_oid ?? undefined}>
              {detail.head_oid ? shortId(detail.head_oid, 12) : 'Not reported'}
            </dd>
          </dl>
          <p className="mt-7 border-t pt-5 text-[13px] leading-6 text-muted-foreground">
            {waiting
              ? 'Replying starts a new agent turn. The Mac can be idle while this task waits for you.'
              : task.state === 'active'
                ? 'The current turn is still running. Its final result appears when the turn ends.'
                : reviewable
                  ? 'Review the result locally, then accept the task or send a follow-up.'
                  : humanize(detail.review_state)}
            {detail.session_present ? ' The agent session is on the worker.' : ''}
          </p>
        </aside>
      </div>
      <section className="mw-panel overflow-hidden">
        <div className="flex items-center justify-between gap-3 border-b px-6 py-4">
          <h2 className="font-semibold">{activeTurn ? 'Previous turns' : 'Turn history'}</h2>
          <span className="mw-help">{history.length} recorded · open one to read its log</span>
        </div>
        {history.length ? (
          <div>
            {history.map((turn) => (
              <Turn
                key={turn.turn_id}
                taskId={task.task_id}
                turn={turn}
                open={openTurn === turn.turn_id}
                onToggle={() => setOpenTurn(openTurn === turn.turn_id ? null : turn.turn_id)}
              />
            ))}
          </div>
        ) : (
          <p className="p-6 text-sm text-muted-foreground">
            {activeTurn ? 'No previous turns.' : 'This task has no recorded turns.'}
          </p>
        )}
      </section>
    </div>
  )
}
