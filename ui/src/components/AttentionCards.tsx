import { useRef, useState } from 'react'
import { GitBranch, MessageSquareText, Reply } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { acceptTask, integrateTask, taskMutationForDetail, questionText, type Snapshot, type TaskRow } from '@/lib/api'
import { integrationAnnotationConfirms } from '@/lib/integration.contract'
import { relativeTime } from '@/lib/format'
import { blockedIntegration, integrationDependencyFailure, needsAnswer, readyForReview, taskEventKey } from '@/lib/taskPresentation'
import { useTaskPreviews, type TaskPreviews } from '@/hooks/useTaskPreviews'

export function AttentionCards({
  snapshot,
  onSelect,
  previews: provided,
  now = Date.now(),
}: {
  snapshot: Snapshot
  onSelect?: (id: string) => void
  previews?: TaskPreviews
  now?: number
}) {
  const questions = snapshot.tasks.filter(needsAnswer)
  const reviews = snapshot.tasks.filter(readyForReview)
  const blocked = snapshot.tasks.filter(blockedIntegration)
  const dependencies = snapshot.tasks.filter(integrationDependencyFailure)
  const tasks = [...questions, ...reviews, ...blocked, ...dependencies]
  const local = useTaskPreviews(tasks, provided === undefined)
  const previews = provided ?? local
  const [actions, setActions] = useState<Record<string, { binding: string; busy: boolean; error: string | null; done: boolean }>>({})
  const inFlight = useRef(new Set<string>())
  const intents = useRef<Record<string, string>>({})
  const matchingPreview = (task: TaskRow) => {
    if (!task.integration) return null
    const detail = previews[task.task_id]
    const full = detail?.integration ?? detail?.task?.integration
    const compact = task.integration
    return detail && detail.task?.task_id === task.task_id && full && compact && integrationAnnotationConfirms({
      integration_id: compact.integration_id, epoch: compact.epoch, revision: compact.revision,
      state: compact.state, code: compact.blocked_code, result_oid: compact.merge_oid ?? compact.observed_target_oid,
    }, full) ? detail : null
  }
  const mutateIntegration = async (task: TaskRow, kind: 'integrate' | 'close') => {
    const detail = matchingPreview(task)
    const integration = task.integration
    if (!detail || !integration || inFlight.current.has(task.task_id)) return
    const binding = taskEventKey(task)
    inFlight.current.add(task.task_id)
    setActions(current => ({ ...current, [task.task_id]: { binding, busy: true, error: null, done: false } }))
    try {
      const body = taskMutationForDetail(detail)
      if (kind === 'integrate') {
        intents.current[binding] ??= crypto.randomUUID().replaceAll('-', '')
        await integrateTask(task.task_id, { expected: body, expected_integration_id: integration.integration_id,
          integration: { task_id: task.task_id, expected: integration.revision, request_id: intents.current[binding] } })
      } else await acceptTask(task.task_id, body)
      setActions(current => ({ ...current, [task.task_id]: { binding, busy: false, error: null, done: true } }))
    } catch (cause) {
      setActions(current => ({ ...current, [task.task_id]: { binding, busy: false, error: cause instanceof Error ? cause.message : String(cause), done: false } }))
    } finally { inFlight.current.delete(task.task_id) }
  }
  if (!tasks.length) return null
  return (
    <section aria-labelledby="attention-title">
      <div className="mw-section-heading">
        <h1 id="attention-title" className="mw-section-title">
          Needs your attention
        </h1>
        <p className="text-[13px] text-muted-foreground">
          {questions.length} {questions.length === 1 ? 'question' : 'questions'} · {reviews.length}{' '}
          ready for review{blocked.length ? ` · ${blocked.length} integration blocked` : ''}
          {dependencies.length ? ` · ${dependencies.length} dependency failed` : ''}
        </p>
      </div>
      <div className="mw-card-grid">
        {tasks.map((task) => {
          const question = needsAnswer(task)
          const integrationBlocked = blockedIntegration(task)
          const dependencyFailed = integrationDependencyFailure(task)
          const integrationError = integrationBlocked || dependencyFailed
          const detail = previews[task.task_id]
          const action = actions[task.task_id]?.binding === taskEventKey(task) ? actions[task.task_id] : undefined
          const canAct = task.state === 'open' && matchingPreview(task) !== null
          const Icon = question ? MessageSquareText : GitBranch
          return (
            <article
              key={task.task_id}
              className="mw-panel mw-attention-card"
              data-kind={integrationError ? 'error' : question ? 'question' : 'review'}
            >
              <div
                className={
                  'flex w-full items-center justify-between gap-3 text-[13px] ' +
                  (integrationError ? 'text-destructive' : question ? 'text-warning' : 'text-success')
                }
              >
                <span className="inline-flex items-center gap-2 font-medium">
                  <Icon size={15} strokeWidth={1.5} aria-hidden="true" />
                  {integrationBlocked ? 'Integration blocked' : dependencyFailed ? 'Dependency not integrated' : question ? 'Needs answer' : 'Ready for review'}
                </span>
                <span className={'text-xs ' + (question ? '' : 'text-muted-foreground')}>
                  {question ? 'Waiting ' : 'Ready '}
                  {relativeTime(task.updated_at_millis, now).replace(/ ago$/, '')}
                </span>
              </div>
              <h3>{task.title}</h3>
              {integrationError ? (
                <p><span>{task.integration!.target}</span><br /><span>{task.integration!.blocked_code ?? task.blocking_code}</span></p>
              ) : question ? (
                <p>
                  {detail?.questions?.length
                    ? detail.questions.map(questionText).join(' ')
                    : detail === undefined
                      ? 'Reading the question…'
                      : detail === null
                        ? 'Could not load the question. Open the task to retry.'
                        : 'No question was recorded. Open the task for context.'}
                </p>
              ) : (
                <p className="text-muted-foreground">
                  {detail?.diff_stat ??
                    (detail?.files_changed?.length
                      ? detail.files_changed.length + ' changed files'
                      : 'A result is ready to inspect.')}
                  <br />
                  {task.worker ?? 'Mac not reported'}
                </p>
              )}
              {integrationBlocked ? (
                <div className="flex flex-wrap gap-2">
                  <Button disabled={!canAct || action?.busy || action?.done} onClick={() => void mutateIntegration(task, 'integrate')}>Re-drive integration</Button>
                  <Button variant="outline" disabled={!canAct || action?.busy || action?.done} onClick={() => void mutateIntegration(task, 'close')}>Close task</Button>
                  {action?.error ? <p role="alert">{action.error}</p> : null}
                  {action?.done ? <p role="status">Action requested. Waiting for task update.</p> : null}
                </div>
              ) : null}
              <Button
                variant={question ? 'default' : 'outline'}
                onClick={() => onSelect?.(task.task_id)}
              >
                {question ? (
                  <Reply size={16} aria-hidden="true" />
                ) : (
                  <GitBranch size={16} aria-hidden="true" />
                )}
                {integrationBlocked ? 'Recover integration' : dependencyFailed ? 'Open task' : question ? 'Answer' : 'Review changes'}
              </Button>
            </article>
          )
        })}
      </div>
    </section>
  )
}
