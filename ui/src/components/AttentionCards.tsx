import { GitBranch, MessageSquareText, Reply } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { questionText, type Snapshot } from '@/lib/api'
import { relativeTime } from '@/lib/format'
import { needsAnswer, readyForReview } from '@/lib/taskPresentation'
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
  const tasks = [...questions, ...reviews]
  const local = useTaskPreviews(tasks, provided === undefined)
  const previews = provided ?? local
  if (!tasks.length) return null
  return (
    <section aria-labelledby="attention-title">
      <div className="mw-section-heading">
        <h1 id="attention-title" className="mw-section-title">
          Needs your attention
        </h1>
        <p className="text-[13px] text-muted-foreground">
          {questions.length} {questions.length === 1 ? 'question' : 'questions'} · {reviews.length}{' '}
          ready for review
        </p>
      </div>
      <div className="mw-card-grid">
        {tasks.map((task) => {
          const question = needsAnswer(task)
          const detail = previews[task.task_id]
          const Icon = question ? MessageSquareText : GitBranch
          return (
            <article
              key={task.task_id}
              className="mw-panel mw-attention-card"
              data-kind={question ? 'question' : 'review'}
            >
              <div
                className={
                  'flex w-full items-center justify-between gap-3 text-[13px] ' +
                  (question ? 'text-warning' : 'text-success')
                }
              >
                <span className="inline-flex items-center gap-2 font-medium">
                  <Icon size={15} strokeWidth={1.5} aria-hidden="true" />
                  {question ? 'Needs answer' : 'Ready for review'}
                </span>
                <span className={'text-xs ' + (question ? '' : 'text-muted-foreground')}>
                  {question ? 'Waiting ' : 'Ready '}
                  {relativeTime(task.updated_at_millis, now).replace(/ ago$/, '')}
                </span>
              </div>
              <h3>{task.title}</h3>
              {question ? (
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
              <Button
                variant={question ? 'default' : 'outline'}
                onClick={() => onSelect?.(task.task_id)}
              >
                {question ? (
                  <Reply size={16} aria-hidden="true" />
                ) : (
                  <GitBranch size={16} aria-hidden="true" />
                )}
                {question ? 'Answer' : 'Review changes'}
              </Button>
            </article>
          )
        })}
      </div>
    </section>
  )
}
