import { useEffect, useState } from 'react'
import { Dialog } from '@base-ui/react/dialog'
import {
  Bell,
  CheckCheck,
  Clock3,
  FileText,
  GitBranch,
  ListTodo,
  MessageSquareText,
  Reply,
  X,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { questionText, type Snapshot, type TaskRow } from '@/lib/api'
import { relativeTime } from '@/lib/format'
import { needsAnswer, readyForReview, taskEventKey, taskPresentation } from '@/lib/taskPresentation'
import type { TaskPreviews } from '@/hooks/useTaskPreviews'

const STORAGE = 'mac-worker:notification-read:v1'
function readSaved(): string[] {
  try {
    const value: unknown = JSON.parse(localStorage.getItem(STORAGE) ?? '[]')
    return Array.isArray(value)
      ? value.filter((key): key is string => typeof key === 'string').slice(-500)
      : []
  } catch {
    return []
  }
}
export function Notifications({
  snapshot,
  previews,
  onSelectTask,
}: {
  snapshot: Snapshot | null
  previews: TaskPreviews
  onSelectTask: (id: string) => void
}) {
  const [open, setOpen] = useState(false)
  const [unreadOnly, setUnreadOnly] = useState(false)
  const [read, setRead] = useState(readSaved)
  const tasks = snapshot?.tasks ?? []
  const action = tasks.filter((task) => needsAnswer(task) || readyForReview(task))
  const recent = tasks
    .filter((task) => !needsAnswer(task) && !readyForReview(task))
    .toSorted((a, b) => b.updated_at_millis - a.updated_at_millis)
    .slice(0, 20)
  const items = [...action, ...recent]
  const unread = items.filter((task) => !read.includes(taskEventKey(task)))
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE, JSON.stringify(read))
    } catch {
      /* Read state still works in memory. */
    }
  }, [read])
  const mark = (tasks: TaskRow[]) =>
    setRead((current) => [...new Set([...current, ...tasks.map(taskEventKey)])].slice(-500))
  const filtered = (tasks: TaskRow[]) =>
    unreadOnly ? tasks.filter((task) => !read.includes(taskEventKey(task))) : tasks
  const renderItem = (task: TaskRow) => {
    const presentation = taskPresentation(task)
    const needsAction = needsAnswer(task) || readyForReview(task)
    const Icon = needsAnswer(task)
      ? MessageSquareText
      : readyForReview(task)
        ? GitBranch
        : task.state === 'active'
          ? Clock3
          : task.state === 'queued'
            ? ListTodo
            : CheckCheck
    const detail = previews[task.task_id]
    const isUnread = !read.includes(taskEventKey(task))
    return (
      <article key={taskEventKey(task)} className="mw-notification-item">
        <Icon
          className={
            needsAnswer(task)
              ? 'text-warning'
              : readyForReview(task)
                ? 'text-success'
                : 'text-muted-foreground'
          }
          size={18}
          strokeWidth={1.5}
          aria-hidden="true"
        />
        <div className="min-w-0 flex-1">
          <div className="flex items-center justify-between gap-3 text-xs">
            <span
              className={
                needsAnswer(task)
                  ? 'text-warning'
                  : readyForReview(task)
                    ? 'text-success'
                    : 'text-muted-foreground'
              }
            >
              {presentation.label}
            </span>
            <span className="flex items-center gap-3 text-muted-foreground">
              {relativeTime(task.updated_at_millis)}
              {isUnread ? (
                <span className="size-1.5 rounded-full bg-[#F79009]" aria-label="Unread" />
              ) : null}
            </span>
          </div>
          <h3>{task.title}</h3>
          {needsAnswer(task) ? (
            <p>
              {detail?.questions?.length
                ? detail.questions.map(questionText).join(' ')
                : 'Open the task to read its question.'}
            </p>
          ) : readyForReview(task) ? (
            <p>
              {detail?.diff_stat ?? 'A result is ready to inspect.'}
              {task.worker ? ' · ' + task.worker : ''}
            </p>
          ) : null}
          {needsAction ? (
            <Button
              size="sm"
              variant={needsAnswer(task) ? 'default' : 'outline'}
              onClick={() => {
                mark([task])
                setOpen(false)
                onSelectTask(task.task_id)
              }}
            >
              {needsAnswer(task) ? <Reply size={14} /> : <FileText size={14} />}
              {presentation.action}
            </Button>
          ) : (
            <button
              type="button"
              className="mw-link mt-2"
              onClick={() => {
                mark([task])
                setOpen(false)
                onSelectTask(task.task_id)
              }}
            >
              Open task
            </button>
          )}
        </div>
      </article>
    )
  }
  return (
    <Dialog.Root open={open} onOpenChange={setOpen}>
      <Dialog.Trigger
        className="mw-button relative ml-auto lg:ml-0"
        data-variant="outline"
        data-size="icon"
        aria-label={'Notifications, ' + unread.length + ' unread'}
      >
        <Bell size={17} strokeWidth={1.5} aria-hidden="true" />
        {unread.length ? (
          <span className="absolute -top-2 -right-1 rounded bg-white px-1 text-[10px] leading-4 text-warning">
            {unread.length}
          </span>
        ) : null}
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Backdrop className="mw-dialog-backdrop" />
        <Dialog.Popup className="mw-notifications">
          <div className="mw-notification-head">
            <div className="flex items-center justify-between gap-4">
              <Dialog.Title className="text-xl leading-6 font-semibold tracking-[-.01em]">
                Notifications
              </Dialog.Title>
              <Dialog.Close className="mw-link p-1" aria-label="Close notifications">
                <X size={18} />
              </Dialog.Close>
            </div>
            <div className="mt-2 flex items-center justify-between gap-4 text-[13px]">
              <Dialog.Description className="text-muted-foreground">
                {action.length} need action
              </Dialog.Description>
              <button
                className="mw-link underline underline-offset-4"
                type="button"
                onClick={() => mark(items)}
              >
                Mark all as read
              </button>
            </div>
            <div className="mw-notification-tabs" role="group" aria-label="Notification filter">
              <button
                className="mw-notification-tab"
                type="button"
                aria-pressed={!unreadOnly}
                onClick={() => setUnreadOnly(false)}
              >
                All {items.length}
              </button>
              <button
                className="mw-notification-tab"
                type="button"
                aria-pressed={unreadOnly}
                onClick={() => setUnreadOnly(true)}
              >
                Unread {unread.length}
              </button>
            </div>
          </div>
          <div className="mw-notification-list">
            {filtered(action).length ? (
              <section>
                <h2 className="mb-3 text-sm font-semibold">
                  Needs action{' '}
                  <span className="ml-2 font-normal text-muted-foreground">
                    {filtered(action).length}
                  </span>
                </h2>
                {filtered(action).map(renderItem)}
              </section>
            ) : null}
            {filtered(recent).length ? (
              <section className={filtered(action).length ? 'mt-6' : ''}>
                <h2 className="mb-3 text-sm font-semibold">
                  Recent activity{' '}
                  <span className="ml-2 font-normal text-muted-foreground">
                    {filtered(recent).length}
                  </span>
                </h2>
                {filtered(recent).map(renderItem)}
              </section>
            ) : null}
            {(unreadOnly && !unread.length) || !items.length ? (
              <div className="mw-empty min-h-64">
                <CheckCheck size={28} strokeWidth={1.3} className="text-muted-foreground" />
                <h2 className="mw-section-title">
                  {unreadOnly ? 'All caught up' : 'No notifications yet'}
                </h2>
                <p className="text-[13px] text-muted-foreground">
                  {action.length
                    ? 'Your unresolved tasks are still in All.'
                    : 'Task activity will appear here.'}
                </p>
                {action.length ? (
                  <Button variant="outline" onClick={() => setUnreadOnly(false)}>
                    View all notifications
                  </Button>
                ) : null}
              </div>
            ) : null}
          </div>
          <p className="border-t px-6 py-4 text-xs text-muted-foreground">
            Marking as read doesn’t resolve a task.
          </p>
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
