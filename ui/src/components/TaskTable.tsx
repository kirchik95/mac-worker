import { FileText, Reply, Settings2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { TaskBadge } from '@/components/TaskBadge'
import { AgentMark } from '@/components/AgentMark'
import { DeliveryChip } from '@/components/DeliveryChip'
import type { TaskRow } from '@/lib/api'
import { relativeTime } from '@/lib/format'
import { taskPresentation, setupAgent, waitingReason } from '@/lib/taskPresentation'

export function TaskTable({
  tasks,
  onSelect,
  onSetup,
  now = Date.now(),
  compact = false,
  onShowRun,
}: {
  tasks: TaskRow[]
  onSelect?: (id: string) => void
  onSetup?: (worker?: string, agent?: string) => void
  now?: number
  compact?: boolean
  onShowRun?: (id: string) => void
}) {
  return (
    <div className="mw-table-wrap">
      <table className="mw-table">
        <colgroup>
          <col style={{ width: compact ? '150px' : '196px' }} />
          <col />
          <col style={{ width: compact ? '280px' : '166px' }} />
          <col style={{ width: '108px' }} />
          <col style={{ width: '110px' }} />
          <col style={{ width: '192px' }} />
        </colgroup>
        <thead>
          <tr>
            <th>Status</th>
            <th>Task</th>
            <th>{compact ? 'Progress / waiting reason' : 'Agent'}</th>
            <th>Mac</th>
            <th>{compact ? 'Time in status' : 'Updated'}</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
          {tasks.map((task) => {
            const presentation = taskPresentation(task)
            const setup = setupAgent(task) && onSetup
            const run =
              compact && task.blocking_code === 'RUN_MAX_PARALLEL' && task.run_id && onShowRun
            const action = setup ? 'Setup instructions' : run ? 'Open run' : presentation.action
            const Icon = setup ? Settings2 : action === 'Answer' ? Reply : FileText
            const open = () =>
              setup
                ? onSetup?.(task.worker ?? undefined, setupAgent(task) ?? task.agent)
                : run
                  ? onShowRun?.(task.run_id!)
                  : onSelect?.(task.task_id)
            return (
              <tr key={task.task_id}>
                <td>
                  <TaskBadge task={task} />
                  <DeliveryChip task={task} freshness={task.freshness} />
                </td>
                <td>
                  <button
                    type="button"
                    className="mw-task-title"
                    onClick={() => onSelect?.(task.task_id)}
                  >
                    {task.title}
                  </button>
                  {!compact ? (
                    <>
                      <span className="mw-task-id" title={task.task_id}>
                        {task.task_id}
                      </span>
                      {task.blocking_code ? (
                        <span className="text-xs">{waitingReason(task)}</span>
                      ) : null}
                    </>
                  ) : null}
                </td>
                <td>{compact ? waitingReason(task) : <AgentMark agent={task.agent} label />}</td>
                <td>{task.worker ?? '—'}</td>
                <td>{relativeTime(task.updated_at_millis, now)}</td>
                <td>
                  <Button variant={action === 'Answer' ? 'default' : 'outline'} onClick={open}>
                    <Icon size={16} strokeWidth={1.5} aria-hidden="true" />
                    {action}
                  </Button>
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
