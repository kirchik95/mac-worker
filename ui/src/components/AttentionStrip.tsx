import { Icon } from '@/components/Icon'
import { attentionCount } from '@/lib/attention'
import type { Snapshot } from '@/lib/api'

/** The one line that says whether anything is waiting on the operator. */
export function AttentionStrip({
  snapshot,
  onShowAttention,
}: {
  snapshot: Snapshot
  onShowAttention?: () => void
}) {
  const attention = attentionCount(snapshot)

  return (
    <div className="flex items-center gap-3 rounded-b-[10px] border border-t-0 bg-muted/40 px-5 py-4">
      <Icon name="alert" size={16} className={attention > 0 ? 'text-primary' : ''} />
      <span className="flex-1 text-sm">
        {attention === 0
          ? 'No task needs attention'
          : `${attention} ${attention === 1 ? 'task needs' : 'tasks need'} attention`}
      </span>
      {attention > 0 && onShowAttention ? (
        <button
          type="button"
          onClick={onShowAttention}
          className="flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
        >
          View
          <Icon name="chevronRight" size={12} />
        </button>
      ) : null}
    </div>
  )
}
