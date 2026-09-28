import { useId, useState } from 'react'
import { Clock3, CloudUpload, RefreshCw, TriangleAlert } from 'lucide-react'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { relativeTime } from '@/lib/format'
import { lastDelivery, type OriginDelivery } from '@/lib/api'
import { cn } from '@/lib/utils'

const PRESENTATION = {
  pending: {
    label: 'Push pending',
    icon: Clock3,
    tone: 'neutral',
    description: 'The result is waiting to be pushed to origin.',
  },
  retrying: {
    label: 'Retrying push',
    icon: RefreshCw,
    tone: 'warning',
    description: 'The push to origin failed. Another attempt is scheduled.',
  },
  delivered: {
    label: 'Pushed to origin',
    icon: CloudUpload,
    tone: 'neutral',
    description: 'The result is available on origin. Review and merging are separate steps.',
  },
  failed: {
    label: 'Push failed',
    icon: TriangleAlert,
    tone: 'error',
    description: 'The result could not be pushed to origin. Check the error and repository access.',
  },
} satisfies Record<OriginDelivery['state'], unknown>

export function deliveryLabel(delivery: OriginDelivery, freshness?: string | null): string {
  return (freshness === 'stale' ? 'Last known: ' : '') + PRESENTATION[delivery.state].label
}

export function DeliveryChip({
  task,
  freshness,
  className,
}: {
  task: { delivery?: OriginDelivery | null; deliveries?: OriginDelivery[] }
  freshness?: string | null
  className?: string
}) {
  const [open, setOpen] = useState(false)
  const triggerId = useId()
  const delivery = lastDelivery(task)
  if (!delivery) return null
  const state = PRESENTATION[delivery.state]
  const Icon = state.icon
  return (
    <Tooltip open={open} onOpenChange={setOpen} triggerId={triggerId}>
      <TooltipTrigger
        id={triggerId}
        render={<span tabIndex={0} />}
        className={cn('mw-delivery', className)}
        data-tone={state.tone}
        aria-describedby={open ? triggerId + '-description' : undefined}
        delay={300}
        closeOnClick={false}
        onPointerUp={(event) => {
          if (event.pointerType !== 'mouse') {
            event.currentTarget.focus()
            setOpen(true)
          }
        }}
      >
        <Icon size={14} strokeWidth={1.5} aria-hidden="true" />
        <span>{deliveryLabel(delivery, freshness)}</span>
      </TooltipTrigger>
      <TooltipContent id={triggerId + '-description'} role="tooltip" className="mw-delivery-tooltip">
        <span>
          {delivery.state === 'delivered' && delivery.superseded_by
            ? 'A newer push already includes this result on origin.'
            : state.description}
        </span>
        <span>Branch: <code>{delivery.target.replace(/^refs\/heads\//, '')}</code></span>
        {delivery.last_error ? <code>{delivery.last_error}</code> : null}
        {freshness === 'stale' ? (
          <span>Last checked {relativeTime(delivery.updated_at_millis)}. Current push status is unverified.</span>
        ) : null}
      </TooltipContent>
    </Tooltip>
  )
}
