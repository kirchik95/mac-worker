import { StatusMark } from '@/components/StatusMark'
import { relativeTime, shortId } from '@/lib/format'
import { lastDelivery, type OriginDelivery } from '@/lib/api'
import { cn } from '@/lib/utils'

export function deliveryLabel(delivery: OriginDelivery, freshness?: string | null): string {
  const turn = shortId(delivery.turn_id, 8)
  const stale =
    freshness === 'stale' ? ` · observed ${relativeTime(delivery.updated_at_millis)}` : ''
  return `${delivery.state} · ${turn}${stale}`
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
  const delivery = lastDelivery(task)
  if (!delivery) return null
  return (
    <span
      className={cn('inline-flex items-center gap-1.5', className)}
      title={delivery.last_error ?? delivery.state}
    >
      <StatusMark value={delivery.state} size={12} />
      {freshness === 'stale' ? (
        <span className="font-mono text-[10px] tracking-[0.06em] text-observatory-hollow uppercase">
          stale
        </span>
      ) : null}
    </span>
  )
}
