import { cn } from '@/lib/utils'

/**
 * The artboards mark state with a coloured dot and an uppercase monospace
 * label rather than a filled pill, so one treatment carries every status.
 */
const DOTS: Record<string, string> = {
  authenticated: 'bg-observatory-green',
  closed: 'bg-observatory-green',
  connected: 'bg-observatory-green',
  done: 'bg-observatory-green',
  ready: 'bg-observatory-green',
  succeeded: 'bg-observatory-green',
  active: 'bg-primary',
  busy: 'bg-primary',
  current: 'bg-primary',
  needs_input: 'bg-primary',
  open: 'bg-primary',
  queued: 'bg-primary',
  running: 'bg-primary',
  stale: 'bg-primary',
  blocked: 'bg-destructive',
  cancelled: 'bg-destructive',
  failed: 'bg-destructive',
  lost: 'bg-destructive',
  timed_out: 'bg-destructive',
  unavailable: 'bg-destructive',
  abandoned: 'bg-muted-foreground',
  unknown: 'bg-muted-foreground',
}

export function StatusBadge({
  value,
  className,
}: {
  value: string | null | undefined
  className?: string
}) {
  const key = (value ?? 'unknown').toLowerCase()
  return (
    <span className={cn('inline-flex items-center gap-[7px] whitespace-nowrap', className)}>
      <span className={cn('size-[5px] shrink-0 rounded-full', DOTS[key] ?? DOTS.unknown)} aria-hidden="true" />
      <span className="font-mono text-[10px] tracking-[0.06em] text-muted-foreground uppercase">
        {(value ?? 'unknown').replace(/_/g, ' ')}
      </span>
    </span>
  )
}
