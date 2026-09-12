import { Icon, type IconName } from '@/components/Icon'
import { cn } from '@/lib/utils'

/**
 * The artboards mark a state or an outcome with a line glyph and an uppercase
 * monospace label. One treatment carries both columns, so a row reads
 * "closed · done" in the same grammar wherever it appears.
 */
const MARKS: Record<string, { icon: IconName; tone: string }> = {
  // States.
  queued: { icon: 'clock', tone: 'text-muted-foreground' },
  active: { icon: 'cpuBusy', tone: 'text-primary' },
  open: { icon: 'clockOpen', tone: 'text-muted-foreground' },
  closed: { icon: 'archive', tone: 'text-observatory-hollow' },
  abandoned: { icon: 'xCircle', tone: 'text-observatory-hollow' },
  // Origin delivery.
  pending: { icon: 'clock', tone: 'text-muted-foreground' },
  retrying: { icon: 'cpuBusy', tone: 'text-primary' },
  delivered: { icon: 'check', tone: 'text-observatory-green' },
  // Process terminal states.
  running: { icon: 'cpuBusy', tone: 'text-primary' },
  succeeded: { icon: 'check', tone: 'text-observatory-green' },
  signalled: { icon: 'xCircle', tone: 'text-destructive' },
  // Outcomes.
  done: { icon: 'check', tone: 'text-observatory-green' },
  needs_input: { icon: 'question', tone: 'text-primary' },
  blocked: { icon: 'xCircle', tone: 'text-destructive' },
  failed: { icon: 'xCircle', tone: 'text-destructive' },
  timed_out: { icon: 'hourglass', tone: 'text-destructive' },
  cancelled: { icon: 'xCircle', tone: 'text-observatory-hollow' },
  lost: { icon: 'linkOff', tone: 'text-destructive' },
  unknown: { icon: 'ellipsis', tone: 'text-observatory-hollow' },
}

export function StatusMark({
  value,
  size = 13,
  className,
}: {
  value: string | null | undefined
  size?: number
  className?: string
}) {
  const key = (value ?? 'unknown').toLowerCase()
  const mark = MARKS[key] ?? MARKS.unknown
  return (
    <span className={cn('inline-flex items-center gap-[7px] whitespace-nowrap', mark.tone, className)}>
      <Icon name={mark.icon} size={size} />
      <span className="font-mono text-[10px] tracking-[0.06em] uppercase">
        {(value ?? 'unknown').replace(/_/g, ' ')}
      </span>
    </span>
  )
}
