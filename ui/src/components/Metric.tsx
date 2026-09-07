import { cn } from '@/lib/utils'

/** Label-over-value pairs, the unit the artboards repeat everywhere. */
export function Metric({
  label,
  value,
  size = 'small',
  tone,
  className,
}: {
  label: string
  value: string
  size?: 'display' | 'small'
  tone?: 'accent' | 'muted'
  className?: string
}) {
  return (
    <div className={cn('flex min-w-0 flex-col gap-2', className)}>
      <span className="font-mono text-[11px] tracking-[0.06em] text-muted-foreground">{label}</span>
      <span
        className={cn(
          'truncate',
          size === 'display'
            ? 'font-mono text-[27px] leading-none tracking-[-0.04em]'
            : 'text-[18px] leading-none',
          tone === 'accent' && 'text-primary',
          tone === 'muted' && 'text-muted-foreground',
        )}
      >
        {value}
      </span>
    </div>
  )
}
