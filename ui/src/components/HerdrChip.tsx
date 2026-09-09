import { herdrChip } from '@/lib/herdr'
import { cn } from '@/lib/utils'
import type { DashboardHerdr } from '@/lib/api'

export function HerdrChip({
  herdr,
  className,
}: {
  herdr: DashboardHerdr | null | undefined
  className?: string
}) {
  const available = herdr?.state === 'available'
  const stale = herdr?.stale === true
  return (
    <span
      className={cn(
        'shrink-0 rounded-full border px-2 py-0.5 font-mono text-[10px] tracking-[0.06em]',
        available && !stale ? 'text-muted-foreground' : 'text-observatory-hollow',
        className,
      )}
    >
      {herdrChip(herdr)}
    </span>
  )
}
