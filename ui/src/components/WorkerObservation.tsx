import { useEffect, useId, useRef, useState } from 'react'
import { RefreshCw } from 'lucide-react'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { relativeTime } from '@/lib/format'

/** A report indicator, not a refresh action. A newer current report gets one brief turn. */
export function WorkerObservation({
  workerName,
  observedAt,
  current,
  now,
}: {
  workerName: string
  observedAt: number | null
  current: boolean
  now: number
}) {
  const [open, setOpen] = useState(false)
  const triggerId = useId()
  const icon = useRef<HTMLSpanElement>(null)
  const lastReport = useRef({ workerName, observedAt })
  const label = observedAt == null
    ? 'Worker has not reported yet'
    : (current ? 'Worker checked ' : 'Last report ') + relativeTime(observedAt, now)

  useEffect(() => {
    const previous = lastReport.current
    if (previous.workerName !== workerName) {
      lastReport.current = { workerName, observedAt }
      return
    }
    if (observedAt == null || !Number.isFinite(observedAt)) return
    // Retain the high-water mark so an out-of-order response cannot repeat the animation.
    lastReport.current = { workerName, observedAt: Math.max(previous.observedAt ?? 0, observedAt) }
    if (!current || observedAt <= (previous.observedAt ?? 0)) return
    const element = icon.current
    if (
      !element?.animate ||
      window.matchMedia?.('(prefers-reduced-motion: reduce)').matches ||
      document.documentElement.dataset.inputModality === 'keyboard'
    ) return
    const style = getComputedStyle(element)
    const animation = element.animate(
      [{ transform: 'rotate(0deg)' }, { transform: 'rotate(180deg)' }],
      {
        duration: Number.parseFloat(style.getPropertyValue('--motion-surface')) || 180,
        easing: style.getPropertyValue('--ease-out').trim() || 'cubic-bezier(0.23, 1, 0.32, 1)',
      },
    )
    return () => animation.cancel()
  }, [workerName, observedAt, current])

  return (
    <Tooltip open={open} onOpenChange={setOpen} triggerId={triggerId}>
      <TooltipTrigger
        id={triggerId}
        render={<span role="img" tabIndex={0} />}
        className="mw-worker-observation"
        aria-label={label}
        aria-describedby={open ? triggerId + '-description' : undefined}
        closeOnClick={false}
        onPointerUp={(event) => {
          if (event.pointerType !== 'mouse') {
            event.currentTarget.focus()
            setOpen(true)
          }
        }}
      >
        <span ref={icon} className="mw-worker-observation-icon" aria-hidden="true">
          <RefreshCw size={14} />
        </span>
      </TooltipTrigger>
      <TooltipContent id={triggerId + '-description'} role="tooltip" className="mw-worker-tooltip">
        {label}
      </TooltipContent>
    </Tooltip>
  )
}
