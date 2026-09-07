import { useEffect, useRef, useState } from 'react'

import { Icon } from '@/components/Icon'

const COPIED_MS = 2000

/**
 * The commands the artboard puts under "take it locally". The dashboard is
 * read only, so the affordance is the command itself: copying it is the whole
 * action, and the confirmation stays until it lapses.
 */
export function CommandList({ commands }: { commands: string[] }) {
  const [copied, setCopied] = useState<string | null>(null)
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => () => { if (timer.current) clearTimeout(timer.current) }, [])

  const copy = async (command: string) => {
    try {
      await navigator.clipboard?.writeText(command)
    } catch {
      // A denied clipboard is not an error worth a banner: the command is on
      // screen and can be selected by hand.
      return
    }
    setCopied(command)
    if (timer.current) clearTimeout(timer.current)
    timer.current = setTimeout(() => setCopied(null), COPIED_MS)
  }

  return (
    <div className="divide-y overflow-hidden rounded-md border bg-card">
      {commands.map((command) => (
        <div key={command} className="flex items-stretch">
          <div className="flex min-w-0 flex-1 items-center gap-3 px-4.5 py-3">
            <span className="font-mono text-[13px] text-observatory-hollow">$</span>
            <code className="truncate font-mono text-[13px]">{command}</code>
          </div>
          <button
            type="button"
            onClick={() => void copy(command)}
            aria-label={`Copy ${command}`}
            className={`flex shrink-0 items-center gap-2 border-l px-5 font-mono text-[11px] tracking-[0.08em] ${
              copied === command
                ? 'bg-observatory-highlight text-primary'
                : 'text-muted-foreground hover:text-foreground'
            }`}
          >
            <Icon name={copied === command ? 'check' : 'copy'} />
            {copied === command ? 'COPIED' : 'COPY'}
          </button>
        </div>
      ))}
    </div>
  )
}
