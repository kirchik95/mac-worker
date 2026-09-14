import { useEffect, useRef, useState } from 'react'

import { Icon } from '@/components/Icon'

const COPIED_MS = 2000

/** Copies a command without executing it. */
export function CommandList({ commands }: { commands: string[] }) {
  const [copied, setCopied] = useState<string | null>(null)
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(
    () => () => {
      if (timer.current) clearTimeout(timer.current)
    },
    [],
  )

  const copy = async (command: string) => {
    try {
      if (!navigator.clipboard) return
      await navigator.clipboard.writeText(command)
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
            <code className="overflow-x-auto whitespace-nowrap font-mono text-xs">{command}</code>
          </div>
          <button
            type="button"
            onClick={() => void copy(command)}
            aria-label={`Copy ${command}`}
            className={`flex shrink-0 items-center gap-2 border-l px-4 text-xs ${
              copied === command
                ? 'bg-observatory-highlight text-primary'
                : 'text-muted-foreground hover:text-foreground'
            }`}
          >
            <Icon name={copied === command ? 'check' : 'copy'} />
            {copied === command ? 'Copied' : 'Copy'}
          </button>
        </div>
      ))}
    </div>
  )
}
