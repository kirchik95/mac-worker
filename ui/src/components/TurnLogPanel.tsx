import { useState } from 'react'

import { CommandList } from '@/components/CommandList'
import { Icon } from '@/components/Icon'
import { useTurnLog } from '@/hooks/useTurnLog'
import { bytes } from '@/lib/format'
import { logKindGlyph, parseLogEvents, type LogEvent, type LogTone } from '@/lib/logEvents'
import { cn } from '@/lib/utils'
import type { LogStream } from '@/lib/api'

const STREAMS: [LogStream, string][] = [
  ['stdout', 'stdout'],
  ['stderr', 'stderr'],
]

const TONE: Record<LogTone, string> = {
  plain: 'text-foreground',
  accent: 'text-observatory-accent-soft',
  good: 'text-observatory-green',
  bad: 'text-destructive',
}

function Line({ event }: { event: LogEvent }) {
  const glyph = logKindGlyph(event.kind)
  return (
    <div className="flex items-start py-1">
      <span className="w-19.5 shrink-0 font-mono text-xs text-observatory-hollow">
        {event.time ?? ''}
      </span>
      <span className={cn('flex w-5.5 shrink-0 pt-0.5', glyph ? TONE[glyph.tone] : '')}>
        {glyph ? <Icon name={glyph.icon} /> : null}
      </span>
      <span
        className={cn(
          'w-16 shrink-0 truncate font-mono text-[11px] tracking-[0.06em] uppercase',
          glyph ? TONE[glyph.tone] : 'text-muted-foreground',
        )}
      >
        {event.kind ?? ''}
      </span>
      <span className="min-w-0 flex-1 font-mono text-xs leading-4.5 break-all whitespace-pre-wrap">
        {event.message}
      </span>
    </div>
  )
}

/**
 * One turn's recorded output. A finished turn is fetched once and never
 * polled — its bytes cannot change — so the panel says plainly that nothing
 * more will arrive.
 */
export function TurnLogPanel({
  taskId,
  turnId,
  turnNumber,
  live,
  truncated,
}: {
  taskId: string
  turnId: string
  turnNumber: number
  live: boolean
  truncated: boolean
}) {
  const [stream, setStream] = useState<LogStream>('stdout')
  const log = useTurnLog(taskId, turnId, stream, live)
  const events = parseLogEvents(log.text)

  return (
    <div>
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 border-b bg-muted/60 py-2.5 pr-5 pl-15">
        <span className="w-19.5 shrink-0 font-mono text-[10px] tracking-[0.06em] text-observatory-hollow">
          TIME
        </span>
        <span className="w-16 shrink-0 font-mono text-[10px] tracking-[0.06em] text-observatory-hollow">
          KIND
        </span>
        <span className="font-mono text-[10px] tracking-[0.06em] text-observatory-hollow">
          MESSAGE
        </span>
        <span className="ml-auto flex items-center gap-3">
          {STREAMS.map(([value, label]) => (
            <button
              key={value}
              type="button"
              onClick={() => setStream(value)}
              className={cn(
                'font-mono text-[10px] tracking-[0.06em] uppercase',
                stream === value ? 'text-foreground' : 'text-observatory-hollow',
              )}
            >
              {label}
            </button>
          ))}
          <span className="text-[11px] text-observatory-hollow">
            {live
              ? 'following · the turn is still running'
              : 'the turn is finished, nothing more will arrive'}
          </span>
        </span>
      </div>

      <div className="py-2.5 pr-5 pl-15">
        {log.error ? (
          <p className="py-2 text-xs text-destructive">Cannot read the log: {log.error}</p>
        ) : events.length === 0 ? (
          <p className="py-2 text-xs text-muted-foreground">
            {live ? 'Waiting for output…' : 'This turn recorded no output on this stream.'}
          </p>
        ) : (
          events.map((event, index) => <Line key={index} event={event} />)
        )}
      </div>

      <div className="flex flex-wrap items-center gap-4 border-t py-3 pr-5 pl-15">
        <div className="min-w-90 flex-1">
          <CommandList commands={[`worker task logs ${taskId} --turn ${turnNumber}`]} />
        </div>
        <span className="flex items-center gap-2 text-[11px] text-observatory-hollow">
          <Icon name="hourglass" size={12} />
          {truncated
            ? 'The host truncated this log; the full text stays on the worker.'
            : `${bytes(log.offset)} read · kept on the worker until the task is closed with --discard`}
        </span>
      </div>
    </div>
  )
}
