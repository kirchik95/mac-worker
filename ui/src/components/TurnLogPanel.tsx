import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import { ArrowDown } from 'lucide-react'

import { CommandList } from '@/components/CommandList'
import { Icon } from '@/components/Icon'
import { useTurnLog } from '@/hooks/useTurnLog'
import { bytes } from '@/lib/format'
import { logKindGlyph, parseLogEvents, type LogEvent, type LogTone } from '@/lib/logEvents'
import { cn } from '@/lib/utils'
import type { LogStream } from '@/lib/api'
import './turn-log.css'

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
    <div className="mw-log-row">
      <span className="font-mono text-xs text-observatory-hollow">{event.time ?? ''}</span>
      <span className={cn('flex pt-0.5', glyph ? TONE[glyph.tone] : '')}>
        {glyph ? <Icon name={glyph.icon} /> : null}
      </span>
      <span className={cn('truncate text-xs', glyph ? TONE[glyph.tone] : 'text-muted-foreground')}>
        {event.kind ?? ''}
      </span>
      <span className="min-w-0 flex-1 font-mono text-xs leading-4.5 break-all whitespace-pre-wrap">
        {event.message}
      </span>
    </div>
  )
}

/**
 * One turn's recorded output. A finished turn is drained sequentially until a
 * read makes no progress, which establishes only the host's current end.
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
  const viewport = useRef<HTMLDivElement>(null)
  const following = useRef(true)
  const [isFollowing, setFollowing] = useState(true)
  const identity = `${taskId}:${turnId}:${stream}`
  const previousIdentity = useRef(identity)
  const jumpToLatest = () => {
    following.current = true
    setFollowing(true)
    const node = viewport.current
    if (node) {
      node.scrollTop = Math.max(0, node.scrollHeight - node.clientHeight)
      node.focus({ preventScroll: true })
    }
  }
  useLayoutEffect(() => {
    if (previousIdentity.current !== identity) {
      previousIdentity.current = identity
      following.current = true
      setFollowing(true)
    }
    const node = viewport.current
    if (node && following.current)
      node.scrollTop = Math.max(0, node.scrollHeight - node.clientHeight)
  }, [identity, log.text])
  useEffect(() => {
    const node = viewport.current
    if (!node || typeof ResizeObserver === 'undefined') return
    const observer = new ResizeObserver(() => {
      if (following.current) node.scrollTop = Math.max(0, node.scrollHeight - node.clientHeight)
    })
    observer.observe(node)
    return () => observer.disconnect()
  }, [])

  return (
    <div>
      <div className="flex flex-wrap items-center gap-3 border-b px-6 py-3">
        <h3 className="text-[13px] font-semibold">Latest output</h3>
        <span className="ml-auto flex items-center gap-3">
          {STREAMS.map(([value, label]) => (
            <button
              key={value}
              type="button"
              aria-pressed={stream === value}
              onClick={() => setStream(value)}
              className={cn(
                'font-mono text-xs',
                stream === value ? 'text-foreground' : 'text-observatory-hollow',
              )}
            >
              {label}
            </button>
          ))}
          <span className="text-[11px] text-observatory-hollow">
            {!isFollowing
              ? 'Reading earlier output'
              : live
                ? 'following · the turn is still running'
                : 'the turn is finished · read to the current end'}
          </span>
        </span>
      </div>
      <div className="mw-log-row mw-log-columns" aria-hidden="true">
        <span>Time</span>
        <span />
        <span>Kind</span>
        <span>Output</span>
      </div>
      <div
        className="mw-log-viewport"
        ref={viewport}
        role="log"
        tabIndex={0}
        aria-live="off"
        aria-label={`Turn ${turnNumber} ${stream} output`}
        onScroll={(event) => {
          const node = event.currentTarget
          const atEnd = node.scrollHeight - node.clientHeight - node.scrollTop <= 2
          following.current = atEnd
          setFollowing(atEnd)
        }}
      >
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
      {!isFollowing ? (
        <div className="mw-log-jump">
          <button
            type="button"
            className="mw-button"
            data-variant="outline"
            data-size="sm"
            onClick={jumpToLatest}
          >
            <ArrowDown size={14} aria-hidden="true" />
            Jump to latest
          </button>
        </div>
      ) : null}

      <div className="flex flex-wrap items-center gap-4 border-t py-3 pr-6 pl-6">
        <div className="min-w-0 w-full">
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
