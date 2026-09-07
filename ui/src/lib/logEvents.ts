import type { IconName } from '@/components/Icon'

/**
 * The artboard renders a turn's output as timestamp / kind / message rows.
 * Agents emit JSON lines, so a line that parses is shown in that grammar and a
 * line that does not is shown verbatim in the message column. Nothing is
 * dropped: the raw text always reaches the reader one way or the other.
 */
export interface LogEvent {
  time: string | null
  kind: string | null
  message: string
}

const KIND_KEYS = ['type', 'event', 'kind', 'subtype']
const MESSAGE_KEYS = ['message', 'text', 'summary', 'command', 'reason', 'content']

function firstString(record: Record<string, unknown>, keys: string[]): string | null {
  for (const key of keys) {
    const value = record[key]
    if (typeof value === 'string' && value.length > 0) return value
  }
  return null
}

function timeOf(record: Record<string, unknown>): string | null {
  const value = record.timestamp ?? record.time ?? record.at
  if (typeof value === 'number' && Number.isFinite(value)) {
    const millis = value > 1e12 ? value : value * 1000
    const date = new Date(millis)
    return [date.getHours(), date.getMinutes(), date.getSeconds()]
      .map((part) => String(part).padStart(2, '0'))
      .join(':')
  }
  if (typeof value === 'string') {
    const match = /(\d{2}:\d{2}:\d{2})/.exec(value)
    if (match) return match[1]
  }
  return null
}

export function parseLogEvents(text: string): LogEvent[] {
  return text
    .split('\n')
    .filter((line) => line.trim().length > 0)
    .map((line) => {
      if (!line.trimStart().startsWith('{')) {
        return { time: null, kind: null, message: line }
      }
      try {
        const value: unknown = JSON.parse(line)
        if (value == null || typeof value !== 'object') {
          return { time: null, kind: null, message: line }
        }
        const record = value as Record<string, unknown>
        const nested = record.item
        const from =
          nested != null && typeof nested === 'object'
            ? { ...record, ...(nested as Record<string, unknown>) }
            : record
        return {
          time: timeOf(record),
          kind: firstString(from, KIND_KEYS),
          message: firstString(from, MESSAGE_KEYS) ?? line,
        }
      } catch {
        return { time: null, kind: null, message: line }
      }
    })
}

export type LogTone = 'plain' | 'accent' | 'good' | 'bad'

/**
 * Agents name their own event kinds, so the glyph is chosen from what the kind
 * reads like rather than from a closed list. A kind nothing matches keeps the
 * lane and gets no glyph, which is truer than guessing.
 */
export function logKindGlyph(kind: string | null): { icon: IconName; tone: LogTone } | null {
  if (!kind) return null
  const key = kind.toLowerCase()
  if (/(fail|error|denied|refus)/.test(key)) return { icon: 'xCircle', tone: 'bad' }
  if (/(pass|success|ok\b|clean)/.test(key)) return { icon: 'check', tone: 'good' }
  if (/(result|complete|done|final)/.test(key)) return { icon: 'flag', tone: 'plain' }
  if (/(exec|command|shell|bash|tool)/.test(key)) return { icon: 'terminal', tone: 'accent' }
  if (/(patch|edit|write|apply|diff)/.test(key)) return { icon: 'pencil', tone: 'accent' }
  if (/(reason|think|plan|message|assistant)/.test(key)) return { icon: 'spark', tone: 'plain' }
  return null
}
