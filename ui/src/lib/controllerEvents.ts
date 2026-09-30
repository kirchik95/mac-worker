/**
 * Browser stream client. Wire types and fixtures come from the frozen T1 contract.
 * Decimal sequences stay strings; never coerce them with Number.
 * Cadences match `events::contracts` (anti-entropy 15s, fallback 2s, debounce 100ms,
 * watchdog 30s, reconnect 1/2/4/5s). An event's JSON plus its JSONL newline must
 * fit in 1024 bytes.
 */

import type { EventCursor, JournalWindow } from './controllerEvents.contract'

export const FALLBACK_POLL_MS = 2_000
export const HEALTHY_ANTI_ENTROPY_MS = 15_000
export const INVALIDATION_DEBOUNCE_MS = 100
export const WATCHDOG_MS = 30_000
export const RECONNECT_BACKOFF_MS = [1_000, 2_000, 4_000, 5_000] as const
export const MAX_EVENT_BYTES = 1_024
export const EVENTS_PATH = '/api/v1/events'

const MAX_U64 = 18446744073709551615n
const MAX_U32 = 4_294_967_295
const NIL_JOURNAL = '00000000-0000-0000-0000-000000000000'
const JOURNAL_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
const SEQ = /^(0|[1-9][0-9]{0,19})$/
const KIND = /^[a-z0-9._]{1,64}$/
const REASON = /^[a-z0-9_]{1,64}$/
const SIMPLE_ID = /^[0-9a-f]{32}$/
const SSE_NAMES = ['controller.event', 'snapshot_required', 'ready', 'snapshot.ready', 'heartbeat']
const UNAVAILABLE_CODES = new Set([
  'CONTROLLER_EVENTS_UNAVAILABLE',
  'CONTROLLER_EVENTS_CANCELLED',
  'CONTROLLER_EVENTS_REPAIR_REGISTRY_TOO_LARGE',
])
const TASK_STATES = new Set(['queued', 'active', 'open', 'closed', 'abandoned', 'lost'])
const QUEUE_STATES = new Set(['waiting', 'dispatching', 'parked'])
const QUEUE_KINDS = new Set(['batch', 'task_turn'])
const OUTCOMES = new Set([
  'done',
  'needs_input',
  'blocked',
  'unknown',
  'failed',
  'cancelled',
  'timed_out',
  'lost',
])

export type EventClientOptions = {
  makeSource: (url: string) => EventSource
  now: () => number
  setTimer: (fn: () => void, delay: number) => number
  clearTimer: (id: number) => void
  invalidate: (taskIds: string[] | null) => void
  health: (healthy: boolean) => void
}

export type EventClient = { start: () => void; stop: () => void }

type Frame =
  | { type: 'malformed' }
  | { type: 'heartbeat' }
  | { type: 'ready' }
  | { type: 'unavailable' }
  | { type: 'repair' }
  | { type: 'snapshot_ready' }
  | { type: 'event'; cursor: EventCursor; taskIds: string[] | null }

type Pending = {
  event: { global: boolean; tasks: Set<string> } | null
  publication: boolean
}

export function controllerEventsUrl(cursor: EventCursor | null): string {
  if (!cursor) return EVENTS_PATH
  // journal_id and seq are validated tokens, so the cursor matches the SSE id literally.
  return `${EVENTS_PATH}?after=${cursor.journal_id}:${cursor.seq}`
}

export function createControllerEvents(options: EventClientOptions): EventClient {
  let cursor: EventCursor | null = null
  let source: EventSource | null = null
  let generation = 0
  let started = false
  let stopped = false
  let healthy = false
  let failing = false
  let reconnectAttempt = 0
  let watchdog: number | null = null
  let debounce: number | null = null
  let reconnect: number | null = null
  let pending: Pending | null = null

  const setHealthy = (next: boolean) => {
    if (healthy === next) return
    healthy = next
    options.health(next)
  }

  const clearTimer = (id: number | null) => {
    if (id !== null) options.clearTimer(id)
  }

  const clearWatchdog = () => {
    clearTimer(watchdog)
    watchdog = null
  }

  const clearDebounce = () => {
    clearTimer(debounce)
    debounce = null
    pending = null
  }

  const clearReconnect = () => {
    clearTimer(reconnect)
    reconnect = null
  }

  const armWatchdog = () => {
    clearWatchdog()
    const token = generation
    watchdog = options.setTimer(() => {
      watchdog = null
      if (stopped || token !== generation) return
      fail(true)
    }, WATCHDOG_MS)
  }

  const closeSource = () => {
    generation += 1
    const current = source
    source = null
    current?.close()
  }

  const connect = () => {
    if (stopped) return
    const token = ++generation
    let next: EventSource
    try {
      next = options.makeSource(controllerEventsUrl(cursor))
    } catch {
      replaceSource(false)
      return
    }
    if (token !== generation || stopped) {
      next.close()
      return
    }
    source = next
    const live = () => token === generation && !stopped
    for (const name of SSE_NAMES) {
      next.addEventListener(name, (event) => {
        if (!live()) return
        handle(name, event as MessageEvent)
      })
    }
    next.addEventListener('error', () => {
      if (!live()) return
      fail(false)
    })
    armWatchdog()
  }

  const replaceSource = (immediate: boolean) => {
    closeSource()
    clearWatchdog()
    clearReconnect()
    if (stopped) return
    if (immediate) {
      connect()
      return
    }
    const delay = RECONNECT_BACKOFF_MS[Math.min(reconnectAttempt, RECONNECT_BACKOFF_MS.length - 1)]
    reconnectAttempt += 1
    reconnect = options.setTimer(() => {
      reconnect = null
      if (!stopped) connect()
    }, delay)
  }

  const fail = (immediate: boolean) => {
    if (failing || stopped) return
    failing = true
    try {
      setHealthy(false)
      clearDebounce()
      options.invalidate(null)
      replaceSource(immediate)
    } finally {
      failing = false
    }
  }

  const noteActivity = () => {
    reconnectAttempt = 0
    setHealthy(true)
    armWatchdog()
  }

  const armDebounce = () => {
    if (debounce !== null) return
    debounce = options.setTimer(() => {
      debounce = null
      flush()
    }, INVALIDATION_DEBOUNCE_MS)
  }

  const ensurePending = (): Pending => {
    if (!pending) pending = { event: null, publication: false }
    return pending
  }

  const queueEvent = (taskIds: string[] | null) => {
    const batch = ensurePending()
    if (!batch.event) batch.event = { global: false, tasks: new Set() }
    if (taskIds === null) batch.event.global = true
    else for (const id of taskIds) batch.event.tasks.add(id)
    armDebounce()
  }

  const queuePublication = () => {
    ensurePending().publication = true
    armDebounce()
  }

  const flush = () => {
    const batch = pending
    pending = null
    if (!batch || stopped) return
    if (batch.event) {
      options.invalidate(batch.event.global ? null : [...batch.event.tasks].sort())
    }
    if (batch.publication) options.invalidate(null)
  }

  const handle = (name: string, event: MessageEvent) => {
    const raw = typeof event.data === 'string' ? event.data : ''
    if (byteLength(raw) + 1 > MAX_EVENT_BYTES) {
      fail(false)
      return
    }
    const frame = interpretFrame(name, raw, event.lastEventId)
    if (frame.type === 'malformed' || frame.type === 'unavailable') {
      fail(false)
      return
    }
    if (frame.type === 'repair') {
      // The window is a repair baseline, not an event cursor. Drop it so a
      // close after lag repair reconnects without the stale after query.
      // Bootstrap, expiry, and epoch controls stay on this stream until ready.
      cursor = null
      setHealthy(false)
      clearDebounce()
      options.invalidate(null)
      return
    }
    if (frame.type === 'event') cursor = frame.cursor
    noteActivity()
    if (frame.type === 'event') queueEvent(frame.taskIds)
    else if (frame.type === 'ready') queueEvent(null)
    else if (frame.type === 'snapshot_ready') queuePublication()
  }

  return {
    start() {
      if (started) return
      started = true
      stopped = false
      healthy = false
      options.health(false)
      connect()
    },
    stop() {
      stopped = true
      started = false
      clearDebounce()
      clearWatchdog()
      clearReconnect()
      closeSource()
    },
  }
}

function interpretFrame(name: string, raw: string, lastEventId: string): Frame {
  const parsed = parseJson(raw)
  if (name === 'heartbeat') {
    return isRecord(parsed) && Object.keys(parsed).length === 0
      ? { type: 'heartbeat' }
      : { type: 'malformed' }
  }
  if (!isRecord(parsed)) return { type: 'malformed' }
  if (name === 'snapshot.ready') {
    return Number.isSafeInteger(parsed.revision) && (parsed.revision as number) >= 0
      ? { type: 'snapshot_ready' }
      : { type: 'malformed' }
  }
  if (name === 'ready') {
    return parseWindow(parsed) ? { type: 'ready' } : { type: 'malformed' }
  }
  if (name === 'snapshot_required') return interpretRepair(parsed)
  if (name !== 'controller.event') return { type: 'malformed' }
  return interpretEvent(parsed, lastEventId)
}

function interpretRepair(parsed: Record<string, unknown>): Frame {
  if (parsed.reason === 'unavailable' && parsed.window === null) {
    return typeof parsed.code === 'string' && UNAVAILABLE_CODES.has(parsed.code)
      ? { type: 'unavailable' }
      : { type: 'malformed' }
  }
  if (typeof parsed.reason !== 'string' || !REASON.test(parsed.reason)) return { type: 'malformed' }
  return parseWindow(parsed.window) ? { type: 'repair' } : { type: 'malformed' }
}

function interpretEvent(parsed: Record<string, unknown>, lastEventId: string): Frame {
  const schema = schemaClass(parsed.schema_version)
  if (schema === 'bad') return { type: 'malformed' }
  if (!isJournalId(parsed.journal_id) || !isPositiveSeq(parsed.seq)) return { type: 'malformed' }
  if (!isTime(parsed.time_millis)) return { type: 'malformed' }
  if (typeof parsed.kind !== 'string' || !KIND.test(parsed.kind)) return { type: 'malformed' }
  if (!isRecord(parsed.data)) return { type: 'malformed' }
  const cursor: EventCursor = { journal_id: parsed.journal_id, seq: parsed.seq }
  if (lastEventId !== '') {
    const advertised = parseCursorId(lastEventId)
    if (!advertised || advertised.journal_id !== cursor.journal_id || advertised.seq !== cursor.seq) {
      return { type: 'malformed' }
    }
  }
  if (schema === 'future') return { type: 'event', cursor, taskIds: null }
  const taskIds = targetsForKind(parsed.kind, parsed.data)
  if (taskIds === 'bad') return { type: 'malformed' }
  return { type: 'event', cursor, taskIds }
}

function targetsForKind(kind: string, data: Record<string, unknown>): string[] | null | 'bad' {
  switch (kind) {
    case 'task.created':
    case 'task.changed':
    case 'task.removed':
    case 'task.closed':
    case 'task.abandoned':
      return taskHint(data)
    case 'turn.started':
      return acceptedHint(data)
    case 'turn.finished':
    case 'turn.outcome_changed':
      return turnHint(data)
    case 'task.auto_continue_scheduled':
      return continuationHint(data)
    case 'queue.changed':
      return queueHint(data)
    case 'run.changed':
      return runHint(data)
    case 'dag.child_admitted':
      return dagHint(data)
    case 'worker.changed':
      return workerHint(data)
    case 'controller.drained':
      return drainHint(data, true)
    case 'controller.undrained':
      return drainHint(data, false)
    default:
      return null
  }
}

function taskHint(data: Record<string, unknown>): string[] | 'bad' {
  if (!isSimpleId(data.task_id) || !isOptionalId(data.run_id) || !isOptionalId(data.turn_id)) {
    return 'bad'
  }
  if (typeof data.state !== 'string' || !TASK_STATES.has(data.state) || !isOptionalCode(data.code)) {
    return 'bad'
  }
  return [data.task_id]
}

function acceptedHint(data: Record<string, unknown>): string[] | 'bad' {
  if (!isSimpleId(data.task_id) || !isSimpleId(data.turn_id) || !isOptionalId(data.run_id)) return 'bad'
  if (!isWorkerName(data.worker)) return 'bad'
  return [data.task_id]
}

function turnHint(data: Record<string, unknown>): string[] | 'bad' {
  if (!isSimpleId(data.task_id) || !isSimpleId(data.turn_id) || !isOptionalId(data.run_id)) return 'bad'
  if (typeof data.outcome !== 'string' || !OUTCOMES.has(data.outcome) || !isOptionalCode(data.code)) {
    return 'bad'
  }
  return [data.task_id]
}

function continuationHint(data: Record<string, unknown>): string[] | 'bad' {
  if (!isSimpleId(data.task_id) || !isOptionalId(data.run_id)) return 'bad'
  if (!isSimpleId(data.previous_turn_id) || !isSimpleId(data.next_turn_id)) return 'bad'
  return [data.task_id]
}

function queueHint(data: Record<string, unknown>): string[] | 'bad' {
  if (!isOptionalId(data.turn_id) || !isOptionalCode(data.code)) return 'bad'
  if (data.state != null && (typeof data.state !== 'string' || !QUEUE_STATES.has(data.state))) return 'bad'
  if (data.kind != null && (typeof data.kind !== 'string' || !QUEUE_KINDS.has(data.kind))) return 'bad'
  return []
}

function runHint(data: Record<string, unknown>): string[] | 'bad' {
  if (!isSimpleId(data.run_id)) return 'bad'
  if (data.task_id == null) return []
  return isSimpleId(data.task_id) ? [data.task_id] : 'bad'
}

function dagHint(data: Record<string, unknown>): string[] | 'bad' {
  if (!isSimpleId(data.run_id) || !isSimpleId(data.task_id) || !isSimpleId(data.turn_id)) return 'bad'
  return [data.task_id]
}

function workerHint(data: Record<string, unknown>): string[] | 'bad' {
  if (!isWorkerName(data.worker) || !isTime(data.observed_at_millis) || !isOptionalCode(data.code)) {
    return 'bad'
  }
  if (data.ready != null && typeof data.ready !== 'boolean') return 'bad'
  return []
}

function drainHint(data: Record<string, unknown>, drained: boolean): string[] | 'bad' {
  return data.drained === drained ? [] : 'bad'
}

function schemaClass(value: unknown): 'current' | 'future' | 'bad' {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 1 || value > MAX_U32) {
    return 'bad'
  }
  return value === 1 ? 'current' : 'future'
}

function parseWindow(value: unknown): JournalWindow | null {
  if (!isRecord(value)) return null
  if (!isJournalId(value.journal_id) || !isPositiveSeq(value.oldest_seq) || !isSeq(value.head_seq)) {
    return null
  }
  if (BigInt(value.oldest_seq) - 1n > BigInt(value.head_seq)) return null
  return {
    journal_id: value.journal_id,
    oldest_seq: value.oldest_seq,
    head_seq: value.head_seq,
  }
}

function parseCursorId(value: string): EventCursor | null {
  const split = value.lastIndexOf(':')
  if (split <= 0) return null
  const journal_id = value.slice(0, split)
  const seq = value.slice(split + 1)
  if (!isJournalId(journal_id) || !isPositiveSeq(seq)) return null
  return { journal_id, seq }
}

function isJournalId(value: unknown): value is string {
  return typeof value === 'string' && value !== NIL_JOURNAL && JOURNAL_ID.test(value)
}

function isSeq(value: unknown): value is string {
  if (typeof value !== 'string' || !SEQ.test(value)) return false
  return BigInt(value) <= MAX_U64
}

function isPositiveSeq(value: unknown): value is string {
  return isSeq(value) && value !== '0'
}

function isSimpleId(value: unknown): value is string {
  return typeof value === 'string' && SIMPLE_ID.test(value)
}

function isOptionalId(value: unknown): boolean {
  return value == null || isSimpleId(value)
}

function isOptionalCode(value: unknown): boolean {
  return value == null || typeof value === 'string'
}

function isWorkerName(value: unknown): boolean {
  return (
    typeof value === 'string' &&
    value.length > 0 &&
    byteLength(value) <= 128 &&
    /^[A-Za-z0-9._@-]+$/.test(value)
  )
}

function isTime(value: unknown): boolean {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

function parseJson(raw: string): unknown {
  try {
    return JSON.parse(raw) as unknown
  } catch {
    return undefined
  }
}

function byteLength(value: string): number {
  return new TextEncoder().encode(value).length
}
