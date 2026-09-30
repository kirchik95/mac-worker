/**
 * Local mirror of the T1 browser contract until `controllerEvents.contract.ts`
 * is on this branch. Decimal sequences stay strings; never coerce them with Number.
 */

export const FALLBACK_POLL_MS = 2_000
export const HEALTHY_ANTI_ENTROPY_MS = 15_000
export const INVALIDATION_DEBOUNCE_MS = 100
export const WATCHDOG_MS = 30_000
export const RECONNECT_BASE_MS = 1_000
export const RECONNECT_MAX_MS = 15_000
export const MAX_FRAME_BYTES = 1_024
export const EVENTS_PATH = '/api/v1/events'

const MAX_U64 = 18446744073709551615n
const JOURNAL_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
const SEQ = /^(0|[1-9][0-9]{0,19})$/
const KIND = /^[a-z][a-z0-9_]{0,48}(?:\.[a-z0-9_]{1,48}){0,4}$/
const REASON = /^[a-z0-9_][a-z0-9_.-]{0,63}$/i
const SSE_NAMES = ['controller.event', 'snapshot_required', 'ready', 'snapshot.ready', 'heartbeat']

const LIFECYCLE_KINDS = new Set([
  'task.created',
  'task.changed',
  'task.removed',
  'turn.started',
  'turn.finished',
  'turn.outcome_changed',
  'task.auto_continue_scheduled',
  'task.closed',
  'task.abandoned',
  'dag.child_admitted',
])

const SNAPSHOT_KINDS = new Set([
  'queue.changed',
  'run.changed',
  'worker.changed',
  'controller.drained',
  'controller.undrained',
])

export type EventCursor = { journal_id: string; seq: string }
export type JournalWindow = {
  journal_id: string
  oldest_seq: string
  head_seq: string
}
export type WireEvent = {
  schema_version: number
  journal_id: string
  seq: string
  time_millis: number
  kind: string
  data: Record<string, unknown>
}
export type SnapshotRequired = { reason: string; window: JournalWindow | null }
export type ViewerMessage =
  | { event: 'controller.event'; data: WireEvent }
  | { event: 'snapshot_required'; data: SnapshotRequired }
  | { event: 'ready'; data: JournalWindow }
  | { event: 'snapshot.ready'; data: { revision: number } }
  | { event: 'heartbeat'; data: Record<string, never> }

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
  | { type: 'snapshot_required' }
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
    const delay = Math.min(
      RECONNECT_BASE_MS * 2 ** Math.min(reconnectAttempt, 4),
      RECONNECT_MAX_MS,
    )
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
    if (byteLength(raw) > MAX_FRAME_BYTES) {
      fail(false)
      return
    }
    const frame = interpretFrame(name, raw, event.lastEventId)
    if (frame.type === 'malformed' || frame.type === 'snapshot_required') {
      fail(false)
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
  if (name === 'heartbeat') {
    if (raw === '' || raw === '{}') return { type: 'heartbeat' }
    const parsed = parseJson(raw)
    return isRecord(parsed) ? { type: 'heartbeat' } : { type: 'malformed' }
  }
  const parsed = parseJson(raw)
  if (!isRecord(parsed)) return { type: 'malformed' }
  if (name === 'snapshot.ready') {
    return Number.isSafeInteger(parsed.revision) && (parsed.revision as number) >= 0
      ? { type: 'snapshot_ready' }
      : { type: 'malformed' }
  }
  if (name === 'ready') {
    return parseWindow(parsed) ? { type: 'ready' } : { type: 'malformed' }
  }
  if (name === 'snapshot_required') {
    if (typeof parsed.reason !== 'string' || !REASON.test(parsed.reason)) return { type: 'malformed' }
    if (parsed.window == null) return { type: 'snapshot_required' }
    return parseWindow(parsed.window) ? { type: 'snapshot_required' } : { type: 'malformed' }
  }
  if (name !== 'controller.event') return { type: 'malformed' }
  return interpretEvent(parsed, lastEventId)
}

function interpretEvent(parsed: Record<string, unknown>, lastEventId: string): Frame {
  const schema = schemaClass(parsed.schema_version)
  if (schema === 'bad') return { type: 'malformed' }
  if (!isJournalId(parsed.journal_id) || !isSeq(parsed.seq)) return { type: 'malformed' }
  if (!isTime(parsed.time_millis)) return { type: 'malformed' }
  if (typeof parsed.kind !== 'string' || !KIND.test(parsed.kind)) return { type: 'malformed' }
  if (!isRecord(parsed.data)) return { type: 'malformed' }
  const cursor = { journal_id: parsed.journal_id, seq: parsed.seq }
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
  if (!LIFECYCLE_KINDS.has(kind) && !SNAPSHOT_KINDS.has(kind)) return null
  if (kind === 'run.changed') return optionalTask(data)
  if (SNAPSHOT_KINDS.has(kind)) return []
  return requiredTask(data)
}

function optionalTask(data: Record<string, unknown>): string[] | 'bad' {
  if (data.task_id == null) return []
  const id = canonicalTaskId(data.task_id)
  return id ? [id] : 'bad'
}

function requiredTask(data: Record<string, unknown>): string[] | null | 'bad' {
  if (data.task_id == null) return null
  const id = canonicalTaskId(data.task_id)
  return id ? [id] : 'bad'
}

function schemaClass(value: unknown): 'current' | 'future' | 'bad' {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 1 || value > 1_000) return 'bad'
  return value === 1 ? 'current' : 'future'
}

function parseWindow(value: unknown): JournalWindow | null {
  if (!isRecord(value)) return null
  if (!isJournalId(value.journal_id) || !isSeq(value.oldest_seq) || !isSeq(value.head_seq)) return null
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
  if (!isJournalId(journal_id) || !isSeq(seq)) return null
  return { journal_id, seq }
}

function canonicalTaskId(value: unknown): string | null {
  if (typeof value !== 'string') return null
  if (/^[0-9a-f]{32}$/.test(value)) return value
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value)) {
    return value.replaceAll('-', '')
  }
  return null
}

function isJournalId(value: unknown): value is string {
  return typeof value === 'string' && JOURNAL_ID.test(value)
}

function isSeq(value: unknown): value is string {
  if (typeof value !== 'string' || !SEQ.test(value)) return false
  return BigInt(value) <= MAX_U64
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
