import { afterEach, describe, expect, it, vi } from 'vitest'

import { createControllerEvents } from './controllerEvents'

/**
 * Local copy of T1's controllerEvents.fixtures.json. Phase B should import
 * `controllerEventFixtureMessages` once that file is on the branch.
 */
const FIXTURES = {
  bootstrap: {
    event: 'snapshot_required',
    data: {
      reason: 'bootstrap',
      window: {
        journal_id: '614dc3be-668f-4922-bd31-b1d7a0056790',
        oldest_seq: '1',
        head_seq: '9007199254740993',
      },
    },
  },
  event_above_2pow53: {
    event: 'controller.event',
    data: {
      schema_version: 1,
      journal_id: '614dc3be-668f-4922-bd31-b1d7a0056790',
      seq: '9007199254740993',
      time_millis: 1790726400000,
      kind: 'turn.finished',
      data: {
        task_id: '0e7b7f915a914dbdbcc2c33338890752',
        turn_id: '16366c4ca0aa419c8b783dcd61e68102',
        run_id: null,
        outcome: 'needs_input',
        code: null,
      },
    },
  },
  'snapshot.ready': {
    event: 'snapshot.ready',
    data: { revision: 42 },
  },
  heartbeat: {
    event: 'heartbeat',
    data: {},
  },
} as const

const JOURNAL = '614dc3be-668f-4922-bd31-b1d7a0056790'
const TASK = '0e7b7f915a914dbdbcc2c33338890752'

class MockEventSource {
  closed = false
  readonly url: string
  private listeners = new Map<string, Set<(event: Event) => void>>()

  constructor(url: string) {
    this.url = url
  }

  addEventListener(type: string, listener: (event: Event) => void) {
    const set = this.listeners.get(type) ?? new Set()
    set.add(listener)
    this.listeners.set(type, set)
  }

  removeEventListener(type: string, listener: (event: Event) => void) {
    this.listeners.get(type)?.delete(listener)
  }

  close() {
    this.closed = true
  }

  emit(name: string, data: string, lastEventId = '') {
    if (this.closed) return
    const event = new MessageEvent(name, { data, lastEventId })
    this.listeners.get(name)?.forEach((listener) => listener(event))
  }

  error() {
    if (this.closed) return
    const event = new Event('error')
    this.listeners.get('error')?.forEach((listener) => listener(event))
  }
}

function makeEventHarness() {
  vi.useFakeTimers()
  const sources: MockEventSource[] = []
  const invalidations: Array<string[] | null> = []
  const health: boolean[] = []
  const client = createControllerEvents({
    makeSource(url) {
      const source = new MockEventSource(url)
      sources.push(source)
      return source as unknown as EventSource
    },
    now: () => Date.now(),
    setTimer: (fn, delay) => setTimeout(fn, delay) as unknown as number,
    clearTimer: (id) => clearTimeout(id),
    invalidate: (taskIds) => {
      invalidations.push(taskIds)
    },
    health: (value) => {
      health.push(value)
    },
  })

  const current = () => sources.at(-1)

  const emit = (name: string, payload: unknown, lastEventId = '') => {
    const source = current()
    if (!source || source.closed) throw new Error('no open source')
    const data = typeof payload === 'string' ? payload : JSON.stringify(payload)
    source.emit(name, data, lastEventId)
  }

  return {
    client,
    invalidations: () => invalidations,
    emit,
    emitFixture(name: keyof typeof FIXTURES) {
      const fixture = FIXTURES[name]
      const data = fixture.data
      const lastEventId =
        fixture.event === 'controller.event' && 'journal_id' in data && 'seq' in data
          ? `${data.journal_id}:${data.seq}`
          : ''
      emit(fixture.event, data, lastEventId)
    },
    advance(ms: number) {
      vi.advanceTimersByTime(ms)
    },
    error() {
      current()?.error()
    },
    lastHealth: () => health.at(-1) ?? null,
    lastReconnectUrl: () => current()?.url ?? '',
    closedSources: () => sources.filter((source) => source.closed).length,
    sourceCount: () => sources.length,
  }
}

function eventFrame(seq: string, kind: string, data: Record<string, unknown>, schema = 1) {
  return {
    schema_version: schema,
    journal_id: JOURNAL,
    seq,
    time_millis: 5,
    kind,
    data,
  }
}

afterEach(() => {
  vi.useRealTimers()
})

describe('controller events client', () => {
  it('falls back after stream silence and preserves string cursor on reconnect', () => {
    const h = makeEventHarness()
    h.client.start()
    h.emitFixture('event_above_2pow53')
    h.advance(30_000)
    expect(h.lastHealth()).toBe(false)
    expect(h.lastReconnectUrl()).toContain('9007199254740993')
    expect(h.closedSources()).toBeGreaterThan(0)
    expect(h.lastReconnectUrl()).not.toContain('9.007199254740993e+15')
  })

  it('named_heartbeat_keeps_health', () => {
    const h = makeEventHarness()
    h.client.start()
    h.emitFixture('event_above_2pow53')
    h.advance(10_000)
    h.emitFixture('heartbeat')
    h.advance(20_000)
    expect(h.lastHealth()).toBe(true)
    expect(h.closedSources()).toBe(0)
    h.advance(10_000)
    expect(h.lastHealth()).toBe(false)
    expect(h.lastReconnectUrl()).toContain('9007199254740993')
  })

  it('tunnel_disconnect_immediate_polling', () => {
    const h = makeEventHarness()
    h.client.start()
    h.emitFixture('heartbeat')
    expect(h.lastHealth()).toBe(true)
    h.error()
    expect(h.lastHealth()).toBe(false)
    expect(h.closedSources()).toBeGreaterThan(0)
    const closed = h.closedSources()
    h.advance(999)
    expect(h.closedSources()).toBe(closed)
    h.advance(1)
    expect(h.sourceCount()).toBeGreaterThan(closed)
  })

  it('unknown_kind_global', () => {
    const h = makeEventHarness()
    h.client.start()
    h.emit(
      'controller.event',
      eventFrame('4', 'future.hint', { task_id: TASK, outcome: 'imaginary_outcome' }),
      `${JOURNAL}:4`,
    )
    h.advance(100)
    expect(h.invalidations()).toEqual([null])
    expect(JSON.stringify(h.invalidations())).not.toContain('imaginary_outcome')
    expect(h.closedSources()).toBe(0)

    h.emit(
      'controller.event',
      eventFrame('5', 'turn.finished', { task_id: TASK, outcome: 'needs_input' }, 2),
      `${JOURNAL}:5`,
    )
    h.advance(100)
    expect(h.invalidations().at(-1)).toBeNull()
    expect(h.closedSources()).toBe(0)
    h.advance(30_000)
    expect(h.lastReconnectUrl()).toContain(':5')
    expect(h.lastReconnectUrl()).not.toContain('9007199254740993')
  })

  it('does not invent task state from an unknown outcome', () => {
    const saved = { title: 'Repair login', last_outcome: { kind: 'done' } }
    const h = makeEventHarness()
    h.client.start()
    h.emit(
      'controller.event',
      eventFrame('6', 'turn.finished', { task_id: TASK, outcome: 'imaginary_outcome' }),
      `${JOURNAL}:6`,
    )
    h.advance(100)
    expect(h.invalidations()).toEqual([[TASK]])
    expect(JSON.stringify(h.invalidations())).not.toContain('imaginary_outcome')
    expect(saved.last_outcome.kind).toBe('done')
  })

  it('malformed_payload_closes_source', () => {
    const h = makeEventHarness()
    h.client.start()
    h.emit('controller.event', '{', '')
    expect(h.closedSources()).toBeGreaterThan(0)
    expect(h.lastHealth()).toBe(false)
    h.advance(1_000)
    expect(h.sourceCount()).toBe(2)

    const reopened = makeEventHarness()
    reopened.client.start()
    reopened.emit('controller.event', 'x'.repeat(1_025), '')
    expect(reopened.closedSources()).toBeGreaterThan(0)
    expect(reopened.lastHealth()).toBe(false)
  })

  it('reset_controls_never_skip_cursor', () => {
    const h = makeEventHarness()
    h.client.start()
    h.emit(
      'controller.event',
      eventFrame('3', 'task.changed', { task_id: TASK, state: 'active' }),
      `${JOURNAL}:3`,
    )
    h.emit('ready', {
      journal_id: JOURNAL,
      oldest_seq: '1',
      head_seq: '9007199254740993',
    })
    expect(h.closedSources()).toBe(0)
    h.emitFixture('bootstrap')
    expect(h.closedSources()).toBeGreaterThan(0)
    h.advance(1_000)
    const url = h.lastReconnectUrl()
    expect(url).toContain(`${JOURNAL}:3`)
    expect(url).not.toContain('9007199254740993')
  })

  it('unsubscribe_closes_timer_and_source', () => {
    const h = makeEventHarness()
    h.client.start()
    expect(h.sourceCount()).toBe(1)
    h.client.stop()
    expect(h.closedSources()).toBe(1)
    h.advance(60_000)
    expect(h.sourceCount()).toBe(1)
    expect(h.closedSources()).toBe(1)
  })
})
