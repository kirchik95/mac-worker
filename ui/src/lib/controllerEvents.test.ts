import { afterEach, describe, expect, it, vi } from 'vitest'

import { controllerEventFixtureMessages } from './controllerEvents.contract'
import fixtures from './controllerEvents.fixtures.json'
import { createControllerEvents } from './controllerEvents'

const FIXTURES = controllerEventFixtureMessages

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

function eventFrame(
  seq: string,
  kind: string,
  data: Record<string, unknown>,
  schema = 1,
  journal = JOURNAL,
) {
  return {
    schema_version: schema,
    journal_id: journal,
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
      eventFrame('5', 'turn.finished', { task_id: TASK, outcome: 'needs_input' }, 1001),
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
      eventFrame('6', 'turn.finished', {
        task_id: TASK,
        turn_id: 'b'.repeat(32),
        run_id: null,
        outcome: 'imaginary_outcome',
        code: null,
      }),
      `${JOURNAL}:6`,
    )
    expect(h.closedSources()).toBeGreaterThan(0)
    expect(JSON.stringify(h.invalidations())).not.toContain('imaginary_outcome')
    expect(JSON.stringify(h.invalidations())).not.toContain(TASK)
    expect(saved.last_outcome.kind).toBe('done')
    h.advance(1_000)
    expect(h.lastReconnectUrl()).not.toContain(':6')
  })

  it('reconnects on the frozen 1s, 2s, 4s, then 5s schedule', () => {
    const h = makeEventHarness()
    h.client.start()
    for (const delay of [1_000, 2_000, 4_000, 5_000, 5_000]) {
      const opened = h.sourceCount()
      h.error()
      h.advance(delay - 1)
      expect(h.sourceCount()).toBe(opened)
      h.advance(1)
      expect(h.sourceCount()).toBe(opened + 1)
    }
    expect(controllerEventFixtureMessages.event_above_2pow53).toEqual({
      event: fixtures.event_above_2pow53.event,
      data: fixtures.event_above_2pow53.data,
    })
    expect(fixtures.event_above_2pow53.data.seq).toBe('9007199254740993')
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

  it('bootstrap then ready then a future event stays healthy on one source', () => {
    const h = makeEventHarness()
    h.client.start()
    const window = fixtures.bootstrap.data.window
    for (const delay of [1_000, 2_000, 4_000, 5_000]) {
      h.emitFixture('bootstrap')
      h.emit('ready', window)
      expect(h.closedSources()).toBe(0)
      h.advance(delay)
      expect(h.sourceCount()).toBe(1)
    }
    expect(h.lastHealth()).toBe(true)
    expect(h.lastReconnectUrl()).toBe('/api/v1/events')
    h.emit('controller.event', eventFrame('4', 'queue.changed', {}), `${JOURNAL}:4`)
    expect(h.sourceCount()).toBe(1)
    expect(h.closedSources()).toBe(0)
    h.error()
    h.advance(1_000)
    expect(h.lastReconnectUrl()).toContain(`${JOURNAL}:4`)
    expect(h.lastReconnectUrl()).not.toContain(window.head_seq)
  })

  it('cursor_expired rebaselines without the stale cursor', () => {
    const h = makeEventHarness()
    h.client.start()
    h.emit('controller.event', eventFrame('3', 'queue.changed', {}), `${JOURNAL}:3`)
    h.emit('snapshot_required', {
      reason: 'cursor_expired',
      window: { journal_id: JOURNAL, oldest_seq: '10', head_seq: '42' },
    })
    expect(h.closedSources()).toBe(0)
    expect(h.sourceCount()).toBe(1)
    h.emit('ready', { journal_id: JOURNAL, oldest_seq: '10', head_seq: '42' })
    expect(h.lastHealth()).toBe(true)
    expect(h.sourceCount()).toBe(1)
    h.error()
    h.advance(1_000)
    expect(h.lastReconnectUrl()).toBe('/api/v1/events')
    expect(h.lastReconnectUrl()).not.toContain(':3')
    expect(h.lastReconnectUrl()).not.toContain(':42')
  })

  it('lag repair reconnects without the stale cursor when the server closes', () => {
    const h = makeEventHarness()
    h.client.start()
    h.emit('controller.event', eventFrame('3', 'queue.changed', {}), `${JOURNAL}:3`)
    h.emit('snapshot_required', {
      reason: 'lagged',
      window: { journal_id: JOURNAL, oldest_seq: '1', head_seq: '42' },
    })
    expect(h.closedSources()).toBe(0)
    h.error()
    h.advance(1_000)
    expect(h.lastReconnectUrl()).toBe('/api/v1/events')
    expect(h.lastReconnectUrl()).not.toContain(':3')
    expect(h.lastReconnectUrl()).not.toContain(':42')
  })

  it('repeats journal_changed epoch repair on the open stream', () => {
    const next = '00000000-0000-0000-0000-000000000009'
    const third = '11111111-1111-4111-8111-111111111111'
    const h = makeEventHarness()
    h.client.start()
    h.emit('controller.event', eventFrame('3', 'queue.changed', {}), `${JOURNAL}:3`)
    for (const journal of [next, third]) {
      const window = { journal_id: journal, oldest_seq: '1', head_seq: '0' }
      h.emit('snapshot_required', { reason: 'journal_changed', window })
      expect(h.closedSources()).toBe(0)
      h.emit('ready', window)
      h.emit(
        'controller.event',
        eventFrame('1', 'queue.changed', {}, 1, journal),
        `${journal}:1`,
      )
    }
    expect(h.sourceCount()).toBe(1)
    expect(h.closedSources()).toBe(0)
    expect(h.lastHealth()).toBe(true)
    h.error()
    h.advance(1_000)
    expect(h.lastReconnectUrl()).toContain(`${third}:1`)
    expect(h.lastReconnectUrl()).not.toContain(JOURNAL)
  })

  it('does not lose an event when replay follows ready', () => {
    const h = makeEventHarness()
    h.client.start()
    h.emit('controller.event', eventFrame('3', 'queue.changed', {}), `${JOURNAL}:3`)
    h.emit('ready', { journal_id: JOURNAL, oldest_seq: '1', head_seq: '10' })
    expect(h.closedSources()).toBe(0)
    expect(h.sourceCount()).toBe(1)
    h.emit('controller.event', eventFrame('4', 'queue.changed', {}), `${JOURNAL}:4`)
    h.advance(100)
    expect(h.closedSources()).toBe(0)
    h.error()
    h.advance(1_000)
    expect(h.lastReconnectUrl()).toContain(`${JOURNAL}:4`)
    expect(h.lastReconnectUrl()).not.toContain(':10')
  })

  it('unavailable still closes and keeps the event cursor', () => {
    const h = makeEventHarness()
    h.client.start()
    h.emit('controller.event', eventFrame('3', 'queue.changed', {}), `${JOURNAL}:3`)
    h.emit('snapshot_required', {
      reason: 'unavailable',
      code: 'CONTROLLER_EVENTS_UNAVAILABLE',
      window: null,
    })
    expect(h.closedSources()).toBeGreaterThan(0)
    h.advance(1_000)
    expect(h.lastReconnectUrl()).toContain(`${JOURNAL}:3`)
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
