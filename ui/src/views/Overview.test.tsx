import { render, screen, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { MALICIOUS, queueEntry, snapshot, task, worker } from '@/test/fixtures'
import { Overview } from './Overview'

// A worker's name appears on its card and again in the capabilities strip;
// the card is the one inside an <article>.
const card = (name: string) =>
  screen
    .getAllByText(name)
    .map((node) => node.closest('article'))
    .find((node): node is HTMLElement => node != null) as HTMLElement

// A busy worker renders the live turn panel, which polls its log.
beforeEach(() =>
  vi.stubGlobal(
    'fetch',
    vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ stream: 'stdout', offset: 0, next_offset: 0, data: '' }),
    }),
  ),
)
afterEach(() => vi.unstubAllGlobals())

describe('Overview', () => {
  it('reports an idle worker as available with its metrics', () => {
    render(<Overview snapshot={snapshot()} />)
    const mini = card('mini-1')

    expect(within(mini).getByText('available')).toBeInTheDocument()
    expect(within(mini).getByText('Ready for the next task')).toBeInTheDocument()
    expect(within(mini).getByText('0 / 1 slots occupied')).toBeInTheDocument()
    expect(within(mini).getByText('DISK FREE')).toBeInTheDocument()
    expect(within(mini).getByText('150.0 GB')).toBeInTheDocument()
    expect(within(mini).getByText('Warn')).toBeInTheDocument()
  })

  it('shows a dash for a CPU reading the worker did not report', () => {
    render(<Overview snapshot={snapshot()} />)
    expect(within(card('mini-1')).getByText('—')).toBeInTheDocument()
  })

  it('names the running task and its launch settings', () => {
    const busy = worker({
      slot: { state: 'busy', capacity: 1, active_job_id: 'job' },
      active_task: {
        task_id: 'a'.repeat(32),
        title: 'Cursor smoke test',
        agent: 'cursor',
        model: 'gpt-5',
        effort: 'high',
        turn_number: 1,
      },
    })
    render(<Overview snapshot={snapshot({ workers: [busy] })} />)
    const mini = card('mini-1')

    expect(within(mini).getByText('running')).toBeInTheDocument()
    expect(within(mini).getByText('Cursor smoke test')).toBeInTheDocument()
    expect(within(mini).getByText('AGENT')).toBeInTheDocument()
    expect(within(mini).getByText('gpt-5')).toBeInTheDocument()
    expect(within(mini).getByText('High')).toBeInTheDocument()
  })

  it('says a cached observation is out of date instead of implying it is live', () => {
    const stale = worker({ freshness: 'stale' })
    render(<Overview snapshot={snapshot({ workers: [stale] })} />)
    const mini = card('mini-1')

    expect(within(mini).getByText('stale')).toBeInTheDocument()
    expect(within(mini).getByText('Observation is out of date')).toBeInTheDocument()
    expect(within(mini).getByText('LAST CPU')).toBeInTheDocument()
  })

  it('surfaces the reason an unreachable worker is offline', () => {
    const broken = worker({
      health: 'unavailable',
      error: { code: 'SSH_UNAVAILABLE', message: 'ssh timed out connecting to mini-1' },
    })
    render(<Overview snapshot={snapshot({ workers: [broken] })} />)
    const mini = card('mini-1')

    expect(within(mini).getByText('offline')).toBeInTheDocument()
    expect(within(mini).getByText('ssh timed out connecting to mini-1')).toBeInTheDocument()
    expect(within(mini).getByText('SSH_UNAVAILABLE')).toBeInTheDocument()
  })

  it('renders a hostile task title as text', () => {
    const busy = worker({
      slot: { state: 'busy', capacity: 1, active_job_id: 'job' },
      active_task: {
        task_id: 'a'.repeat(32),
        title: MALICIOUS,
        agent: 'codex',
        model: null,
        effort: null,
        turn_number: 1,
      },
    })
    render(<Overview snapshot={snapshot({ workers: [busy] })} />)
    // The title appears on the card and again in the live turn panel; both must
    // be text, never markup.
    expect(screen.getAllByText(MALICIOUS).length).toBeGreaterThan(0)
    expect(document.querySelector('img')).toBeNull()
  })

  it('projects a queue entry with its position and the code that says why it waits', () => {
    const queued = snapshot({
      tasks: [task({ task_id: 'q'.repeat(32), title: 'Fix login redirect loop' })],
      queue: [
        queueEntry({
          task_id: 'q'.repeat(32),
          position: 2,
          blocking_code: 'NO_COMPATIBLE_IDLE_WORKER',
          created_at_millis: 0,
        }),
      ],
    })
    render(<Overview snapshot={queued} now={60_000} />)

    expect(screen.getByText('02')).toBeInTheDocument()
    expect(screen.getByText('Fix login redirect loop')).toBeInTheDocument()
    expect(screen.getByText('NO_COMPATIBLE_IDLE_WORKER')).toBeInTheDocument()
    expect(
      screen.getByText('No idle worker advertises what this entry needs.'),
    ).toBeInTheDocument()
    expect(screen.getByText('1m')).toBeInTheDocument()
  })

  it('names the capability a pin is missing rather than only the code', () => {
    const queued = snapshot({
      queue: [
        queueEntry({
          blocking_code: 'CAPABILITY_MISSING:agent:opencode',
          pinned_worker: 'mini-3',
        }),
      ],
    })
    render(<Overview snapshot={queued} />)

    expect(screen.getByText('CAPABILITY_MISSING')).toBeInTheDocument()
    expect(screen.getByText(/Missing: agent:opencode/)).toBeInTheDocument()
    expect(screen.getByText('pinned to mini-3')).toBeInTheDocument()
  })

  it('says the queue is empty rather than drawing an empty table', () => {
    render(<Overview snapshot={snapshot()} />)
    expect(screen.getByText('The queue is empty.')).toBeInTheDocument()
  })

  it('explains a queue that cannot move because the agent facts expired', () => {
    const stalled = snapshot({
      workers: [
        worker({ agent_facts: { collected_at_millis: 0, freshness: 'stale', agents: [] } }),
        worker({ name: 'mini-3', health: 'unavailable', agent_facts: null }),
      ],
      queue: [queueEntry({ created_at_millis: 0 })],
    })
    render(<Overview snapshot={stalled} now={40 * 60_000} />)

    expect(screen.getByText(/Nothing has dispatched for 40m/)).toBeInTheDocument()
    expect(screen.getByText('worker workers --refresh')).toBeInTheDocument()
    expect(screen.getByText(/will fail while mini-3 is unreachable/)).toBeInTheDocument()
  })

  it('does not claim a stall while a worker is running a turn', () => {
    const running = snapshot({
      workers: [
        worker({
          slot: { state: 'busy', capacity: 1, active_job_id: 'job' },
          agent_facts: { collected_at_millis: 0, freshness: 'stale', agents: [] },
        }),
      ],
      queue: [queueEntry({ created_at_millis: 0 })],
    })
    render(<Overview snapshot={running} now={40 * 60_000} />)

    expect(screen.queryByText(/Nothing has dispatched/)).toBeNull()
  })

  it('marks a worker whose agent facts have expired as advertising nothing usable', () => {
    const lapsed = snapshot({
      workers: [
        worker({
          agent_facts: {
            collected_at_millis: 0,
            freshness: 'stale',
            agents: [{ name: 'codex', version: null, auth: 'authenticated', auth_by_profile: [] }],
          },
        }),
      ],
    })
    render(<Overview snapshot={lapsed} now={40 * 60_000} />)

    expect(screen.getByText('EXPIRED')).toBeInTheDocument()
    expect(screen.getByText('codex · lapsed')).toBeInTheDocument()
    expect(screen.getByText('0 of 1 workers can take work')).toBeInTheDocument()
  })

  it('says the fleet is empty instead of drawing nothing', () => {
    render(<Overview snapshot={snapshot({ workers: [] })} />)
    expect(screen.getByText('No workers are configured.')).toBeInTheDocument()
  })
})

describe('Overview attention strip', () => {
  it('offers a way to the tasks that need an answer', async () => {
    const { default: userEvent } = await import('@testing-library/user-event')
    const onShowAttention = vi.fn()
    const needing = snapshot({
      tasks: [task({ state: 'open', last_outcome: { kind: 'needs_input' } })],
    })
    render(<Overview snapshot={needing} onShowAttention={onShowAttention} />)

    expect(screen.getByText('1 task needs attention')).toBeInTheDocument()
    await userEvent.setup().click(screen.getByRole('button', { name: /View/ }))
    expect(onShowAttention).toHaveBeenCalled()
  })

  it('does not offer the action when nothing needs attention', () => {
    render(<Overview snapshot={snapshot()} onShowAttention={() => {}} />)
    expect(screen.getByText('No task needs attention')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /View/ })).toBeNull()
  })
})
