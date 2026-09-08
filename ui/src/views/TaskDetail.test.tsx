import { act, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { MALICIOUS, task } from '@/test/fixtures'
import { TaskDetail } from './TaskDetail'

function detail(overrides: Record<string, unknown> = {}) {
  return {
    task: task({ model: 'gpt-6-astra', effort: 'xhigh', state: 'closed' }),
    project_id: 'p',
    worktree_id: 'w',
    base_oid: '0123456789abcdef0123456789abcdef01234567',
    head_oid: 'fedcba9876543210fedcba9876543210fedcba98',
    session_present: true,
    summary: 'Added two tests.',
    questions: [],
    files_changed: ['src/lib.rs'],
    diff_stat: '1 file changed',
    fetch_command: 'worker task fetch aaaa',
    turns: [],
    timeline: [],
    ...overrides,
  }
}

const serve = (payload: unknown) =>
  vi.stubGlobal(
    'fetch',
    vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => payload }),
  )

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

function turnRow(overrides: Record<string, unknown> = {}) {
  return {
    turn_number: 1,
    turn_id: 'q'.repeat(32),
    terminal: null,
    outcome: null,
    agent_committed: false,
    log_truncated: false,
    started_at_millis: null,
    ended_at_millis: null,
    ...overrides,
  }
}

function jsonResponse(payload: unknown) {
  return { ok: true, status: 200, json: async () => payload }
}

afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
})

describe('TaskDetail', () => {
  it('shows the structured result and the exact fetch command', async () => {
    serve(detail())
    render(<TaskDetail taskId="aaaa" />)

    expect(await screen.findByText('Added two tests.')).toBeInTheDocument()
    expect(screen.getByText('worker task fetch aaaa')).toBeInTheDocument()
    expect(screen.getByText('src/lib.rs')).toBeInTheDocument()
  })

  it('renders both question shapes, with the answers an agent will accept', async () => {
    serve(
      detail({
        questions: ['Anything else?', { text: 'Which base?', options: ['main', 'release'] }],
      }),
    )
    render(<TaskDetail taskId="aaaa" />)

    expect(await screen.findByText('Anything else?')).toBeInTheDocument()
    expect(screen.getByText('Which base?')).toBeInTheDocument()
    expect(screen.getByText('main')).toBeInTheDocument()
    expect(screen.getByText('release')).toBeInTheDocument()
  })

  it('renders a hostile summary as text', async () => {
    serve(detail({ summary: MALICIOUS }))
    render(<TaskDetail taskId="aaaa" />)

    expect(await screen.findByText(MALICIOUS)).toBeInTheDocument()
    expect(document.querySelector('img')).toBeNull()
  })

  it('lists the turns with their outcome and terminal state', async () => {
    serve(
      detail({
        turns: [
          {
            turn_number: 1,
            turn_id: 'b'.repeat(32),
            terminal: 'succeeded',
            outcome: { kind: 'blocked' },
            agent_committed: false,
            log_truncated: true,
            started_at_millis: 1,
            ended_at_millis: 2,
          },
        ],
      }),
    )
    render(<TaskDetail taskId="aaaa" />)

    expect(await screen.findByText('01')).toBeInTheDocument()
    // Statuses render as an uppercase monospace label beside a line glyph. The
    // process state is shown next to the structured outcome because a zero exit
    // with a blocked outcome is a failed turn.
    expect(screen.getByText('blocked')).toBeInTheDocument()
    expect(screen.getByText('succeeded')).toBeInTheDocument()
    expect(screen.getByText('LOG TRUNCATED')).toBeInTheDocument()
  })

  it('opens one turn at a time and reads its recorded log', async () => {
    const { default: userEvent } = await import('@testing-library/user-event')
    const payload = detail({
      turns: [
        {
          turn_number: 2,
          turn_id: 'c'.repeat(32),
          terminal: 'succeeded',
          outcome: { kind: 'done' },
          agent_committed: true,
          log_truncated: false,
          started_at_millis: 1_000,
          ended_at_millis: 4_000,
        },
      ],
    })
    const line = JSON.stringify({ type: 'exec', command: 'cargo test --locked' })
    vi.stubGlobal(
      'fetch',
      vi.fn().mockImplementation((url: string) =>
        Promise.resolve({
          ok: true,
          status: 200,
          json: async () =>
            url.includes('/logs')
              ? {
                  stream: 'stdout',
                  offset: 0,
                  next_offset: line.length,
                  data: btoa(line),
                }
              : payload,
        }),
      ),
    )
    render(<TaskDetail taskId="aaaa" />)

    await userEvent.setup().click(await screen.findByRole('button', { name: /02/ }))
    expect(await screen.findByText('cargo test --locked')).toBeInTheDocument()
    expect(
      screen.getByText('the turn is finished · read to the current end'),
    ).toBeInTheDocument()
  })

  it('says a task has no turns rather than drawing an empty card', async () => {
    serve(detail())
    render(<TaskDetail taskId="aaaa" />)
    expect(await screen.findByText('This task has no recorded turns.')).toBeInTheDocument()
  })

  it('shows a full-page error when the first detail read fails', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('task missing')))
    render(<TaskDetail taskId="aaaa" />)

    expect(await screen.findByText('task missing')).toBeInTheDocument()
    expect(screen.queryByText('Loading task…')).not.toBeInTheDocument()
  })

  it('keeps last-good detail through a failed poll and recovers on the next', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(detail({ summary: 'Detail A kept' })))
      .mockRejectedValueOnce(new Error('poll failed'))
      .mockResolvedValue(jsonResponse(detail({ summary: 'Detail B recovered' })))
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    render(<TaskDetail taskId="aaaa" />)
    expect(await screen.findByText('Detail A kept')).toBeInTheDocument()

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000)
    })
    expect(screen.getByText('Detail A kept')).toBeInTheDocument()
    expect(screen.getByText('poll failed')).toBeInTheDocument()
    expect(screen.queryByText('Loading task…')).not.toBeInTheDocument()

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000)
    })
    expect(await screen.findByText('Detail B recovered')).toBeInTheDocument()
    expect(screen.queryByText('poll failed')).not.toBeInTheDocument()
    expect(screen.queryByText('Detail A kept')).not.toBeInTheDocument()
  })

  it('does not let a previous task id replace a later one', async () => {
    const pendingA = deferred<ReturnType<typeof jsonResponse>>()
    const pendingB = deferred<ReturnType<typeof jsonResponse>>()
    vi.stubGlobal(
      'fetch',
      vi.fn((url: string) =>
        String(url).includes('task-a') ? pendingA.promise : pendingB.promise,
      ),
    )

    const { rerender } = render(<TaskDetail taskId="task-a" />)
    rerender(<TaskDetail taskId="task-b" />)

    await act(async () => {
      pendingA.resolve(jsonResponse(detail({ summary: 'From task A' })))
    })
    expect(screen.queryByText('From task A')).not.toBeInTheDocument()

    await act(async () => {
      pendingB.resolve(jsonResponse(detail({ summary: 'From task B' })))
    })
    expect(await screen.findByText('From task B')).toBeInTheDocument()
  })

  it('does not fetch logs for an expanded queued turn, then drains a skipped terminal tail', async () => {
    const queued = detail({
      summary: 'Still queued',
      turns: [turnRow()],
    })
    const terminal = detail({
      summary: 'Turn already ended',
      turns: [
        turnRow({
          terminal: 'failed',
          outcome: { kind: 'failed' },
          started_at_millis: null,
          ended_at_millis: 4_000,
        }),
      ],
    })
    const line = JSON.stringify({ type: 'exec', command: 'queued-to-terminal tail' })
    let detailPolls = 0
    const fetchMock = vi.fn((url: string) => {
      if (String(url).includes('/logs')) {
        return Promise.resolve({
          ok: true,
          status: 200,
          json: async () => ({
            stream: 'stdout',
            offset: 0,
            next_offset: line.length,
            data: btoa(line),
          }),
        })
      }
      detailPolls += 1
      return Promise.resolve(jsonResponse(detailPolls === 1 ? queued : terminal))
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    render(<TaskDetail taskId="aaaa" />)
    expect(await screen.findByText('Still queued')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /01/ }))

    expect(screen.getByText('Waiting for this turn to start…')).toBeInTheDocument()
    expect(
      screen.queryByText('the turn is finished · read to the current end'),
    ).not.toBeInTheDocument()
    expect(fetchMock.mock.calls.some(([url]) => String(url).includes('/logs'))).toBe(false)

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000)
    })
    expect(await screen.findByText('queued-to-terminal tail')).toBeInTheDocument()
    expect(
      screen.getByText('the turn is finished · read to the current end'),
    ).toBeInTheDocument()
    expect(screen.queryByText('Waiting for this turn to start…')).not.toBeInTheDocument()
    expect(fetchMock.mock.calls.some(([url]) => String(url).includes('/logs'))).toBe(true)
  })

  it('starts following after an observed queued-to-running transition', async () => {
    const queued = detail({
      summary: 'Still queued',
      turns: [turnRow()],
    })
    const running = detail({
      summary: 'Turn is running',
      turns: [turnRow({ started_at_millis: 1_000, ended_at_millis: null })],
    })
    let detailPolls = 0
    const fetchMock = vi.fn((url: string) => {
      if (String(url).includes('/logs')) {
        return Promise.resolve({
          ok: true,
          status: 200,
          json: async () => ({
            stream: 'stdout',
            offset: 0,
            next_offset: 0,
            data: '',
          }),
        })
      }
      detailPolls += 1
      return Promise.resolve(jsonResponse(detailPolls === 1 ? queued : running))
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    render(<TaskDetail taskId="aaaa" />)
    expect(await screen.findByText('Still queued')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /01/ }))
    expect(screen.getByText('Waiting for this turn to start…')).toBeInTheDocument()

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000)
    })
    expect(await screen.findByText('following · the turn is still running')).toBeInTheDocument()
    expect(screen.queryByText('Waiting for this turn to start…')).not.toBeInTheDocument()
    expect(fetchMock.mock.calls.some(([url]) => String(url).includes('/logs'))).toBe(true)
  })
})
