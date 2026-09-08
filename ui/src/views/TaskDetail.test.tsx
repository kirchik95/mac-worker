import { render, screen } from '@testing-library/react'
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

afterEach(() => vi.unstubAllGlobals())

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
})
