import { StrictMode } from 'react'
import { act, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { MALICIOUS, task } from '@/test/fixtures'
import { TaskDetail } from './TaskDetail'

function reviewable(overrides: Record<string, unknown> = {}) {
  return detail({
    review_state: 'ready_for_review',
    close_policy: 'never',
    summary: 'Ready for review',
    task: task({
      state: 'open',
      last_outcome: { kind: 'done' },
      close_policy: 'never',
      review_state: 'ready_for_review',
    }),
    ...overrides,
  })
}

function originDelivery(overrides: Record<string, unknown> = {}) {
  return {
    turn_id: 'e'.repeat(32),
    state: 'delivered',
    oid: '0123456789abcdef0123456789abcdef01234567',
    origin: 'https://example.test/repo.git',
    target: 'refs/heads/release-candidate',
    attempt: 1,
    next_attempt_at_millis: 0,
    last_error: null,
    superseded_by: null,
    created_at_millis: 1,
    updated_at_millis: 2,
    ...overrides,
  }
}

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
    review_state: 'closed',
    close_policy: 'done',
    reported_checks: [],
    fetched_head: null,
    fetched_ref: null,
    review_commands: ['worker task fetch aaaa'],
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
    expect(screen.queryByText('DELIVERY')).toBeNull()
  })

  it('shows origin delivery from the detail payload the host serializes', async () => {
    const delivery = originDelivery()
    serve(detail({ delivery, deliveries: [delivery] }))
    render(<TaskDetail taskId="aaaa" />)

    expect(await screen.findByText('DELIVERY')).toBeInTheDocument()
    expect(screen.getByText('delivered · eeeeeeee')).toBeInTheDocument()
  })

  it('shows a pending delivery listed only in deliveries', async () => {
    serve(
      detail({
        deliveries: [originDelivery({ state: 'pending', turn_id: 'f'.repeat(32) })],
      }),
    )
    render(<TaskDetail taskId="aaaa" />)

    expect(await screen.findByText('DELIVERY')).toBeInTheDocument()
    expect(screen.getByText('pending · ffffffff')).toBeInTheDocument()
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

  it('does not start another detail request while the previous poll is pending', async () => {
    const pending = deferred<ReturnType<typeof jsonResponse>>()
    let inFlight = 0
    let peak = 0
    const fetchMock = vi.fn(() => {
      inFlight += 1
      peak = Math.max(peak, inFlight)
      return pending.promise.finally(() => {
        inFlight -= 1
      })
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    render(<TaskDetail taskId="aaaa" />)
    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(peak).toBe(1)

    await act(async () => {
      await vi.advanceTimersByTimeAsync(6000)
    })
    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(peak).toBe(1)

    await act(async () => {
      pending.resolve(jsonResponse(detail({ summary: 'Settled once' })))
    })
    expect(await screen.findByText('Settled once')).toBeInTheDocument()

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000)
    })
    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(peak).toBe(1)
  })

  it('retries 2s after a rejected poll', async () => {
    const fetchMock = vi
      .fn()
      .mockRejectedValueOnce(new Error('poll failed'))
      .mockResolvedValue(jsonResponse(detail({ summary: 'Recovered' })))
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    render(<TaskDetail taskId="aaaa" />)
    expect(await screen.findByText('poll failed')).toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledTimes(1)

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000)
    })
    expect(await screen.findByText('Recovered')).toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('does not apply or reschedule a pending poll after unmount', async () => {
    const pending = deferred<ReturnType<typeof jsonResponse>>()
    const fetchMock = vi.fn(() => pending.promise)
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    const { unmount } = render(<TaskDetail taskId="aaaa" />)
    expect(fetchMock).toHaveBeenCalledTimes(1)
    unmount()

    await act(async () => {
      pending.resolve(jsonResponse(detail({ summary: 'After unmount' })))
      await vi.advanceTimersByTimeAsync(6000)
    })
    expect(screen.queryByText('After unmount')).not.toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('does not let a previous task schedule after a task switch while pending', async () => {
    const pendingA = deferred<ReturnType<typeof jsonResponse>>()
    const pendingB = deferred<ReturnType<typeof jsonResponse>>()
    const fetchMock = vi.fn((url: string) =>
      String(url).includes('task-a') ? pendingA.promise : pendingB.promise,
    )
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    const { rerender } = render(<TaskDetail taskId="task-a" />)
    rerender(<TaskDetail taskId="task-b" />)
    expect(fetchMock).toHaveBeenCalledTimes(2)

    await act(async () => {
      pendingA.resolve(jsonResponse(detail({ summary: 'From task A' })))
      await vi.advanceTimersByTimeAsync(6000)
    })
    expect(screen.queryByText('From task A')).not.toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledTimes(2)

    await act(async () => {
      pendingB.resolve(jsonResponse(detail({ summary: 'From task B' })))
    })
    expect(await screen.findByText('From task B')).toBeInTheDocument()
  })

  it('does not let a StrictMode stale generation schedule after remount', async () => {
    const queue: Array<ReturnType<typeof deferred<ReturnType<typeof jsonResponse>>>> = []
    const fetchMock = vi.fn(() => {
      const item = deferred<ReturnType<typeof jsonResponse>>()
      queue.push(item)
      return item.promise
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })

    render(
      <StrictMode>
        <TaskDetail taskId="aaaa" />
      </StrictMode>,
    )
    expect(queue.length).toBeGreaterThanOrEqual(1)
    const started = fetchMock.mock.calls.length

    await act(async () => {
      queue[0].resolve(jsonResponse(detail({ summary: 'Stale generation' })))
      await vi.advanceTimersByTimeAsync(6000)
    })
    expect(fetchMock.mock.calls.length).toBe(started)
  })

  it('keeps the typed follow-up when a reply is rejected', async () => {
    const reviewable = detail({
      review_state: 'ready_for_review',
      close_policy: 'never',
      reported_checks: [
        {
          name: 'unit',
          command: 'cargo test',
          status: 'pass',
          detail: 'agent ran tests',
          source: 'agent_reported',
        },
      ],
      task: task({
        state: 'open',
        last_outcome: { kind: 'done' },
        close_policy: 'never',
        review_state: 'ready_for_review',
      }),
    })
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce({ ok: true, status: 200, json: async () => reviewable })
      .mockResolvedValueOnce({
        ok: false,
        status: 409,
        json: async () => ({
          error: { code: 'TASK_REVISION_CONFLICT', message: 'task changed before this action' },
        }),
      })
    vi.stubGlobal('fetch', fetchMock)
    render(<TaskDetail taskId="aaaa" />)

    expect(await screen.findByText('AGENT REPORTED')).toBeInTheDocument()
    expect(screen.getByText('unit')).toBeInTheDocument()
    const box = screen.getByLabelText('Follow-up')
    fireEvent.change(box, { target: { value: 'please add tests' } })
    fireEvent.click(screen.getByRole('button', { name: 'Reply' }))
    expect(await screen.findByText('task changed before this action')).toBeInTheDocument()
    expect(screen.getByLabelText('Follow-up')).toHaveValue('please add tests')
  })

  it('posts a reply only once while the first request is in flight', async () => {
    const pendingReply = deferred<ReturnType<typeof jsonResponse>>()
    const fetchMock = vi.fn((_url: string, init?: RequestInit) => {
      if (init?.method === 'POST') return pendingReply.promise
      return Promise.resolve(jsonResponse(reviewable()))
    })
    vi.stubGlobal('fetch', fetchMock)
    render(<TaskDetail taskId="aaaa" />)

    expect(await screen.findByRole('button', { name: 'Reply' })).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('Follow-up'), { target: { value: 'please add tests' } })
    fireEvent.click(screen.getByRole('button', { name: 'Reply' }))
    fireEvent.click(screen.getByRole('button', { name: 'Reply' }))

    expect(fetchMock.mock.calls.filter(([, init]) => init?.method === 'POST')).toHaveLength(1)

    await act(async () => {
      pendingReply.resolve(jsonResponse(reviewable({ summary: 'Follow-up sent', review_state: 'not_reviewable' })))
    })
    expect(await screen.findByText('Follow-up sent')).toBeInTheDocument()
  })

  it('aborts an in-flight reply and does not apply it after a task switch', async () => {
    const pendingReply = deferred<ReturnType<typeof jsonResponse>>()
    let replySignal: AbortSignal | undefined
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (init?.method === 'POST') {
        replySignal = init.signal ?? undefined
        return pendingReply.promise
      }
      if (String(url).includes('task-b')) {
        return Promise.resolve(jsonResponse(reviewable({ summary: 'Task B summary' })))
      }
      return Promise.resolve(jsonResponse(reviewable({ summary: 'Task A summary' })))
    })
    vi.stubGlobal('fetch', fetchMock)

    const { rerender } = render(<TaskDetail taskId="task-a" />)
    expect(await screen.findByText('Task A summary')).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('Follow-up'), { target: { value: 'from A' } })
    fireEvent.click(screen.getByRole('button', { name: 'Reply' }))
    expect(replySignal?.aborted).toBe(false)

    rerender(<TaskDetail taskId="task-b" />)
    expect(replySignal?.aborted).toBe(true)
    expect(await screen.findByText('Task B summary')).toBeInTheDocument()
    expect(screen.getByLabelText('Follow-up')).toHaveValue('')

    await act(async () => {
      pendingReply.resolve(jsonResponse(reviewable({ summary: 'Late reply from A' })))
    })
    expect(screen.queryByText('Late reply from A')).not.toBeInTheDocument()
    expect(screen.queryByText('from A')).not.toBeInTheDocument()
    expect(screen.getByText('Task B summary')).toBeInTheDocument()
  })

  it('aborts an in-flight accept and does not apply it after a task switch', async () => {
    const pendingAccept = deferred<ReturnType<typeof jsonResponse>>()
    let acceptSignal: AbortSignal | undefined
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (init?.method === 'POST') {
        acceptSignal = init.signal ?? undefined
        return pendingAccept.promise
      }
      if (String(url).includes('task-b')) {
        return Promise.resolve(jsonResponse(reviewable({ summary: 'Other task' })))
      }
      return Promise.resolve(jsonResponse(reviewable({ summary: 'Accept this' })))
    })
    vi.stubGlobal('fetch', fetchMock)

    const { rerender } = render(<TaskDetail taskId="task-a" />)
    expect(await screen.findByRole('button', { name: 'Accept' })).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Accept' }))
    expect(acceptSignal?.aborted).toBe(false)

    rerender(<TaskDetail taskId="task-b" />)
    expect(acceptSignal?.aborted).toBe(true)
    expect(await screen.findByText('Other task')).toBeInTheDocument()

    await act(async () => {
      pendingAccept.resolve(
        jsonResponse(
          reviewable({
            summary: 'Late accept from A',
            review_state: 'accepted',
          }),
        ),
      )
    })
    expect(screen.queryByText('Late accept from A')).not.toBeInTheDocument()
    expect(screen.getByText('Other task')).toBeInTheDocument()
  })

  it('does not let a pre-reply GET replace a successful reply', async () => {
    const staleGet = deferred<ReturnType<typeof jsonResponse>>()
    let detailGets = 0
    const fetchMock = vi.fn((_url: string, init?: RequestInit) => {
      if (init?.method === 'POST') {
        return Promise.resolve(
          jsonResponse(reviewable({ summary: 'Reply applied', review_state: 'waiting_on_you' })),
        )
      }
      detailGets += 1
      if (detailGets === 1) {
        return Promise.resolve(jsonResponse(reviewable({ summary: 'Before reply' })))
      }
      if (detailGets === 2) return staleGet.promise
      return Promise.resolve(
        jsonResponse(reviewable({ summary: 'Later poll', review_state: 'waiting_on_you' })),
      )
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    render(<TaskDetail taskId="aaaa" />)

    expect(await screen.findByText('Before reply')).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('Follow-up'), { target: { value: 'please add tests' } })
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000)
    })
    expect(detailGets).toBe(2)

    fireEvent.click(screen.getByRole('button', { name: 'Reply' }))
    expect(await screen.findByText('Reply applied')).toBeInTheDocument()

    await act(async () => {
      staleGet.resolve(jsonResponse(reviewable({ summary: 'Stale GET after reply' })))
    })
    expect(screen.queryByText('Stale GET after reply')).not.toBeInTheDocument()
    expect(screen.getByText('Reply applied')).toBeInTheDocument()
    expect(screen.getByLabelText('Follow-up')).toHaveValue('')

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000)
    })
    expect(await screen.findByText('Later poll')).toBeInTheDocument()
  })

  it('does not let a pre-accept GET replace a successful accept', async () => {
    const staleGet = deferred<ReturnType<typeof jsonResponse>>()
    let detailGets = 0
    const fetchMock = vi.fn((_url: string, init?: RequestInit) => {
      if (init?.method === 'POST') {
        return Promise.resolve(
          jsonResponse(reviewable({ summary: 'Accept applied', review_state: 'accepted' })),
        )
      }
      detailGets += 1
      if (detailGets === 1) {
        return Promise.resolve(jsonResponse(reviewable({ summary: 'Before accept' })))
      }
      if (detailGets === 2) return staleGet.promise
      return Promise.resolve(
        jsonResponse(reviewable({ summary: 'Later accept poll', review_state: 'accepted' })),
      )
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    render(<TaskDetail taskId="aaaa" />)

    expect(await screen.findByText('Before accept')).toBeInTheDocument()
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000)
    })
    expect(detailGets).toBe(2)

    fireEvent.click(screen.getByRole('button', { name: 'Accept' }))
    expect(await screen.findByText('Accept applied')).toBeInTheDocument()

    await act(async () => {
      staleGet.resolve(jsonResponse(reviewable({ summary: 'Stale GET after accept' })))
    })
    expect(screen.queryByText('Stale GET after accept')).not.toBeInTheDocument()
    expect(screen.getByText('Accept applied')).toBeInTheDocument()

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000)
    })
    expect(await screen.findByText('Later accept poll')).toBeInTheDocument()
  })
})
