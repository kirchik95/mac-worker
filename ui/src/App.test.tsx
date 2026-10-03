import { type ReactNode } from 'react'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { ControllerEventsProvider } from '@/hooks/ControllerEventsContext'
import { useAttentionQuestions } from '@/hooks/useAttentionQuestions'
import { useSnapshot } from '@/hooks/useSnapshot'
import { useTaskPreviews } from '@/hooks/useTaskPreviews'
import { snapshot, task, worker } from '@/test/fixtures'
import { TaskDetail } from '@/views/TaskDetail'
import integrationFixtures from '@/lib/integration.fixtures.json'
import { decodeIntegrationView } from '@/lib/integration.contract'
import App, { documentTitle, parseTaskHash } from './App'

afterEach(() => {
  window.location.hash = ''
  vi.unstubAllGlobals()
  vi.useRealTimers()
})

describe('App states', () => {
  it('enables blocked integration actions in Overview after a confirmed preview loads', async () => {
    const view = decodeIntegrationView(integrationFixtures.cases.find(row => row.name === 'blocked')!.view)
    const row = { ...task({ state: 'open' }), ...view }
    const armed = decodeIntegrationView(integrationFixtures.cases.find(row => row.name === 'armed')!.view)
    const dependency = { ...task({ task_id: 'b'.repeat(32), state: 'abandoned',
      blocking_code: 'INTEGRATION_DEPENDENCY_NOT_INTEGRATED' }), ...armed,
      workflow_state: 'needs_you' as const }
    let loadPreview!: (value: unknown) => void
    const preview = new Promise(resolve => { loadPreview = resolve })
    const fetch = vi.fn((url: string, options?: RequestInit) => {
      if (String(url).includes('/api/v1/snapshot')) return Promise.resolve({ ok: true,
        json: async () => snapshot({ tasks: [row, dependency] }) })
      if (options?.method === 'POST') return Promise.resolve({ ok: true, json: async () => ({}) })
      if (String(url).endsWith(`/tasks/${row.task_id}`)) return Promise.resolve({ ok: true,
        json: () => preview })
      return Promise.resolve({ ok: true, json: async () => ({ task: dependency }) })
    })
    vi.stubGlobal('fetch', fetch)
    render(<App />)
    const redrive = await screen.findByRole('button', { name: 'Re-drive integration' })
    expect(redrive).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Close task' })).toBeDisabled()
    await act(async () => loadPreview({ task: row, ...view, head_oid: 'a'.repeat(40),
      turns: [], timeline: [], questions: [], files_changed: [] }))
    await waitFor(() => expect(redrive).toBeEnabled())
    expect(screen.getByRole('button', { name: 'Close task' })).toBeEnabled()
    expect(fetch.mock.calls.some(([url]) => String(url).endsWith(`/tasks/${dependency.task_id}`))).toBe(true)
    fireEvent.click(redrive)
    await screen.findByText('Action requested. Waiting for task update.')
    const [url, options] = fetch.mock.calls.find(([, options]) => options?.method === 'POST')!
    expect(url).toBe(`/api/v1/tasks/${row.task_id}/integrate`)
    expect(JSON.parse(options!.body as string)).toMatchObject({ expected: { expected_task_id: row.task_id },
      expected_integration_id: view.integration!.integration_id,
      integration: { task_id: row.task_id, expected: view.integration!.revision } })
  })

  it('shows the skeleton until the first snapshot arrives', () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => new Promise(() => {})),
    )
    render(<App />)

    expect(screen.getByLabelText('Loading the first snapshot')).toBeInTheDocument()
    expect(screen.getByText('Connecting to your Macs…')).toBeInTheDocument()
    expect(screen.getByText(/Waiting for the first snapshot/)).toBeInTheDocument()
  })

  it('keeps the last snapshot on screen and says the API stopped answering', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce({ ok: true, status: 200, json: async () => snapshot() })
      .mockRejectedValue(new Error('connection refused'))
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    render(<App />)

    await waitFor(() => expect(screen.getAllByText('mini-1').length).toBeGreaterThan(0))
    await vi.advanceTimersByTimeAsync(2100)

    await waitFor(() =>
      expect(screen.getByText('The dashboard API stopped answering')).toBeInTheDocument(),
    )
    // The fleet stays readable; only the header and footer say it is not live.
    expect(screen.getAllByText('mini-1').length).toBeGreaterThan(0)
    expect(screen.getByText('Showing last snapshot')).toBeInTheDocument()
    expect(screen.getByText(/Last known snapshot/)).toBeInTheDocument()
  })

  it('shows a one-line restart banner when the laptop binary is outdated', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        json: async () => snapshot({ laptop: { binary_outdated: true } }),
      }),
    )
    render(<App />)

    await waitFor(() =>
      expect(screen.getByText('worker was updated, restart the dashboard')).toBeInTheDocument(),
    )
  })

  it('does not show the restart banner when the laptop binary matches', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => snapshot() }),
    )
    render(<App />)

    await waitFor(() => expect(screen.getAllByText('mini-1').length).toBeGreaterThan(0))
    expect(screen.queryByText('worker was updated, restart the dashboard')).not.toBeInTheDocument()
  })
})

describe('tab title', () => {
  it('leads with the number of tasks waiting on an answer', () => {
    expect(documentTitle(2)).toBe('(2) mac-worker — pool')
  })

  it('drops the count entirely when nothing is waiting', () => {
    expect(documentTitle(0)).toBe('mac-worker — pool')
  })

  it('counts what the pool is actually waiting on', async () => {
    const waiting = snapshot({
      tasks: [
        task({ task_id: 'a'.repeat(32), state: 'open', last_outcome: { kind: 'needs_input' } }),
        task({ task_id: 'b'.repeat(32), state: 'closed', last_outcome: { kind: 'done' } }),
      ],
    })
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => waiting }),
    )
    render(<App />)

    await waitFor(() => expect(document.title).toBe('(1) mac-worker — pool'))
  })
})

describe('worker errors', () => {
  it('shows a worker error object as text instead of crashing (React #31)', async () => {
    const withError = snapshot({
      workers: [
        worker({
          health: 'unavailable',
          error: {
            code: 'SSH_UNAVAILABLE',
            message: 'ssh timed out connecting to mini-1',
          },
        }),
      ],
    })
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => withError }),
    )
    render(<App />)

    await waitFor(() =>
      expect(screen.getAllByText('ssh timed out connecting to mini-1').length).toBeGreaterThan(0),
    )
    expect(screen.getAllByText('SSH_UNAVAILABLE').length).toBeGreaterThan(0)
  })

  it('still shows a legacy string error instead of crashing', async () => {
    const withError = snapshot({
      workers: [worker({ health: 'unavailable' })],
    })
    ;(withError.workers[0] as { error: unknown }).error = 'SSH_UNAVAILABLE'
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => withError }),
    )
    render(<App />)

    await waitFor(() => expect(screen.getAllByText('SSH_UNAVAILABLE').length).toBeGreaterThan(0))
  })
})

describe('deep links', () => {
  it('selects a task from the hash on load and follows back/forward', async () => {
    window.location.hash = `#/tasks/${'a'.repeat(32)}`
    vi.stubGlobal(
      'fetch',
      vi.fn((url: string) => {
        if (String(url).includes('/api/v1/snapshot')) {
          return Promise.resolve({ ok: true, status: 200, json: async () => snapshot() })
        }
        return new Promise(() => {})
      }),
    )
    render(<App />)
    expect(await screen.findByText('Loading task…')).toBeInTheDocument()

    window.location.hash = ''
    window.dispatchEvent(new HashChangeEvent('hashchange'))
    await waitFor(() => expect(screen.queryByText('Loading task…')).not.toBeInTheDocument())
    expect(screen.getByText('Repair login')).toBeInTheDocument()
  })

  it('does not crash when the hash cannot be decoded', async () => {
    window.location.hash = '#/tasks/%'
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => snapshot() }),
    )
    expect(parseTaskHash('#/tasks/%')).toEqual({ status: 'invalid' })
    render(<App />)
    expect(await screen.findByText('That task link is not valid.')).toBeInTheDocument()
    expect(screen.getByText('Repair login')).toBeInTheDocument()
    expect(screen.queryByText('Loading task…')).not.toBeInTheDocument()
  })

  it('rejects a hash that is not a task id', async () => {
    window.location.hash = '#/tasks/not-a-task-id'
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => snapshot() }),
    )
    render(<App />)
    expect(await screen.findByText('That task link is not valid.')).toBeInTheDocument()
    expect(screen.getByText('Repair login')).toBeInTheDocument()
  })
})

describe('shared event stream', () => {
  const taskId = 'a'.repeat(32)

  it('one_source_for_many_hooks', async () => {
    const urls: string[] = []
    vi.stubGlobal(
      'EventSource',
      class {
        constructor(url: string) {
          urls.push(url)
        }
        addEventListener() {}
        removeEventListener() {}
        close() {}
      },
    )
    vi.stubGlobal(
      'fetch',
      vi.fn((url: string) => {
        if (String(url).includes('/api/v1/snapshot')) {
          return Promise.resolve({
            ok: true,
            status: 200,
            json: async () =>
              snapshot({
                tasks: [
                  task({
                    task_id: taskId,
                    state: 'open',
                    review_state: 'waiting_on_you',
                    last_outcome: { kind: 'needs_input' },
                  }),
                ],
              }),
          })
        }
        return Promise.resolve({
          ok: true,
          status: 200,
          json: async () => ({
            task: task({
              task_id: taskId,
              state: 'open',
              review_state: 'waiting_on_you',
              last_outcome: { kind: 'needs_input' },
            }),
            project_id: 'p',
            worktree_id: 'w',
            base_oid: null,
            head_oid: null,
            session_present: false,
            summary: 'Loaded',
            questions: ['What next?'],
            files_changed: [],
            diff_stat: null,
            fetch_command: 'worker task fetch',
            review_state: 'waiting_on_you',
            close_policy: 'done',
            reported_checks: [],
            fetched_head: null,
            fetched_ref: null,
            review_commands: [],
            turns: [],
            timeline: [],
          }),
        })
      }),
    )

    function Many({ children }: { children?: ReactNode }) {
      useSnapshot()
      useAttentionQuestions([taskId])
      useTaskPreviews([
        task({
          task_id: taskId,
          state: 'open',
          review_state: 'waiting_on_you',
          last_outcome: { kind: 'needs_input' },
        }),
      ])
      return (
        <>
          {children}
          <TaskDetail taskId={taskId} />
        </>
      )
    }

    render(
      <ControllerEventsProvider>
        <Many />
      </ControllerEventsProvider>,
    )
    expect(await screen.findByText('Loaded')).toBeInTheDocument()
    expect(urls).toHaveLength(1)
    expect(urls[0]).toContain('/api/v1/events')
  })
})
