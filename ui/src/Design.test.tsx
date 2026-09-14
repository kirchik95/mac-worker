import { act, render, screen, within, fireEvent, cleanup } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import App from './App'
import { Tasks } from '@/views/Tasks'
import { Settings } from '@/views/Settings'
import { agentSettings } from '@/test/fixtures'
import { Overview } from '@/views/Overview'
import { snapshot, task, worker } from '@/test/fixtures'

const waiting = task({
  task_id: 'a'.repeat(32),
  title: 'Choose the follow-up label',
  state: 'open',
  review_state: 'waiting_on_you',
  last_outcome: { kind: 'needs_input' },
})
const review = task({
  task_id: 'b'.repeat(32),
  title: 'Cover the outcome parser',
  state: 'open',
  review_state: 'ready_for_review',
  last_outcome: { kind: 'done' },
})
const data = snapshot({ tasks: [waiting, review] })

beforeEach(() => {
  window.history.replaceState(null, '', '/')
  localStorage.clear()
  vi.stubGlobal(
    'fetch',
    vi.fn(async (path: string) => ({
      ok: true,
      status: 200,
      json: async () =>
        path.includes('/snapshot')
          ? data
          : {
              task: path.includes(waiting.task_id) ? waiting : review,
              questions: path.includes(waiting.task_id) ? ['Use alpha or beta?'] : [],
              files_changed: ['src/task.rs'],
              diff_stat: '1 file changed',
              summary: 'Parser coverage added.',
              project_id: 'p',
              worktree_id: 'w',
              base_oid: null,
              head_oid: null,
              session_present: true,
              fetch_command: 'worker task fetch test',
              review_state: 'waiting_on_you',
              close_policy: 'never',
              reported_checks: [],
              fetched_head: null,
              fetched_ref: null,
              review_commands: [],
              turns: [],
              timeline: [],
            },
    })),
  )
})
afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
})

describe('Paper dashboard workflow', () => {
  it('puts named questions and review actions before the Macs and opens their task', async () => {
    const onSelectTask = vi.fn()
    render(<Overview snapshot={data} onSelectTask={onSelectTask} />)
    const question = await screen.findByText('Use alpha or beta?')
    const card = question.closest('article')!
    fireEvent.click(within(card).getByRole('button', { name: 'Answer' }))
    expect(onSelectTask).toHaveBeenCalledWith(waiting.task_id)
    const reviewButton = screen.getByRole('button', { name: 'Review changes' })
    fireEvent.click(reviewButton)
    expect(onSelectTask).toHaveBeenLastCalledWith(review.task_id)
    expect(
      question.compareDocumentPosition(screen.getByText('Your Macs')) &
        Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy()
  })

  it('does not advertise free slots from an unavailable worker', () => {
    render(<Overview snapshot={snapshot({ workers: [worker({ health: 'unavailable' })] })} />)
    expect(screen.getByText(/Offline.*capacity unknown/)).toBeInTheDocument()
    expect(screen.queryByText(/slots free/)).not.toBeInTheDocument()
  })

  it('reading notifications preserves unresolved tasks and makes no mutations', async () => {
    render(<App />)
    const trigger = await screen.findByRole('button', { name: /Notifications/ })
    fireEvent.click(trigger)
    const drawer = await screen.findByRole('dialog', { name: 'Notifications' })
    fireEvent.click(within(drawer).getByRole('button', { name: 'Mark all as read' }))
    expect(within(drawer).getByText('Choose the follow-up label')).toBeInTheDocument()
    expect(within(drawer).getByText('Cover the outcome parser')).toBeInTheDocument()
    expect(
      vi.mocked(fetch).mock.calls.every(([, init]) => !init?.method || init.method === 'GET'),
    ).toBe(true)
  })
})

it('opens the requested run and allows clearing its filter', () => {
  render(
    <Tasks
      snapshot={snapshot({
        tasks: [
          task({ title: 'Inside run', run_id: 'run-a' }),
          task({ task_id: 'e'.repeat(32), title: 'Outside run' }),
        ],
      })}
      initialRun="run-a"
      onSelect={() => {}}
    />,
  )
  expect(screen.getByText('Inside run')).toBeInTheDocument()
  expect(screen.queryByText('Outside run')).not.toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: 'Clear filters' }))
  expect(screen.getByText('Outside run')).toBeInTheDocument()
})
it('opens setup for the requested Mac and agent', async () => {
  vi.stubGlobal(
    'fetch',
    vi.fn(async () => ({ ok: true, status: 200, json: async () => agentSettings() })),
  )
  render(
    <Settings
      snapshot={snapshot({ workers: [worker(), worker({ name: 'mini-2' })] })}
      initialWorker="mini-2"
      initialAgent="opencode"
    />,
  )
  expect(await screen.findByRole('heading', { name: 'OpenCode on mini-2' })).toBeInTheDocument()
  expect(fetch).toHaveBeenCalledWith('/api/v1/workers/mini-2/agent-settings', expect.anything())
})

it('returns from a task to its filtered run', async () => {
  window.history.replaceState(null, '', '/#/tasks?run=run-a')
  vi.stubGlobal(
    'fetch',
    vi.fn(async (path: string) => ({
      ok: true,
      status: 200,
      json: async () =>
        path.includes('/snapshot')
          ? snapshot({
              tasks: [
                { ...waiting, run_id: 'run-a' },
                { ...review, run_id: 'run-b' },
              ],
            })
          : {
              task: waiting,
              questions: ['Pick a label'],
              files_changed: [],
              diff_stat: null,
              summary: 'Choose a label',
              project_id: 'p',
              worktree_id: 'w',
              base_oid: null,
              head_oid: null,
              session_present: true,
              fetch_command: 'worker task fetch test',
              review_state: 'waiting_on_you',
              close_policy: 'never',
              reported_checks: [],
              fetched_head: null,
              fetched_ref: null,
              review_commands: [],
              turns: [],
              timeline: [],
            },
    })),
  )
  render(<App />)
  const navigation = new Promise<Event>((resolve) =>
    window.addEventListener('hashchange', resolve, { once: true }),
  )
  fireEvent.click(await screen.findByRole('button', { name: 'Answer' }))
  await act(async () => {
    await navigation
  })
  await screen.findByRole('heading', { name: waiting.title })
  fireEvent.click(
    within(screen.getByRole('navigation', { name: 'Breadcrumb' })).getByRole('button', {
      name: 'Tasks',
    }),
  )
  expect(screen.queryByText(review.title)).not.toBeInTheDocument()
})

it.each([
  'NO_WORKER_OFFERS:darwin-arm64,agent:codex',
  'CAPABILITY_MISSING:darwin-arm64,agent:codex',
])('offers setup for the live missing-agent code %s', (blocking_code) => {
  const setup = vi.fn()
  render(
    <Tasks
      snapshot={snapshot({
        tasks: [task({ state: 'queued', worker: 'mini-2', agent: 'codex', blocking_code })],
      })}
      onSelect={() => {}}
      onSetup={setup}
    />,
  )
  fireEvent.click(screen.getByRole('button', { name: 'Setup instructions' }))
  expect(setup).toHaveBeenCalledWith('mini-2', 'codex')
})
