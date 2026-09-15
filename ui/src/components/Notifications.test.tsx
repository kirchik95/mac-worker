import { act, fireEvent, render, screen, within, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, expect, it, vi } from 'vitest'
import { Notifications } from './Notifications'
import { snapshot, task } from '@/test/fixtures'
import type { Snapshot } from '@/lib/api'

const { announce } = vi.hoisted(() => ({ announce: Object.assign(vi.fn(), { dismiss: vi.fn() }) }))
vi.mock('sonner', () => ({ toast: announce }))
beforeEach(() => {
  localStorage.clear()
  vi.clearAllMocks()
})
const waiting = task({
  state: 'open',
  review_state: 'waiting_on_you',
  last_outcome: { kind: 'needs_input' },
  updated_at_millis: 1000,
})
const data = snapshot({ tasks: [waiting] })
it('persists read state and treats a newer task revision as unread', async () => {
  const first = render(<Notifications snapshot={data} previews={{}} onSelectTask={() => {}} />)
  fireEvent.click(screen.getByRole('button', { name: 'Notifications, 1 unread' }))
  fireEvent.click(screen.getByRole('button', { name: 'Mark all as read' }))
  fireEvent.click(screen.getByRole('button', { name: 'Unread 0' }))
  expect(screen.getByText('All caught up')).toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: 'View all notifications' }))
  expect(within(screen.getByRole('dialog')).getByText(waiting.title)).toBeInTheDocument()
  first.unmount()
  const second = render(<Notifications snapshot={data} previews={{}} onSelectTask={() => {}} />)
  expect(screen.getByRole('button', { name: 'Notifications, 0 unread' })).toBeInTheDocument()
  second.rerender(
    <Notifications
      snapshot={snapshot({ tasks: [{ ...waiting, turn_count: 2, updated_at_millis: 2000 }] })}
      previews={{}}
      onSelectTask={() => {}}
    />,
  )
  expect(screen.getByRole('button', { name: 'Notifications, 1 unread' })).toBeInTheDocument()
})
it('closes with Escape and restores focus to the bell', async () => {
  const user = userEvent.setup()
  render(<Notifications snapshot={data} previews={{}} onSelectTask={vi.fn()} />)
  const bell = screen.getByRole('button', { name: 'Notifications, 1 unread' })
  await user.click(bell)
  expect(screen.getByRole('dialog', { name: 'Notifications' })).toBeInTheDocument()
  await user.keyboard('{Escape}')
  await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
  expect(bell).toHaveFocus()
})

it('silently loads existing questions, then announces each new turn only once', () => {
  const select = vi.fn()
  const view = (value: Snapshot | null) => (
    <Notifications snapshot={value} previews={{}} onSelectTask={select} />
  )
  const { rerender } = render(view(null))
  rerender(view(data))
  rerender(view(snapshot({ tasks: [{ ...waiting, updated_at_millis: 2000 }] })))
  expect(announce).not.toHaveBeenCalled()

  const next = { ...waiting, turn_count: 2, updated_at_millis: 3000 }
  rerender(view(snapshot({ tasks: [next] })))
  expect(announce).toHaveBeenCalledTimes(1)
  expect(announce).toHaveBeenLastCalledWith(
    'Task needs your answer',
    expect.objectContaining({
      description: waiting.title,
      action: expect.objectContaining({ label: 'Answer' }),
    }),
  )
  announce.mock.calls[0][1].action.onClick()
  expect(select).toHaveBeenCalledWith(waiting.task_id)
  rerender(view(snapshot({ tasks: [{ ...next, updated_at_millis: 4000 }] })))
  expect(announce).toHaveBeenCalledTimes(1)
})

it('announces a new review result and dismisses it when it is no longer actionable', () => {
  const select = vi.fn()
  const view = (tasks: Snapshot['tasks']) => (
    <Notifications snapshot={snapshot({ tasks })} previews={{}} onSelectTask={select} />
  )
  const { rerender } = render(view([task()]))
  const review = task({ state: 'open', review_state: 'ready_for_review' })
  rerender(view([review]))
  expect(announce).toHaveBeenCalledWith(
    'Ready for review',
    expect.objectContaining({
      action: expect.objectContaining({ label: 'Review changes' }),
    }),
  )
  announce.mock.calls[0][1].action.onClick()
  expect(select).toHaveBeenCalledWith(review.task_id)
  announce.dismiss.mockClear()
  rerender(view([{ ...review, state: 'closed' }]))
  expect(announce.dismiss).toHaveBeenCalled()
})

it('groups simultaneous arrivals into one notification with an action to open the center', () => {
  const view = (tasks: Snapshot['tasks']) => (
    <Notifications snapshot={snapshot({ tasks })} previews={{}} onSelectTask={vi.fn()} />
  )
  const { rerender } = render(view([]))
  rerender(
    view([
      waiting,
      task({ task_id: 'b'.repeat(32), state: 'open', review_state: 'ready_for_review' }),
    ]),
  )
  expect(announce).toHaveBeenCalledTimes(1)
  expect(announce).toHaveBeenCalledWith(
    '2 tasks need your attention',
    expect.objectContaining({
      action: expect.objectContaining({ label: 'View notifications' }),
    }),
  )
  act(() => announce.mock.calls[0][1].action.onClick())
  expect(screen.getByRole('dialog')).toBeInTheDocument()
})

it('suppresses stale and offline arrivals and seeds recovery without a backlog', () => {
  const view = (value: Snapshot, offline = false) => (
    <Notifications snapshot={value} offline={offline} previews={{}} onSelectTask={vi.fn()} />
  )
  const { rerender } = render(view(snapshot()))
  rerender(view(snapshot({ tasks: [waiting], collection: { freshness: 'stale', errors: [] } })))
  rerender(view(data))
  rerender(view(snapshot({ tasks: [{ ...waiting, turn_count: 2 }] }), true))
  rerender(view(snapshot({ tasks: [{ ...waiting, turn_count: 3 }] })))
  expect(announce).not.toHaveBeenCalled()
  rerender(view(snapshot({ tasks: [{ ...waiting, turn_count: 4 }] })))
  expect(announce).toHaveBeenCalledTimes(1)
})

it('does not announce a stale task when its observation becomes current', () => {
  const view = (tasks: Snapshot['tasks']) => (
    <Notifications snapshot={snapshot({ tasks })} previews={{}} onSelectTask={vi.fn()} />
  )
  const { rerender } = render(view([]))
  rerender(view([{ ...waiting, freshness: 'stale' }]))
  rerender(view([waiting]))
  expect(announce).not.toHaveBeenCalled()
})

it.each([task({ freshness: 'stale' }), { ...waiting, freshness: 'stale' as const }])(
  'silently recovers an individual task even when its actionable turn changed',
  (staleTask) => {
    const view = (tasks: Snapshot['tasks']) => (
      <Notifications snapshot={snapshot({ tasks })} previews={{}} onSelectTask={vi.fn()} />
    )
    const { rerender } = render(view([task()]))
    rerender(view([staleTask]))
    const recovered = task({ state: 'open', review_state: 'ready_for_review', turn_count: 2 })
    rerender(view([recovered]))
    expect(announce).not.toHaveBeenCalled()
    rerender(view([{ ...recovered, updated_at_millis: 4000 }]))
    expect(announce).not.toHaveBeenCalled()
    rerender(view([{ ...recovered, turn_count: 3 }]))
    expect(announce).toHaveBeenCalledTimes(1)
  },
)

it('does not toast the task already on screen or repeat it after navigating away', () => {
  const view = (value: Snapshot, activeTaskId?: string) => (
    <Notifications
      snapshot={value}
      activeTaskId={activeTaskId}
      previews={{}}
      onSelectTask={vi.fn()}
    />
  )
  const { rerender } = render(view(snapshot(), waiting.task_id))
  rerender(view(data, waiting.task_id))
  rerender(view(data))
  expect(announce).not.toHaveBeenCalled()
})
