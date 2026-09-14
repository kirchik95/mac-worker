import { fireEvent, render, screen, within, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, expect, it, vi } from 'vitest'
import { Notifications } from './Notifications'
import { snapshot, task } from '@/test/fixtures'

beforeEach(() => localStorage.clear())
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
