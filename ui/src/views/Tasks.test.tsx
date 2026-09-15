import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { MALICIOUS, originDelivery, snapshot, task } from '@/test/fixtures'
import { Tasks } from './Tasks'

const fixture = snapshot({
  tasks: [
    task({
      task_id: 'a'.repeat(32),
      title: 'Repair login',
      agent: 'codex',
      worker: 'mini-1',
      state: 'active',
    }),
    task({
      task_id: 'b'.repeat(32),
      title: 'Extract billing',
      agent: 'cursor',
      worker: 'mini-2',
      state: 'closed',
    }),
    task({
      task_id: 'c'.repeat(32),
      title: MALICIOUS,
      agent: 'codex',
      worker: null,
      state: 'abandoned',
    }),
  ],
})

// The first cell carries the state marks; the title is the second.
const titles = () =>
  screen
    .getAllByRole('row')
    .slice(1)
    .map((row) => within(row).getAllByRole('cell')[1].textContent ?? '')

afterEach(() => vi.unstubAllGlobals())

describe('Tasks', () => {
  it('renders a hostile title as text and never as markup', () => {
    render(<Tasks snapshot={fixture} onSelect={() => {}} />)
    expect(screen.getByText(MALICIOUS)).toBeInTheDocument()
    expect(document.querySelector('img')).toBeNull()
  })

  it('shows a delivery chip on the task row', () => {
    render(
      <Tasks
        snapshot={snapshot({
          tasks: [
            task({
              title: 'Push the result',
              delivery: originDelivery({ state: 'retrying' }),
              deliveries: [originDelivery({ state: 'retrying' })],
            }),
          ],
        })}
        onSelect={() => {}}
      />,
    )
    expect(screen.getByText('Push the result')).toBeInTheDocument()
    expect(screen.getByText('retrying')).toBeInTheDocument()
  })

  it('filters locally without issuing a request', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)
    const user = userEvent.setup()
    render(<Tasks snapshot={fixture} onSelect={() => {}} />)

    await user.type(screen.getByLabelText('Filter tasks'), 'billing')
    expect(titles().some((title) => title.includes('Extract billing'))).toBe(true)
    expect(titles()).toHaveLength(1)
    expect(fetchMock).not.toHaveBeenCalled()
    vi.unstubAllGlobals()
  })

  it('matches on the task id as well as the title', async () => {
    const user = userEvent.setup()
    render(<Tasks snapshot={fixture} onSelect={() => {}} />)
    await user.type(screen.getByLabelText('Filter tasks'), 'bbbbbbbb')
    expect(titles()).toHaveLength(1)
  })

  it('reports the filtered count against the total', async () => {
    const user = userEvent.setup()
    render(<Tasks snapshot={fixture} onSelect={() => {}} />)
    expect(screen.getByText('3 of 3')).toBeInTheDocument()
    await user.type(screen.getByLabelText('Filter tasks'), 'login')
    expect(screen.getByText('1 of 3')).toBeInTheDocument()
  })

  it('shows the query and retains table headings when nothing matches', async () => {
    const user = userEvent.setup()
    render(<Tasks snapshot={fixture} onSelect={() => {}} />)
    await user.type(screen.getByLabelText('Filter tasks'), 'nothing matches this')
    expect(screen.getByText('No tasks match “nothing matches this”')).toBeInTheDocument()
    expect(screen.getByRole('columnheader', { name: 'Action' })).toBeInTheDocument()
  })

  it('filters by outcome and offers the same view as a command', async () => {
    const user = userEvent.setup()
    const outcomes = snapshot({
      tasks: [
        task({ task_id: 'a'.repeat(32), title: 'Repair login', last_outcome: { kind: 'done' } }),
        task({
          task_id: 'b'.repeat(32),
          title: 'Extract billing',
          last_outcome: { kind: 'blocked' },
        }),
      ],
    })
    render(<Tasks snapshot={outcomes} onSelect={() => {}} />)

    await user.click(screen.getByRole('combobox', { name: 'Outcome' }))
    await user.click(await screen.findByRole('option', { name: 'Blocked' }))
    await user.click(screen.getByText('CLI equivalent'))
    expect(titles().some((title) => title.includes('Extract billing'))).toBe(true)
    expect(titles()).toHaveLength(1)
    expect(screen.getByText('worker task list --outcome blocked')).toBeInTheDocument()
    expect(screen.getByText('1 of 2 tasks do not match')).toBeInTheDocument()
  })

  it('spells a multi-word outcome the way the flag takes it', async () => {
    const user = userEvent.setup()
    const waiting = snapshot({
      tasks: [task({ last_outcome: { kind: 'needs_input' }, state: 'open' })],
    })
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({ questions: [] }) }),
    )
    render(<Tasks snapshot={waiting} onSelect={() => {}} />)

    await user.click(screen.getByRole('combobox', { name: 'Outcome' }))
    await user.click(await screen.findByRole('option', { name: 'Needs input' }))
    await user.click(screen.getByText('CLI equivalent'))
    expect(screen.getByText('worker task list --outcome needs-input')).toBeInTheDocument()
    vi.unstubAllGlobals()
  })

  it('links a named question to the right task', async () => {
    const onSelect = vi.fn()
    const user = userEvent.setup()
    const waiting = snapshot({
      tasks: [
        task({
          task_id: 'd'.repeat(32),
          title: 'Give the dashboard a queue projection',
          state: 'open',
          last_outcome: { kind: 'needs_input' },
        }),
      ],
    })
    render(<Tasks snapshot={waiting} onSelect={onSelect} />)
    expect(screen.getByText('1 task needs your answer')).toBeInTheDocument()
    await user.click(
      screen.getAllByRole('button', { name: 'Give the dashboard a queue projection' })[0],
    )
    expect(onSelect).toHaveBeenCalledWith('d'.repeat(32))
  })

  it('keeps unanswered questions reachable when the search has no matches', async () => {
    const user = userEvent.setup()
    render(
      <Tasks
        snapshot={snapshot({
          tasks: [
            task({ title: 'Waiting task', state: 'open', last_outcome: { kind: 'needs_input' } }),
            task({
              task_id: 'b'.repeat(32),
              title: 'Review task',
              state: 'open',
              review_state: 'ready_for_review',
            }),
          ],
        })}
        onSelect={() => {}}
      />,
    )
    await user.type(screen.getByLabelText('Filter tasks'), 'different')
    expect(screen.getByText('1 task needs your answer')).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: 'Show questions' }))
    expect(screen.getByLabelText('Filter tasks')).toHaveValue('')
    expect(screen.getByRole('button', { name: 'Waiting task' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Answer' })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Review task' })).not.toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: 'Show all tasks' }))
    expect(screen.getByRole('button', { name: 'Review task' })).toBeInTheDocument()
  })

  it('opens the task the operator clicked', async () => {
    const onSelect = vi.fn()
    const user = userEvent.setup()
    render(<Tasks snapshot={fixture} onSelect={onSelect} />)
    await user.click(screen.getByText('Repair login'))
    expect(onSelect).toHaveBeenCalledWith('a'.repeat(32))
  })

  it('does not keep a closed question in the answer shortcuts', () => {
    render(
      <Tasks
        snapshot={snapshot({
          tasks: [task({ state: 'closed', last_outcome: { kind: 'needs_input' } })],
        })}
        onSelect={() => {}}
      />,
    )
    expect(screen.queryByText(/task needs your answer/)).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Answer' })).not.toBeInTheDocument()
  })
})
