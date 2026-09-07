import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'

import { MALICIOUS, snapshot, task } from '@/test/fixtures'
import { Tasks } from './Tasks'

const fixture = snapshot({
  tasks: [
    task({ task_id: 'a'.repeat(32), title: 'Repair login', agent: 'codex', worker: 'mini-1', state: 'active' }),
    task({ task_id: 'b'.repeat(32), title: 'Extract billing', agent: 'cursor', worker: 'mini-2', state: 'closed' }),
    task({ task_id: 'c'.repeat(32), title: MALICIOUS, agent: 'codex', worker: null, state: 'abandoned' }),
  ],
})

// The first cell carries the state marks; the title is the second.
const titles = () =>
  screen
    .getAllByRole('row')
    .slice(1)
    .map((row) => within(row).getAllByRole('cell')[1].textContent ?? '')

describe('Tasks', () => {
  it('renders a hostile title as text and never as markup', () => {
    render(<Tasks snapshot={fixture} onSelect={() => {}} />)
    expect(screen.getByText(MALICIOUS)).toBeInTheDocument()
    expect(document.querySelector('img')).toBeNull()
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

  it('says so when nothing matches instead of showing an empty table', async () => {
    const user = userEvent.setup()
    render(<Tasks snapshot={fixture} onSelect={() => {}} />)
    await user.type(screen.getByLabelText('Filter tasks'), 'nothing matches this')
    expect(screen.getByText('No task matches these filters.')).toBeInTheDocument()
  })

  it('filters by outcome and offers the same view as a command', async () => {
    const user = userEvent.setup()
    const outcomes = snapshot({
      tasks: [
        task({ task_id: 'a'.repeat(32), title: 'Repair login', last_outcome: { kind: 'done' } }),
        task({ task_id: 'b'.repeat(32), title: 'Extract billing', last_outcome: { kind: 'blocked' } }),
      ],
    })
    render(<Tasks snapshot={outcomes} onSelect={() => {}} />)

    await user.click(screen.getByRole('button', { name: /blocked/ }))
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

    await user.click(screen.getAllByRole('button', { name: /needs input/ })[0])
    expect(screen.getByText('worker task list --outcome needs-input')).toBeInTheDocument()
    vi.unstubAllGlobals()
  })

  it('shows the question, the answers the agent will take, and how to reply', async () => {
    const waiting = snapshot({
      tasks: [
        task({
          task_id: 'd'.repeat(32),
          title: 'Give the dashboard a queue projection',
          state: 'open',
          turn_count: 3,
          last_outcome: { kind: 'needs_input' },
        }),
      ],
    })
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        json: async () => ({
          questions: [{ text: 'Keep dispatched entries?', options: ['keep them', 'drop them'] }],
        }),
      }),
    )
    render(<Tasks snapshot={waiting} onSelect={() => {}} />)

    expect(screen.getByText('Waiting on you · 1')).toBeInTheDocument()
    expect(await screen.findByText('Keep dispatched entries?')).toBeInTheDocument()
    expect(screen.getByText('keep them')).toBeInTheDocument()
    expect(screen.getByText('drop them')).toBeInTheDocument()
    expect(
      screen.getByText(`worker task say ${'d'.repeat(32)} --message-file reply.md`),
    ).toBeInTheDocument()
    expect(screen.getByText('your answer starts turn 4')).toBeInTheDocument()
    vi.unstubAllGlobals()
  })

  it('says a question carries no options rather than leaving the row bare', async () => {
    const waiting = snapshot({
      tasks: [task({ state: 'open', last_outcome: { kind: 'needs_input' } })],
    })
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        json: async () => ({ questions: ['Which base should it use?'] }),
      }),
    )
    render(<Tasks snapshot={waiting} onSelect={() => {}} />)

    expect(await screen.findByText('Which base should it use?')).toBeInTheDocument()
    expect(
      screen.getByText('the agent offered no options — this one needs a written answer'),
    ).toBeInTheDocument()
    vi.unstubAllGlobals()
  })

  it('opens the task the operator clicked', async () => {
    const onSelect = vi.fn()
    const user = userEvent.setup()
    render(<Tasks snapshot={fixture} onSelect={onSelect} />)
    await user.click(screen.getByText('Repair login'))
    expect(onSelect).toHaveBeenCalledWith('a'.repeat(32))
  })
})
