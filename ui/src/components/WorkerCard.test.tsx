import { act, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { Worker } from '@/lib/api'
import { worker } from '@/test/fixtures'
import { WorkerCard } from './WorkerCard'

const reportedAgents: NonNullable<Worker['agent_facts']> = {
  collected_at_millis: 1_000,
  freshness: 'current',
  agents: [
    { name: 'claude', version: '2.1', auth: 'authenticated', auth_by_profile: [] },
    { name: 'codex', version: '0.153', auth: 'authenticated', auth_by_profile: [] },
    { name: 'cursor', version: null, auth: 'unknown', auth_by_profile: [] },
    { name: 'opencode', version: '1.2', auth: 'unauthenticated', auth_by_profile: [] },
  ],
}
const working = worker({
  health: 'busy',
  agent_facts: reportedAgents,
  slot: {
    state: 'busy', capacity: 2, busy: 1, active_job_id: 'turn-1', active_job_ids: ['turn-1'],
  },
  active_task: {
    task_id: 'task-1', title: 'Repair login', agent: 'codex', model: null,
    effort: null, turn_number: 2,
  },
})

const originalAnimate = Object.getOwnPropertyDescriptor(Element.prototype, 'animate')
let animate: ReturnType<typeof vi.fn>
let cancelAnimation: ReturnType<typeof vi.fn>
let reducedMotion = false

beforeEach(() => {
  reducedMotion = false
  vi.stubGlobal('matchMedia', vi.fn((query: string) => ({
    matches: reducedMotion && query === '(prefers-reduced-motion: reduce)',
    media: query, onchange: null,
    addEventListener: vi.fn(), removeEventListener: vi.fn(),
    addListener: vi.fn(), removeListener: vi.fn(), dispatchEvent: vi.fn(),
  })))
  cancelAnimation = vi.fn()
  animate = vi.fn(() => ({ cancel: cancelAnimation }))
  Object.defineProperty(Element.prototype, 'animate', { configurable: true, value: animate })
})

afterEach(() => {
  if (originalAnimate) Object.defineProperty(Element.prototype, 'animate', originalAnimate)
  else delete (Element.prototype as Partial<Element>).animate
  vi.unstubAllGlobals()
  delete document.documentElement.dataset.inputModality
})

describe('worker agent logos', () => {
  it('shows every reported agent, including sign-in needed, and opens its settings', async () => {
    const user = userEvent.setup()
    const setup = vi.fn()
    render(<WorkerCard worker={working} now={3_000} onSetup={setup} />)

    expect(screen.getByText('Agents:')).toBeInTheDocument()
    for (const name of ['Claude Code', 'Codex', 'Cursor', 'OpenCode']) {
      const icon = screen.getByRole('button', { name: `${name} settings for mini-1` })
      expect(icon.querySelector('svg')).not.toBeNull()
      expect(icon).not.toHaveTextContent('CC')
      expect(icon).toHaveAttribute('data-tone', name === 'OpenCode' ? 'sign-in-needed' : 'default')
    }
    expect(screen.queryByText(/Installed:/)).not.toBeInTheDocument()
    expect(screen.getByText('OpenCode needs sign-in on Mac')).toBeVisible()
    await user.click(screen.getByRole('button', { name: 'OpenCode settings for mini-1' }))
    expect(setup).toHaveBeenLastCalledWith('mini-1', 'opencode')
    await user.click(screen.getByRole('button', { name: 'Setup instructions' }))
    expect(setup).toHaveBeenLastCalledWith('mini-1', 'opencode')
    expect(screen.getByText('codex · turn 2')).toBeVisible()
  })

  it.each([
    ['idle with a cached task', { slot: { ...working.slot, state: 'idle', busy: 0 } }],
    ['stale', { freshness: 'stale' }],
    ['offline', { health: 'unavailable' }],
    ['unknown slots', { slot: { ...working.slot, state: 'unknown' } }],
    ['occupied without a known task', { active_task: null }],
  ] satisfies [string, Partial<Worker>][])('does not claim current activity when %s', async (_, overrides) => {
    const user = userEvent.setup()
    render(<WorkerCard worker={{ ...working, ...overrides }} now={3_000} onSetup={vi.fn()} />)
    const codex = screen.getByRole('button', { name: 'Codex settings for mini-1' })
    for (let i = 0; i < 6 && document.activeElement !== codex; i++) await user.tab()
    expect(await screen.findByRole('tooltip')).not.toHaveTextContent('Running current task')
    expect(codex).toHaveAttribute('data-tone', 'default')
  })

  it.each(['stale', 'unknown'] as const)('does not gray an agent based on %s authentication facts', (freshness) => {
    render(<WorkerCard worker={{ ...working, agent_facts: { ...reportedAgents, freshness } }} now={3_000} onSetup={vi.fn()} />)
    expect(screen.getByRole('button', { name: 'OpenCode settings for mini-1' }))
      .toHaveAttribute('data-tone', 'default')
  })

  it('reveals name, sign-in and current activity on keyboard focus and dismisses with Escape', async () => {
    const user = userEvent.setup()
    const setup = vi.fn()
    render(<WorkerCard worker={working} now={3_000} onSetup={setup} />)
    const claude = screen.getByRole('button', { name: 'Claude Code settings for mini-1' })
    for (let i = 0; i < 5 && document.activeElement !== claude; i++) await user.tab()
    expect(claude).toHaveFocus()
    const firstTooltip = await screen.findByRole('tooltip')
    expect(firstTooltip).toHaveTextContent('Claude Code')
    expect(firstTooltip).toHaveTextContent('Signed in')
    expect(firstTooltip).toHaveTextContent('No reported active task')
    expect(claude).toHaveAccessibleDescription(/Claude Code Signed in No reported active task/)

    await user.tab()
    const codex = screen.getByRole('button', { name: 'Codex settings for mini-1' })
    expect(codex).toHaveFocus()
    await waitFor(() => expect(screen.getByRole('tooltip')).toHaveTextContent('Running current task'))
    expect(screen.getByRole('tooltip')).toHaveTextContent('turn 2')
    await user.keyboard('{Escape}')
    await waitFor(() => expect(screen.queryByRole('tooltip')).not.toBeInTheDocument())
    expect(codex).toHaveFocus()
    await user.keyboard('{Enter}')
    expect(setup).toHaveBeenCalledWith('mini-1', 'codex')
  })

  it('does not describe stale sign-in or task observations as current', async () => {
    const user = userEvent.setup()
    render(<WorkerCard worker={{ ...working, freshness: 'stale' }} now={3_000} onSetup={vi.fn()} />)
    const claude = screen.getByRole('button', { name: 'Claude Code settings for mini-1' })
    for (let i = 0; i < 5 && document.activeElement !== claude; i++) await user.tab()
    const tooltip = await screen.findByRole('tooltip')
    expect(tooltip).toHaveTextContent('Sign-in unverified')
    expect(tooltip).toHaveTextContent('Activity unverified')
    expect(tooltip).not.toHaveTextContent('Running current task')
    expect(screen.getByRole('button', { name: 'OpenCode settings for mini-1' }))
      .toHaveAttribute('data-tone', 'default')
  })

  it('preserves the absence of reported facts without inventing agent logos', () => {
    render(<WorkerCard worker={worker({ agent_facts: null })} now={3_000} />)
    expect(screen.getByText('Agent status not reported')).toBeVisible()
    expect(screen.queryByText('Agents:')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Codex settings for mini-1' })).not.toBeInTheDocument()
  })
})

describe('worker observation indicator', () => {
  it('moves the report time into a focusable informational tooltip that updates with time', async () => {
    const user = userEvent.setup()
    const { rerender } = render(<WorkerCard worker={worker()} now={3_000} />)
    const indicator = screen.getByRole('img', { name: 'Worker checked 2s ago' })
    expect(indicator).toHaveAttribute('tabindex', '0')
    expect(screen.queryByRole('button', { name: /Worker checked/ })).not.toBeInTheDocument()
    expect(screen.queryByText('Worker checked 2s ago')).not.toBeInTheDocument()
    await user.tab()
    await user.tab()
    expect(indicator).toHaveFocus()
    expect(await screen.findByRole('tooltip')).toHaveTextContent('Worker checked 2s ago')
    rerender(<WorkerCard worker={worker()} now={5_000} />)
    expect(screen.getByRole('img', { name: 'Worker checked 4s ago' })).toHaveFocus()
    expect(screen.getByRole('tooltip')).toHaveTextContent('Worker checked 4s ago')
    expect(animate).not.toHaveBeenCalled()
  })

  it('makes the report tooltip available by touch', async () => {
    const user = userEvent.setup()
    render(<WorkerCard worker={worker()} now={3_000} />)
    const indicator = screen.getByRole('img', { name: 'Worker checked 2s ago' })
    await user.pointer({ keys: '[TouchA]', target: indicator })
    expect(await screen.findByRole('tooltip')).toHaveTextContent('Worker checked 2s ago')
    await user.click(screen.getByRole('heading', { name: 'mini-1' }))
    await waitFor(() => expect(screen.queryByRole('tooltip')).not.toBeInTheDocument())
  })

  it('keeps unknown report times explicit', () => {
    render(<WorkerCard worker={worker({ observed_at_millis: null, freshness: 'unknown' })} now={3_000} />)
    expect(screen.getByRole('img', { name: 'Worker has not reported yet' })).toBeInTheDocument()
  })

  it('animates once per newer current report, never for mount, clock ticks, duplicates or stale reports', () => {
    const view = (observedAt: number, freshness: Worker['freshness'] = 'current', now = 10_000) => (
      <WorkerCard worker={worker({ observed_at_millis: observedAt, freshness })} now={now} />
    )
    const { rerender, unmount } = render(view(1_000))
    rerender(view(1_000, 'current', 11_000))
    expect(animate).not.toHaveBeenCalled()
    rerender(view(2_000))
    expect(animate).toHaveBeenCalledTimes(1)
    const [frames, options] = animate.mock.calls[0]
    expect(frames).toEqual([{ transform: 'rotate(0deg)' }, { transform: 'rotate(180deg)' }])
    expect(options.duration).toBeGreaterThan(0)
    expect(options.duration).toBeLessThanOrEqual(300)
    expect(options.iterations ?? 1).toBe(1)
    rerender(view(2_000))
    rerender(view(900))
    rerender(view(2_000))
    rerender(view(3_000, 'stale'))
    rerender(view(3_000))
    expect(animate).toHaveBeenCalledTimes(1)
    rerender(view(4_000))
    expect(animate).toHaveBeenCalledTimes(2)
    unmount()
    expect(cancelAnimation).toHaveBeenCalled()
  })

  it.each(['reduced motion', 'keyboard modality'])('skips report animation for %s', (mode) => {
    reducedMotion = mode === 'reduced motion'
    if (mode === 'keyboard modality') document.documentElement.dataset.inputModality = 'keyboard'
    const { rerender } = render(<WorkerCard worker={worker()} now={3_000} />)
    act(() => rerender(<WorkerCard worker={worker({ observed_at_millis: 2_000 })} now={3_000} />))
    expect(animate).not.toHaveBeenCalled()
  })
})
