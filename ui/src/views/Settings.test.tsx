import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { agentSettings, snapshot } from '@/test/fixtures'
import { Settings } from './Settings'

function mockFetch(save: (body: unknown) => { ok: boolean; status: number; payload: unknown }) {
  const calls: unknown[] = []
  vi.stubGlobal(
    'fetch',
    vi.fn(async (_url: string, init?: RequestInit) => {
      if (init?.method === 'POST') {
        const body: unknown = JSON.parse(String(init.body))
        calls.push(body)
        const result = save(body)
        return { ok: result.ok, status: result.status, json: async () => result.payload }
      }
      return { ok: true, status: 200, json: async () => agentSettings() }
    }),
  )
  return calls
}

const codexPanel = async () => {
  const heading = await screen.findByText('mini-1 / codex')
  return heading.closest('section') as HTMLElement
}

afterEach(() => vi.unstubAllGlobals())

describe('Settings', () => {
  it('shows the recorded defaults for each agent', async () => {
    mockFetch(() => ({ ok: true, status: 200, payload: agentSettings() }))
    render(<Settings snapshot={snapshot()} />)

    const codex = await codexPanel()
    expect(within(codex).getByText('GPT-6-Astra')).toBeInTheDocument()
    expect(within(codex).getByText('Xhigh')).toBeInTheDocument()
  })

  it('says when the selected agent publishes no global effort setting', async () => {
    mockFetch(() => ({ ok: true, status: 200, payload: agentSettings() }))
    const user = userEvent.setup()
    render(<Settings snapshot={snapshot()} />)

    await user.click(await screen.findByText('OpenCode'))
    expect(
      await screen.findByText('opencode publishes no global effort setting.'),
    ).toBeInTheDocument()
  })

  it('reports each agent connection from the cached facts', async () => {
    mockFetch(() => ({ ok: true, status: 200, payload: agentSettings() }))
    render(<Settings snapshot={snapshot()} />)

    // codex is authenticated in the fixture; opencode is absent from the facts.
    const table = await screen.findByRole('table')
    expect(within(table).getByText('Connected')).toBeInTheDocument()
    expect(within(table).getByText('Not configured')).toBeInTheDocument()
  })

  it('keeps Save and Cancel inert until the draft differs', async () => {
    mockFetch(() => ({ ok: true, status: 200, payload: agentSettings() }))
    render(<Settings snapshot={snapshot()} />)

    const codex = await codexPanel()
    expect(within(codex).getByRole('button', { name: 'Save' })).toBeDisabled()
    expect(within(codex).getByRole('button', { name: 'Cancel' })).toBeDisabled()

    await userEvent.setup().click(within(codex).getByRole('switch'))
    expect(within(codex).getByRole('button', { name: 'Save' })).toBeEnabled()
  })

  it('sends the revision the host checks', async () => {
    const calls = mockFetch(() => ({ ok: true, status: 200, payload: agentSettings() }))
    const user = userEvent.setup()
    render(<Settings snapshot={snapshot()} />)

    const codex = await codexPanel()
    await user.click(within(codex).getByRole('switch'))
    await user.click(within(codex).getByRole('button', { name: 'Save' }))

    await waitFor(() => expect(calls).toHaveLength(1))
    expect(calls[0]).toMatchObject({ agent: 'codex', revision: 'rev-1', fast: true })
  })

  it('keeps the edit when the host rejects the revision', async () => {
    mockFetch(() => ({
      ok: false,
      status: 409,
      payload: { error: { code: 'REVISION_STALE' } },
    }))
    const user = userEvent.setup()
    render(<Settings snapshot={snapshot()} />)

    const codex = await codexPanel()
    await user.click(within(codex).getByRole('switch'))
    await user.click(within(codex).getByRole('button', { name: 'Save' }))

    // The draft must survive a conflict, or the operator loses what they typed.
    await waitFor(() => expect(within(codex).getByRole('switch')).toBeChecked())
    expect(within(codex).getByRole('button', { name: 'Save' })).toBeEnabled()
    expect(within(codex).getByText(/REVISION_STALE/)).toBeInTheDocument()
  })

  it('restores the saved values on Cancel', async () => {
    mockFetch(() => ({ ok: true, status: 200, payload: agentSettings() }))
    const user = userEvent.setup()
    render(<Settings snapshot={snapshot()} />)

    const codex = await codexPanel()
    await user.click(within(codex).getByRole('switch'))
    expect(within(codex).getByRole('switch')).toBeChecked()

    await user.click(within(codex).getByRole('button', { name: 'Cancel' }))
    expect(within(codex).getByRole('switch')).not.toBeChecked()
    expect(within(codex).getByRole('button', { name: 'Save' })).toBeDisabled()
  })
})
