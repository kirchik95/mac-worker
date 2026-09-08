import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { agentSettings, snapshot, worker } from '@/test/fixtures'
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

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

const codexPanel = async () => {
  const heading = await screen.findByText('mini-1 / codex')
  return heading.closest('section') as HTMLElement
}

const draftIn = (panel: HTMLElement) => {
  const selects = within(panel).getAllByRole('combobox')
  return {
    model: selects[0].textContent,
    effort: selects[1].textContent,
    fast: within(panel).getByRole('switch').getAttribute('aria-checked'),
  }
}

afterEach(() => vi.unstubAllGlobals())

describe('Settings', () => {
  it('shows the recorded defaults for each agent', async () => {
    mockFetch(() => ({ ok: true, status: 200, payload: agentSettings().agents[0] }))
    render(<Settings snapshot={snapshot()} />)

    const codex = await codexPanel()
    expect(within(codex).getByText('GPT-6-Astra')).toBeInTheDocument()
    expect(within(codex).getByText('Xhigh')).toBeInTheDocument()
  })

  it('says when the selected agent publishes no global effort setting', async () => {
    mockFetch(() => ({ ok: true, status: 200, payload: agentSettings().agents[0] }))
    const user = userEvent.setup()
    render(<Settings snapshot={snapshot()} />)

    await user.click(await screen.findByText('OpenCode'))
    expect(
      await screen.findByText('opencode publishes no global effort setting.'),
    ).toBeInTheDocument()
  })

  it('reports each agent connection from the cached facts', async () => {
    mockFetch(() => ({ ok: true, status: 200, payload: agentSettings().agents[0] }))
    render(<Settings snapshot={snapshot()} />)

    // codex is authenticated in the fixture; opencode is absent from the facts.
    const table = await screen.findByRole('table')
    expect(within(table).getByText('Connected')).toBeInTheDocument()
    expect(within(table).getByText('Not configured')).toBeInTheDocument()
  })

  it('keeps Save and Cancel inert until the draft differs', async () => {
    mockFetch(() => ({ ok: true, status: 200, payload: agentSettings().agents[0] }))
    render(<Settings snapshot={snapshot()} />)

    const codex = await codexPanel()
    expect(within(codex).getByRole('button', { name: 'Save' })).toBeDisabled()
    expect(within(codex).getByRole('button', { name: 'Cancel' })).toBeDisabled()

    await userEvent.setup().click(within(codex).getByRole('switch'))
    expect(within(codex).getByRole('button', { name: 'Save' })).toBeEnabled()
  })

  it('sends the revision the host checks', async () => {
    const calls = mockFetch(() => ({
      ok: true,
      status: 200,
      payload: agentSettings().agents[0],
    }))
    const user = userEvent.setup()
    render(<Settings snapshot={snapshot()} />)

    const codex = await codexPanel()
    await user.click(within(codex).getByRole('switch'))
    await user.click(within(codex).getByRole('button', { name: 'Save' }))

    await waitFor(() => expect(calls).toHaveLength(1))
    expect(calls[0]).toMatchObject({ agent: 'codex', revision: 'rev-1', fast: true })
  })

  it('merges the singular saved agent without dropping its peer', async () => {
    const savedCodex = {
      ...agentSettings().agents[0],
      effort: 'low',
      fast: true,
      revision: 'rev-2',
    }
    const calls = mockFetch(() => ({ ok: true, status: 200, payload: savedCodex }))
    const user = userEvent.setup()
    render(<Settings snapshot={snapshot()} />)

    const codex = await codexPanel()
    await user.click(within(codex).getByRole('switch'))
    await user.click(within(codex).getByRole('button', { name: 'Save' }))

    expect(await within(codex).findByText('Saved.')).toBeInTheDocument()
    const table = screen.getByRole('table')
    expect(within(table).getByText('Low')).toBeInTheDocument()
    expect(within(table).getByText('OpenCode')).toBeInTheDocument()

    await waitFor(() => {
      expect(within(codex).getByRole('switch')).toBeChecked()
      expect(within(codex).getByRole('button', { name: 'Save' })).toBeDisabled()
    })
    await user.click(within(codex).getByRole('switch'))
    await waitFor(() =>
      expect(within(codex).getByRole('button', { name: 'Save' })).toBeEnabled(),
    )
    await user.click(within(codex).getByRole('button', { name: 'Save' }))
    await waitFor(() => expect(calls).toHaveLength(2))
    expect(calls[1]).toMatchObject({ agent: 'codex', revision: 'rev-2', fast: false })
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
    mockFetch(() => ({ ok: true, status: 200, payload: agentSettings().agents[0] }))
    const user = userEvent.setup()
    render(<Settings snapshot={snapshot()} />)

    const codex = await codexPanel()
    await user.click(within(codex).getByRole('switch'))
    expect(within(codex).getByRole('switch')).toBeChecked()

    await user.click(within(codex).getByRole('button', { name: 'Cancel' }))
    expect(within(codex).getByRole('switch')).not.toBeChecked()
    expect(within(codex).getByRole('button', { name: 'Save' })).toBeDisabled()
  })

  it('ignores a delayed save after switching workers', async () => {
    const workerOneSave = deferred<{
      ok: boolean
      status: number
      json: () => Promise<unknown>
    }>()
    const miniTwoSettings = {
      agents: [
        {
          ...agentSettings().agents[0],
          model: 'tiny',
          effort: 'low',
          revision: 'mini-2-rev-1',
        },
      ],
    }
    vi.stubGlobal(
      'fetch',
      vi.fn(async (url: string, init?: RequestInit) => {
        if (init?.method === 'POST') return workerOneSave.promise
        return {
          ok: true,
          status: 200,
          json: async () => (url.includes('/mini-2/') ? miniTwoSettings : agentSettings()),
        }
      }),
    )
    const user = userEvent.setup()
    render(
      <Settings
        snapshot={snapshot({ workers: [worker(), worker({ name: 'mini-2' })] })}
      />,
    )

    const miniOne = await codexPanel()
    await user.click(within(miniOne).getByRole('switch'))
    await user.click(within(miniOne).getByRole('button', { name: 'Save' }))
    await user.click(screen.getByRole('combobox', { name: 'Worker' }))
    await user.click(await screen.findByRole('option', { name: 'mini-2' }))

    const miniTwoHeading = await screen.findByText('mini-2 / codex')
    const miniTwo = miniTwoHeading.closest('section') as HTMLElement
    const beforeDelayedResolution = draftIn(miniTwo)
    workerOneSave.resolve({
      ok: true,
      status: 200,
      json: async () => ({ ...agentSettings().agents[0], fast: true, revision: 'rev-2' }),
    })

    await waitFor(() => expect(screen.getByText('mini-2 / codex')).toBeInTheDocument())
    expect(draftIn(miniTwo)).toEqual(beforeDelayedResolution)
    expect(within(miniTwo).getByRole('button', { name: 'Save' })).toBeDisabled()
  })

  it('invalidates a delayed save when selecting another agent', async () => {
    const codexSave = deferred<{
      ok: boolean
      status: number
      json: () => Promise<unknown>
    }>()
    vi.stubGlobal(
      'fetch',
      vi.fn(async (_url: string, init?: RequestInit) => {
        if (init?.method === 'POST') return codexSave.promise
        return { ok: true, status: 200, json: async () => agentSettings() }
      }),
    )
    const user = userEvent.setup()
    render(<Settings snapshot={snapshot()} />)

    const codex = await codexPanel()
    await user.click(within(codex).getByRole('switch'))
    await user.click(within(codex).getByRole('button', { name: 'Save' }))
    await user.click(screen.getByText('OpenCode'))

    const openCodeHeading = await screen.findByText('mini-1 / opencode')
    const openCode = openCodeHeading.closest('section') as HTMLElement
    await waitFor(() =>
      expect(within(openCode).getByRole('button', { name: 'Save' })).toBeInTheDocument(),
    )
    const beforeDelayedResolution = draftIn(openCode)
    codexSave.resolve({
      ok: true,
      status: 200,
      json: async () => ({ ...agentSettings().agents[0], fast: true, revision: 'rev-2' }),
    })

    await waitFor(() => expect(screen.getByText('mini-1 / opencode')).toBeInTheDocument())
    expect(draftIn(openCode)).toEqual(beforeDelayedResolution)
    expect(within(openCode).getByText('Task settings can override these defaults.')).toBeInTheDocument()
    expect(within(openCode).getByRole('button', { name: 'Save' })).toBeDisabled()
  })
})
