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
  const heading = await screen.findByText('Codex on mini-1')
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
    const table = await screen.findByRole('complementary', { name: 'Agents' })
    expect(within(table).getByText(/Connected/)).toBeInTheDocument()
    expect(within(table).getByText(/Not configured/)).toBeInTheDocument()
  })

  it('keeps Save and Cancel inert until the draft differs', async () => {
    mockFetch(() => ({ ok: true, status: 200, payload: agentSettings().agents[0] }))
    render(<Settings snapshot={snapshot()} />)

    const codex = await codexPanel()
    expect(within(codex).getByRole('button', { name: 'Save defaults' })).toBeDisabled()
    expect(within(codex).getByRole('button', { name: 'Cancel' })).toBeDisabled()

    await userEvent.setup().click(within(codex).getByRole('switch'))
    expect(within(codex).getByRole('button', { name: 'Save defaults' })).toBeEnabled()
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
    await user.click(within(codex).getByRole('button', { name: 'Save defaults' }))

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
    await user.click(within(codex).getByRole('button', { name: 'Save defaults' }))

    expect(await within(codex).findByText('Saved.')).toBeInTheDocument()
    const table = screen.getByRole('complementary', { name: 'Agents' })
    expect(within(codex).getByText('Low')).toBeInTheDocument()
    expect(within(table).getByText('OpenCode')).toBeInTheDocument()

    await waitFor(() => {
      expect(within(codex).getByRole('switch')).toBeChecked()
      expect(within(codex).getByRole('button', { name: 'Save defaults' })).toBeDisabled()
    })
    await user.click(within(codex).getByRole('switch'))
    await waitFor(() =>
      expect(within(codex).getByRole('button', { name: 'Save defaults' })).toBeEnabled(),
    )
    await user.click(within(codex).getByRole('button', { name: 'Save defaults' }))
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
    await user.click(within(codex).getByRole('button', { name: 'Save defaults' }))

    // The draft must survive a conflict, or the operator loses what they typed.
    await waitFor(() => expect(within(codex).getByRole('switch')).toBeChecked())
    expect(within(codex).getByRole('button', { name: 'Save defaults' })).toBeEnabled()
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
    expect(within(codex).getByRole('button', { name: 'Save defaults' })).toBeDisabled()
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
    render(<Settings snapshot={snapshot({ workers: [worker(), worker({ name: 'mini-2' })] })} />)

    const miniOne = await codexPanel()
    await user.click(within(miniOne).getByRole('switch'))
    await user.click(within(miniOne).getByRole('button', { name: 'Save defaults' }))
    await user.click(screen.getByRole('combobox', { name: 'Worker' }))
    await user.click(await screen.findByRole('option', { name: 'mini-2' }))

    const miniTwoHeading = await screen.findByText('Codex on mini-2')
    const miniTwo = miniTwoHeading.closest('section') as HTMLElement
    const beforeDelayedResolution = draftIn(miniTwo)
    workerOneSave.resolve({
      ok: true,
      status: 200,
      json: async () => ({ ...agentSettings().agents[0], fast: true, revision: 'rev-2' }),
    })

    await waitFor(() => expect(screen.getByText('Codex on mini-2')).toBeInTheDocument())
    expect(draftIn(miniTwo)).toEqual(beforeDelayedResolution)
    expect(within(miniTwo).getByRole('button', { name: 'Save defaults' })).toBeDisabled()
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
    await user.click(within(codex).getByRole('button', { name: 'Save defaults' }))
    await user.click(screen.getByText('OpenCode'))

    const openCodeHeading = await screen.findByText('OpenCode on mini-1')
    const openCode = openCodeHeading.closest('section') as HTMLElement
    await waitFor(() =>
      expect(within(openCode).getByRole('button', { name: 'Save defaults' })).toBeInTheDocument(),
    )
    const beforeDelayedResolution = draftIn(openCode)
    codexSave.resolve({
      ok: true,
      status: 200,
      json: async () => ({ ...agentSettings().agents[0], fast: true, revision: 'rev-2' }),
    })

    await waitFor(() => expect(screen.getByText('OpenCode on mini-1')).toBeInTheDocument())
    expect(draftIn(openCode)).toEqual(beforeDelayedResolution)
    expect(
      within(openCode).getByText('Task settings can override these defaults.'),
    ).toBeInTheDocument()
    expect(within(openCode).getByRole('button', { name: 'Save defaults' })).toBeDisabled()
  })
})

it('keeps a manually selected Mac and its draft when the snapshot refreshes', async () => {
  mockFetch(() => ({ ok: true, status: 200, payload: agentSettings().agents[0] }))
  const user = userEvent.setup()
  const pool = () => snapshot({ workers: [worker(), worker({ name: 'mini-2' })] })
  const { rerender } = render(
    <Settings snapshot={pool()} initialWorker="mini-1" initialAgent="codex" />,
  )
  await codexPanel()
  await user.click(screen.getByRole('combobox', { name: 'Worker' }))
  await user.click(await screen.findByRole('option', { name: 'mini-2' }))
  const editor = (await screen.findByText('Codex on mini-2')).closest('section')!
  await user.click(within(editor).getByRole('switch'))
  rerender(<Settings snapshot={pool()} initialWorker="mini-1" initialAgent="codex" />)
  expect(screen.getByText('Codex on mini-2')).toBeInTheDocument()
  expect(within(editor).getByRole('switch')).toBeChecked()
})

it('presents unavailable native defaults as read-only facts', async () => {
  vi.stubGlobal(
    'fetch',
    vi.fn(async () => ({
      ok: true,
      status: 200,
      json: async () => ({
        agents: [
          {
            ...agentSettings().agents[1],
            model: null,
            effort: null,
            fast: null,
            writable: false,
            message: 'This Mac has not reported editable defaults.',
          },
        ],
      }),
    })),
  )
  render(<Settings snapshot={snapshot()} initialAgent="opencode" />)
  await screen.findByRole('heading', { name: 'OpenCode on mini-1' })
  expect(screen.queryByRole('combobox', { name: 'Model' })).not.toBeInTheDocument()
  expect(screen.queryByRole('button', { name: 'Save defaults' })).not.toBeInTheDocument()
  expect(screen.getByRole('heading', { name: 'Read-only defaults' })).toBeInTheDocument()
})
