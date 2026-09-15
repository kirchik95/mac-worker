import { act, render, screen, waitFor, within } from '@testing-library/react'
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
    model: selects[0].querySelector('[data-slot="select-value"]')?.textContent,
    effort: selects[1].querySelector('[data-slot="select-value"]')?.textContent,
    fast: within(panel).getByRole('switch').getAttribute('aria-checked'),
  }
}

afterEach(() => vi.unstubAllGlobals())

describe('Settings', () => {
  it('searches models without changing the draft until an option is selected and Save is pressed', async () => {
    const calls = mockFetch((body) => ({
      ok: true,
      status: 200,
      payload: { ...agentSettings().agents[0], ...(body as object) },
    }))
    const user = userEvent.setup()
    render(<Settings snapshot={snapshot()} />)
    const panel = await codexPanel()
    const trigger = within(panel).getByRole('combobox', { name: 'Model' })
    const save = within(panel).getByRole('button', { name: 'Save defaults' })

    await user.click(trigger)
    const search = await screen.findByRole('combobox', { name: 'Search models' })
    expect(search).toHaveFocus()
    await user.type(search, 'TiNy')
    expect(screen.getByRole('option', { name: 'Tiny' })).toBeInTheDocument()
    expect(screen.queryByRole('option', { name: 'GPT-6-Astra' })).not.toBeInTheDocument()
    expect(trigger).toHaveTextContent('GPT-6-Astra')
    expect(save).toBeDisabled()
    expect(calls).toHaveLength(0)

    await user.keyboard('{ArrowDown}{Enter}')
    expect(trigger).toHaveTextContent('Tiny')
    expect(save).toBeEnabled()
    expect(calls).toHaveLength(0)
    await user.click(save)
    await screen.findByText('Saved.')
    expect(calls).toEqual([
      { agent: 'codex', model: 'tiny', effort: null, fast: null, revision: 'rev-1' },
    ])
  })

  it('dismisses an empty model search without losing the selection and starts fresh on reopen', async () => {
    const calls = mockFetch(() => ({ ok: true, status: 200, payload: agentSettings().agents[0] }))
    const user = userEvent.setup()
    render(<Settings snapshot={snapshot()} />)
    const panel = await codexPanel()
    const trigger = within(panel).getByRole('combobox', { name: 'Model' })

    await user.click(trigger)
    await user.type(await screen.findByRole('combobox', { name: 'Search models' }), 'no-such-model')
    expect(screen.getByText('No models found.')).toBeInTheDocument()
    expect(screen.queryByRole('option')).not.toBeInTheDocument()
    await user.keyboard('{Escape}')
    await waitFor(() => expect(trigger).toHaveFocus())
    expect(trigger).toHaveTextContent('GPT-6-Astra')
    expect(within(panel).getByRole('button', { name: 'Save defaults' })).toBeDisabled()

    await user.keyboard('{ArrowDown}')
    expect(await screen.findByRole('combobox', { name: 'Search models' })).toHaveValue('')
    expect(screen.getByRole('option', { name: 'Agent default' })).toBeInTheDocument()
    expect(screen.getByRole('option', { name: 'GPT-6-Astra' })).toBeInTheDocument()
    expect(calls).toHaveLength(0)
  })

  it('uses the Mac’s live Cursor catalogue and saves the canonical native ID', async () => {
    const cursor = {
      ...agentSettings().agents[0],
      agent: 'cursor',
      model: 'grok-4.6',
      effort: 'high',
      fast: true,
      model_catalog_source: 'live',
      model_options: [
        {
          id: 'grok-4.6',
          label: 'Cursor Grok 4.6 High Fast',
          effort_options: ['high'],
          fast_supported: true,
          capabilities_known: true,
        },
        {
          id: 'gpt-5.6-sol',
          label: 'GPT-5.6 Sol',
          effort_options: ['none', 'medium', 'extra-high'],
          fast_supported: true,
          capabilities_known: true,
        },
        {
          id: 'worker-custom-model',
          label: 'Worker custom model',
          effort_options: [],
          fast_supported: false,
        },
      ],
    }
    const saves: unknown[] = []
    vi.stubGlobal(
      'fetch',
      vi.fn(async (_url: string, init?: RequestInit) => {
        if (init?.method === 'POST') {
          const request = JSON.parse(String(init.body))
          saves.push(request)
          return { ok: true, status: 200, json: async () => ({ ...cursor, ...request }) }
        }
        return { ok: true, status: 200, json: async () => ({ agents: [cursor] }) }
      }),
    )
    const user = userEvent.setup()
    render(<Settings snapshot={snapshot()} />)
    const panel = (await screen.findByRole('heading', { name: 'Cursor on mini-1' })).closest(
      'section',
    )!
    expect(within(panel).getByRole('switch')).toBeChecked()
    expect(within(panel).getByRole('switch')).toBeEnabled()
    await user.click(within(panel).getByRole('combobox', { name: 'Model' }))
    expect(await screen.findByRole('option', { name: 'Worker custom model' })).toBeInTheDocument()
    await user.click(await screen.findByRole('option', { name: 'GPT-5.6 Sol' }))
    await user.click(within(panel).getByRole('combobox', { name: 'Reasoning effort' }))
    await user.click(await screen.findByRole('option', { name: /extra.high/i }))
    await user.click(within(panel).getByRole('button', { name: 'Save defaults' }))
    await screen.findByText('Saved.')
    expect(saves).toEqual([
      {
        agent: 'cursor',
        model: 'gpt-5.6-sol',
        effort: 'extra-high',
        fast: true,
        revision: cursor.revision,
      },
    ])
    expect(within(panel).getByRole('combobox', { name: 'Model' })).toHaveTextContent('GPT-5.6 Sol')
  })

  it('distinguishes unavailable Cursor capabilities from unsupported controls', async () => {
    const cursor = { ...agentSettings().agents[0], agent: 'cursor', model: 'unknown', effort: null, fast: null,
      model_options: [
        { id: 'unknown', label: 'Remembered model', effort_options: [], fast_supported: false },
        { id: 'default', label: 'Auto', effort_options: [], fast_supported: false, capabilities_known: true },
      ], model_catalog_source: 'remembered' }
    vi.stubGlobal('fetch', vi.fn(async () => ({ ok: true, status: 200, json: async () => ({ agents: [cursor] }) })))
    const user = userEvent.setup()
    render(<Settings snapshot={snapshot()} />)
    const panel = (await screen.findByRole('heading', { name: 'Cursor on mini-1' })).closest('section')!
    expect(within(panel).getByText('Fast support hasn’t been reported by this Mac.')).toBeInTheDocument()
    expect(within(panel).getByRole('switch')).toHaveAttribute('aria-disabled', 'true')
    await user.click(within(panel).getByRole('combobox', { name: 'Model' }))
    await user.click(await screen.findByRole('option', { name: 'Auto' }))
    expect(within(panel).getByText('Not supported by this model.')).toBeInTheDocument()
    expect(within(panel).getByText('This model has no reasoning control.')).toBeInTheDocument()
  })

  it('clears Fast when switching from an enabled model to a model without Fast', async () => {
    const cursor = { ...agentSettings().agents[0], agent: 'cursor', model: 'fast-model', fast: true,
      model_options: [
        { id: 'fast-model', label: 'Fast model', effort_options: [], fast_supported: true, capabilities_known: true },
        { id: 'default', label: 'Auto', effort_options: [], fast_supported: false, capabilities_known: true },
      ] }
    vi.stubGlobal('fetch', vi.fn(async () => ({ ok: true, status: 200, json: async () => ({ agents: [cursor] }) })))
    const user = userEvent.setup()
    render(<Settings snapshot={snapshot()} />)
    const panel = (await screen.findByRole('heading', { name: 'Cursor on mini-1' })).closest('section')!
    expect(within(panel).getByRole('switch')).toBeChecked()
    await user.click(within(panel).getByRole('combobox', { name: 'Model' }))
    await user.click(await screen.findByRole('option', { name: 'Auto' }))
    expect(within(panel).getByRole('switch')).not.toBeChecked()
    expect(within(panel).getByRole('switch')).toHaveAttribute('aria-disabled', 'true')
    expect(within(panel).getByText('Off')).toBeInTheDocument()
  })

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
      await screen.findByText('This agent does not expose a reasoning default.'),
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

  it('retains an unsaved draft when switching agents', async () => {
    const calls = mockFetch(() => ({
      ok: true,
      status: 200,
      payload: agentSettings().agents[0],
    }))
    const user = userEvent.setup()
    render(<Settings snapshot={snapshot()} />)

    const codex = await codexPanel()
    await user.click(within(codex).getByRole('switch'))
    await user.click(within(codex).getByRole('combobox', { name: 'Reasoning effort' }))
    await user.click(await screen.findByRole('option', { name: 'Low' }))
    await user.click(screen.getByText('OpenCode'))
    expect(await screen.findByText('OpenCode on mini-1')).toBeInTheDocument()
    await user.click(screen.getByText('Codex', { selector: '.font-medium' }))

    const restored = await codexPanel()
    expect(draftIn(restored)).toEqual({ model: 'GPT-6-Astra', effort: 'Low', fast: 'true' })
    expect(within(restored).getByRole('button', { name: 'Save defaults' })).toBeEnabled()
    expect(calls).toHaveLength(0)
  })

  it('keeps separate drafts for each Mac through refetch and Cancel', async () => {
    const calls = mockFetch(() => ({
      ok: true,
      status: 200,
      payload: agentSettings().agents[0],
    }))
    const user = userEvent.setup()
    render(<Settings snapshot={snapshot({ workers: [worker(), worker({ name: 'mini-2' })] })} />)

    const miniOne = await codexPanel()
    await user.click(within(miniOne).getByRole('switch'))
    await user.click(screen.getByRole('combobox', { name: 'Worker' }))
    await user.click(await screen.findByRole('option', { name: 'mini-2' }))
    const miniTwo = (await screen.findByText('Codex on mini-2')).closest('section')!
    expect(within(miniTwo).getByRole('switch')).not.toBeChecked()
    await user.click(within(miniTwo).getByRole('combobox', { name: 'Reasoning effort' }))
    await user.click(await screen.findByRole('option', { name: 'Low' }))

    await user.click(screen.getByRole('combobox', { name: 'Worker' }))
    await user.click(await screen.findByRole('option', { name: 'mini-1' }))
    const restoredOne = await codexPanel()
    expect(draftIn(restoredOne)).toEqual({ model: 'GPT-6-Astra', effort: 'Xhigh', fast: 'true' })
    await user.click(within(restoredOne).getByRole('button', { name: 'Cancel' }))
    expect(within(restoredOne).getByRole('switch')).not.toBeChecked()

    await user.click(screen.getByRole('combobox', { name: 'Worker' }))
    await user.click(await screen.findByRole('option', { name: 'mini-2' }))
    const restoredTwo = (await screen.findByText('Codex on mini-2')).closest('section')!
    expect(draftIn(restoredTwo)).toEqual({ model: 'GPT-6-Astra', effort: 'Low', fast: 'false' })
    expect(within(restoredTwo).getByRole('button', { name: 'Save defaults' })).toBeEnabled()
    expect(calls).toHaveLength(0)
  })

  it('keeps the draft revision through a refetch until Cancel accepts the newer defaults', async () => {
    let fetched = agentSettings()
    const requests: unknown[] = []
    vi.stubGlobal(
      'fetch',
      vi.fn(async (_url: string, init?: RequestInit) => {
        if (init?.method === 'POST') {
          requests.push(JSON.parse(String(init.body)))
          return {
            ok: false,
            status: 409,
            json: async () => ({ error: { code: 'REVISION_STALE' } }),
          }
        }
        return { ok: true, status: 200, json: async () => fetched }
      }),
    )
    const user = userEvent.setup()
    const { rerender } = render(<Settings snapshot={snapshot()} initialAgent="codex" />)
    await user.click(within(await codexPanel()).getByRole('switch'))
    fetched = {
      agents: agentSettings().agents.map((agent) =>
        agent.agent === 'codex'
          ? { ...agent, model: 'tiny', effort: 'low', revision: 'rev-newer' }
          : agent,
      ),
    }
    rerender(<Settings snapshot={snapshot()} initialAgent="opencode" />)
    await screen.findByText('OpenCode on mini-1')
    await user.click(screen.getByText('Codex', { selector: '.font-medium' }))

    const codex = await codexPanel()
    expect(draftIn(codex)).toEqual({ model: 'GPT-6-Astra', effort: 'Xhigh', fast: 'true' })
    await user.click(within(codex).getByRole('button', { name: 'Save defaults' }))
    await within(codex).findByText(/REVISION_STALE/)
    expect(requests[0]).toMatchObject({ revision: 'rev-1', model: 'gpt-6-astra', fast: true })
    expect(within(codex).getByRole('switch')).toBeChecked()

    await user.click(within(codex).getByRole('button', { name: 'Cancel' }))
    expect(draftIn(codex)).toEqual({ model: 'Tiny', effort: 'Low', fast: 'false' })
    expect(within(codex).getByRole('button', { name: 'Save defaults' })).toBeDisabled()
    await user.click(within(codex).getByRole('combobox', { name: 'Model' }))
    await user.click(await screen.findByRole('option', { name: 'GPT-6-Astra' }))
    await user.click(within(codex).getByRole('button', { name: 'Save defaults' }))
    await waitFor(() => expect(requests).toHaveLength(2))
    expect(requests[1]).toMatchObject({ revision: 'rev-newer' })
  })

  it('locks mutable controls until the save completes and reports its result', async () => {
    const response = deferred<{ ok: boolean; status: number; json: () => Promise<unknown> }>()
    vi.stubGlobal(
      'fetch',
      vi.fn(async (_url: string, init?: RequestInit) => {
        if (init?.method === 'POST') return response.promise
        return { ok: true, status: 200, json: async () => agentSettings() }
      }),
    )
    const user = userEvent.setup()
    render(<Settings snapshot={snapshot()} />)
    const codex = await codexPanel()
    await user.click(within(codex).getByRole('switch'))
    await user.click(within(codex).getByRole('button', { name: 'Save defaults' }))

    const saving = within(codex).getByRole('button', { name: 'Saving…' })
    expect(saving).toHaveAttribute('aria-busy', 'true')
    expect(saving).toBeDisabled()
    expect(within(codex).getByRole('combobox', { name: 'Model' })).toBeDisabled()
    expect(within(codex).getByRole('combobox', { name: 'Reasoning effort' })).toBeDisabled()
    expect(within(codex).getByRole('switch')).toHaveAttribute('aria-disabled', 'true')
    expect(within(codex).getByRole('button', { name: 'Cancel' })).toBeDisabled()
    await user.click(within(codex).getByRole('switch'))
    expect(within(codex).getByRole('switch')).toBeChecked()
    expect(within(codex).queryByRole('button', { name: 'Saved' })).not.toBeInTheDocument()

    await act(async () => {
      response.resolve({
        ok: true,
        status: 200,
        json: async () => ({ ...agentSettings().agents[0], fast: true, revision: 'rev-saved' }),
      })
    })
    const saved = await within(codex).findByRole('button', { name: 'Saved' })
    expect(saved).toBeDisabled()
    expect(saved).toHaveAttribute('aria-busy', 'false')
    expect(within(codex).getByRole('switch')).not.toHaveAttribute('aria-disabled', 'true')
    expect(within(codex).getByRole('switch')).toBeChecked()
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
      expect(within(codex).getByRole('button', { name: 'Saved' })).toBeDisabled()
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
    expect(await within(codex).findByRole('button', { name: 'Try again' })).toBeEnabled()
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
