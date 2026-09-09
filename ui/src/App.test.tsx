import { render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { snapshot, task, worker } from '@/test/fixtures'
import App, { documentTitle } from './App'

afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
})

describe('App states', () => {
  it('shows the skeleton until the first snapshot arrives', () => {
    vi.stubGlobal('fetch', vi.fn(() => new Promise(() => {})))
    render(<App />)

    expect(screen.getByLabelText('Loading the first snapshot')).toBeInTheDocument()
    expect(screen.getByText(/SNAPSHOT --:--:--/)).toBeInTheDocument()
    expect(screen.getByText(/Waiting for the first snapshot/)).toBeInTheDocument()
  })

  it('keeps the last snapshot on screen and says the API stopped answering', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce({ ok: true, status: 200, json: async () => snapshot() })
      .mockRejectedValue(new Error('connection refused'))
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    render(<App />)

    await waitFor(() => expect(screen.getAllByText('mini-1').length).toBeGreaterThan(0))
    await vi.advanceTimersByTimeAsync(2100)

    await waitFor(() =>
      expect(screen.getByText('The dashboard API stopped answering')).toBeInTheDocument(),
    )
    // The fleet stays readable; only the header and footer say it is not live.
    expect(screen.getAllByText('mini-1').length).toBeGreaterThan(0)
    expect(screen.getByText(/LAST SNAPSHOT/)).toBeInTheDocument()
    expect(screen.getByText(/Last known snapshot/)).toBeInTheDocument()
  })
})

describe('tab title', () => {
  it('leads with the number of tasks waiting on an answer', () => {
    expect(documentTitle(2)).toBe('(2) mac-worker — pool')
  })

  it('drops the count entirely when nothing is waiting', () => {
    expect(documentTitle(0)).toBe('mac-worker — pool')
  })

  it('counts what the pool is actually waiting on', async () => {
    const waiting = snapshot({
      tasks: [
        task({ task_id: 'a'.repeat(32), state: 'open', last_outcome: { kind: 'needs_input' } }),
        task({ task_id: 'b'.repeat(32), state: 'closed', last_outcome: { kind: 'done' } }),
      ],
    })
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => waiting }),
    )
    render(<App />)

    await waitFor(() => expect(document.title).toBe('(1) mac-worker — pool'))
  })
})

describe('worker errors', () => {
  it('shows a worker error object as text instead of crashing (React #31)', async () => {
    const withError = snapshot({
      workers: [
        worker({
          health: 'unavailable',
          error: {
            code: 'SSH_UNAVAILABLE',
            message: 'ssh timed out connecting to mini-1',
          },
        }),
      ],
    })
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => withError }),
    )
    render(<App />)

    await waitFor(() =>
      expect(screen.getAllByText('ssh timed out connecting to mini-1').length).toBeGreaterThan(0),
    )
    expect(screen.getAllByText('SSH_UNAVAILABLE').length).toBeGreaterThan(0)
  })

  it('still shows a legacy string error instead of crashing', async () => {
    const withError = snapshot({
      workers: [worker({ health: 'unavailable' })],
    })
    ;(withError.workers[0] as { error: unknown }).error = 'SSH_UNAVAILABLE'
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => withError }),
    )
    render(<App />)

    await waitFor(() =>
      expect(screen.getAllByText('SSH_UNAVAILABLE').length).toBeGreaterThan(0),
    )
  })
})
