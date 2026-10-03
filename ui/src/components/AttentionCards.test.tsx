import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, expect, it, vi } from 'vitest'
import { snapshot, task } from '@/test/fixtures'
import fixtures from '@/lib/integration.fixtures.json'
import { decodeIntegrationView } from '@/lib/integration.contract'
import { AttentionCards } from './AttentionCards'
import type { TaskDetail } from '@/lib/api'

afterEach(() => vi.unstubAllGlobals())

it('offers re-drive and close with the exact task and integration revision', async () => {
  const view = decodeIntegrationView(fixtures.cases.find(row => row.name === 'blocked')!.view)
  const row = { ...task({ state: 'open' }), ...view }
  const preview = { task: row, ...view, head_oid: 'a'.repeat(40), turns: [], timeline: [],
    questions: [], files_changed: [] } as unknown as TaskDetail
  const fetch = vi.fn().mockResolvedValue({ ok: false, status: 503,
    json: async () => ({ error: { code: 'INTEGRATION_UNAVAILABLE', message: 'Compatible owner unavailable' } }) })
  vi.stubGlobal('fetch', fetch)
  render(<AttentionCards snapshot={snapshot({ tasks: [row] })} previews={{ [row.task_id]: preview }} />)
  fireEvent.click(screen.getByRole('button', { name: 'Re-drive integration' }))
  await screen.findByText('Compatible owner unavailable')
  expect(screen.getByText(view.integration!.blocked_code!)).toBeInTheDocument()
  const body = JSON.parse(fetch.mock.calls[0][1].body)
  expect(body.integration.task_id).toBe(row.task_id)
  expect(body.expected_integration_id).toBe(view.integration!.integration_id)
  expect(body.integration.expected).toBe(view.integration!.revision)
  fireEvent.click(screen.getByRole('button', { name: 'Close task' }))
  await waitFor(() => expect(fetch).toHaveBeenCalledTimes(2))
  expect(JSON.parse(fetch.mock.calls[1][1].body)).toMatchObject({
    expected_task_id: row.task_id, expected_integration_id: view.integration!.integration_id,
    expected_integration_revision: view.integration!.revision,
  })
})

it('shows blocked integration code and recovery link using the exact task id', () => {
  const view = decodeIntegrationView(fixtures.cases.find(row => row.view.integration?.state === 'blocked')!.view)
  const row = { ...task({ state: 'open' }), ...view }
  const onSelect = vi.fn()
  render(<AttentionCards snapshot={snapshot({ tasks: [row] })} previews={{}} onSelect={onSelect} />)
  expect(screen.getByText(view.integration!.blocked_code!)).toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: 'Recover integration' }))
  expect(onSelect).toHaveBeenCalledWith(row.task_id)
})

it('does not show a success attention card even if old review state is present', () => {
  const view = decodeIntegrationView(fixtures.cases.find(row => row.name === 'integrated')!.view)
  render(<AttentionCards snapshot={snapshot({ tasks: [{ ...task({ state: 'open', review_state: 'ready_for_review' }),
    ...view, review_state: 'ready_for_review' }] })} previews={{}} />)
  expect(screen.queryByRole('heading', { name: 'Needs your attention' })).not.toBeInTheDocument()
})

it('shows a terminal dependency error and opens the exact task without re-drive', () => {
  const view = decodeIntegrationView(fixtures.cases.find(row => row.name === 'armed')!.view)
  const row = { ...task({ state: 'abandoned', blocking_code: 'INTEGRATION_DEPENDENCY_NOT_INTEGRATED' }),
    ...view, workflow_state: 'needs_you' as const }
  const onSelect = vi.fn()
  render(<AttentionCards snapshot={snapshot({ tasks: [row] })} previews={{}} onSelect={onSelect} />)
  expect(screen.getByText('INTEGRATION_DEPENDENCY_NOT_INTEGRATED')).toBeInTheDocument()
  expect(screen.queryByRole('button', { name: 'Re-drive integration' })).not.toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: 'Open task' }))
  expect(onSelect).toHaveBeenCalledWith(row.task_id)
})
