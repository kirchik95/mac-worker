import { afterEach, expect, it, vi } from 'vitest'
import { acceptTask, fetchAgentSettings, fetchTaskDetail, fetchTurnLog } from './api'
import { exampleSnapshot } from './exampleSnapshot'

afterEach(() => {
  window.history.replaceState(null, '', '/')
  vi.unstubAllGlobals()
})
it('keeps example details, settings, logs and mutations away from the live API', async () => {
  window.history.replaceState(null, '', '/?example')
  vi.stubGlobal(
    'fetch',
    vi.fn(() => {
      throw new Error('unexpected live request')
    }),
  )
  const task = exampleSnapshot().tasks[0]
  expect(task.task_id).toMatch(/^[a-f0-9]{32}$/)
  const detail = await fetchTaskDetail(task.task_id)
  expect(detail.task.task_id).toBe(task.task_id)
  expect((await fetchAgentSettings('mini-1')).agents.length).toBeGreaterThan(0)
  expect(await fetchTurnLog(task.task_id, detail.turns[0].turn_id, 'stdout', 0)).toHaveProperty(
    'data',
  )
  await expect(
    acceptTask(task.task_id, {
      expected_task_id: task.task_id,
      expected_turn_id: null,
      expected_turn_count: 1,
      expected_head_oid: null,
      expected_updated_at_millis: 1,
      expected_state: 'open',
    }),
  ).rejects.toThrow(/read-only/)
  expect(fetch).not.toHaveBeenCalled()
})
