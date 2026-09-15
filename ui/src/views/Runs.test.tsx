import { render, screen } from '@testing-library/react'
import { expect, it } from 'vitest'
import { Runs } from './Runs'
import { snapshot, task } from '@/test/fixtures'

const run = {
  run_id: 'run-1',
  name: 'Parser work',
  max_parallel: 1,
  created_at_millis: 1000,
  progress: { total: 2, active: 1, queued: 1, open: 0, closed: 0, failed_like: 0 },
}

it('explains a run limit when a task is actually blocked by that limit', () => {
  const { rerender } = render(
    <Runs
      snapshot={snapshot({
        runs: [run],
        tasks: [task({ run_id: 'run-1', state: 'queued', blocking_code: 'RUN_MAX_PARALLEL' })],
      })}
    />,
  )
  expect(screen.getByText(/Parallel limit reached/)).toBeInTheDocument()
  expect(screen.getByText('1 at a time')).toBeInTheDocument()
  rerender(
    <Runs
      snapshot={snapshot({
        runs: [run],
        tasks: [task({ run_id: 'run-1', state: 'queued', blocking_code: 'NO_ELIGIBLE_WORKER' })],
      })}
    />,
  )
  expect(screen.queryByText(/Parallel limit reached/)).not.toBeInTheDocument()
})
