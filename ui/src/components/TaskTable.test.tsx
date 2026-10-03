import { render, screen } from '@testing-library/react'
import { expect, it } from 'vitest'
import { task } from '@/test/fixtures'
import fixtures from '@/lib/integration.fixtures.json'
import { decodeIntegrationView } from '@/lib/integration.contract'
import { TaskTable } from './TaskTable'

it('shows integration target and code separately from ordinary delivery', () => {
  const view = decodeIntegrationView(fixtures.cases.find(row => row.view.integration?.state === 'blocked')!.view)
  render(<TaskTable tasks={[{ ...task({ state: 'open' }), ...view }]} />)
  expect(screen.getByText(view.integration!.blocked_code!)).toBeInTheDocument()
  expect(screen.getByText(view.integration!.target, { exact: false })).toBeInTheDocument()
})
