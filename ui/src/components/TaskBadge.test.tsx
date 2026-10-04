import { render, screen } from '@testing-library/react'
import { expect, it } from 'vitest'
import { task } from '@/test/fixtures'
import fixtures from '@/lib/integration.fixtures.json'
import { decodeIntegrationView } from '@/lib/integration.contract'
import { TaskBadge } from './TaskBadge'

it('renders integrated success for an Open task', () => {
  render(<TaskBadge task={{ ...task({ state: 'open' }),
    ...decodeIntegrationView(fixtures.cases.find(row => row.name === 'integrated')!.view) }} />)
  expect(screen.getByText('Integrated')).toBeInTheDocument()
})
