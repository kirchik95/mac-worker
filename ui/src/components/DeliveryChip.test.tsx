import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'

import { originDelivery } from '@/test/fixtures'
import { DeliveryChip, deliveryLabel } from './DeliveryChip'

describe('DeliveryChip', () => {
  it('renders nothing when the task has no origin delivery', () => {
    const { container } = render(<DeliveryChip task={{}} />)
    expect(container).toBeEmptyDOMElement()
  })

  it('shows the last delivery state from deliveries', () => {
    render(
      <DeliveryChip
        task={{
          deliveries: [originDelivery({ state: 'retrying', last_error: 'ORIGIN_AUTH_FAILED' })],
        }}
      />,
    )
    expect(screen.getByText('Retrying push')).toBeInTheDocument()
    expect(screen.queryByText(/Last known/)).toBeNull()
  })

  it('marks a last-observed delivery as stale', () => {
    render(
      <DeliveryChip
        task={{ delivery: originDelivery({ state: 'failed' }) }}
        freshness="stale"
      />,
    )
    expect(screen.getByText('Last known: Push failed')).toBeInTheDocument()
  })

  it('labels a stale delivery as last known', () => {
    expect(
      deliveryLabel(originDelivery({ state: 'pending', turn_id: 'f'.repeat(32), updated_at_millis: 1_000 }), 'stale'),
    ).toBe('Last known: Push pending')
  })
})
