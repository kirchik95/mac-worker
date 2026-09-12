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
    expect(screen.getByText('retrying')).toBeInTheDocument()
    expect(screen.queryByText('stale')).toBeNull()
  })

  it('marks a last-observed delivery as stale', () => {
    render(
      <DeliveryChip
        task={{ delivery: originDelivery({ state: 'failed' }) }}
        freshness="stale"
      />,
    )
    expect(screen.getByText('failed')).toBeInTheDocument()
    expect(screen.getByText('stale')).toBeInTheDocument()
  })

  it('labels a stale delivery with its observed time', () => {
    expect(
      deliveryLabel(originDelivery({ state: 'pending', turn_id: 'f'.repeat(32), updated_at_millis: 1_000 }), 'stale'),
    ).toMatch(/^pending · ffffffff · observed /)
  })
})
