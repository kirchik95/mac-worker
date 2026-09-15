import { useState } from 'react'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it } from 'vitest'

import { ModelPicker } from './ModelPicker'

const options = [
  { id: 'astra', label: 'Astra' },
  { id: 'native-2026-mini', label: 'Compact' },
  { id: 'default', label: 'Auto' },
]

function Picker({ initialValue = 'astra' }: { initialValue?: string | null }) {
  const [value, setValue] = useState(initialValue)
  return (
    <>
      <ModelPicker options={options} value={value} onValueChange={setValue} />
      <output aria-label="Selected model">{JSON.stringify(value)}</output>
    </>
  )
}

describe('ModelPicker', () => {
  it.each(['CoMpAcT', 'NATIVE-2026'])('finds a model by label or native ID: %s', async (query) => {
    const user = userEvent.setup()
    render(<Picker />)
    const trigger = screen.getByRole('combobox', { name: 'Model' })

    await user.click(trigger)
    await user.type(await screen.findByRole('combobox', { name: 'Search models' }), query)
    expect(screen.getByRole('option', { name: 'Compact' })).toBeInTheDocument()
    expect(screen.queryByRole('option', { name: 'Astra' })).not.toBeInTheDocument()
    expect(trigger).toHaveTextContent('Astra')
    expect(screen.getByLabelText('Selected model')).toHaveTextContent('"astra"')

    await user.keyboard('{ArrowDown}{Enter}')
    expect(trigger).toHaveTextContent('Compact')
    expect(screen.getByLabelText('Selected model')).toHaveTextContent('"native-2026-mini"')
  })

  it('keeps an unlisted current model available and searchable', async () => {
    const user = userEvent.setup()
    render(<Picker initialValue="private-alpha" />)
    const trigger = screen.getByRole('combobox', { name: 'Model' })
    expect(trigger).toHaveTextContent('private-alpha')

    await user.click(trigger)
    expect(await screen.findByRole('option', { name: 'private-alpha' })).toHaveAttribute(
      'aria-selected',
      'true',
    )
    await user.type(await screen.findByRole('combobox', { name: 'Search models' }), 'PRIVATE')
    expect(screen.getByRole('option', { name: 'private-alpha' })).toBeInTheDocument()
    await user.keyboard('{Escape}')
    expect(screen.getByLabelText('Selected model')).toHaveTextContent('"private-alpha"')
  })

  it('distinguishes Agent default from a model whose native ID is default', async () => {
    const user = userEvent.setup()
    render(<Picker initialValue={null} />)
    const trigger = screen.getByRole('combobox', { name: 'Model' })
    expect(trigger).toHaveTextContent('Agent default')

    await user.click(trigger)
    await user.click(await screen.findByRole('option', { name: 'Auto' }))
    expect(trigger).toHaveTextContent('Auto')
    expect(screen.getByLabelText('Selected model')).toHaveTextContent('"default"')

    await user.click(trigger)
    await user.click(await screen.findByRole('option', { name: 'Agent default' }))
    expect(trigger).toHaveTextContent('Agent default')
    expect(screen.getByLabelText('Selected model')).toHaveTextContent('null')
  })
})
