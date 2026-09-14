import { fireEvent, renderHook } from '@testing-library/react'
import { expect, it } from 'vitest'
import { useInputModality } from './useInputModality'

it('keeps keyboard and assistive activations immediate, including inside portals', () => {
  const { unmount } = renderHook(useInputModality)
  const root = document.documentElement
  expect(root.dataset.inputModality).toBe('keyboard')

  fireEvent.pointerDown(document.body)
  expect(root.dataset.inputModality).toBe('pointer')
  fireEvent.keyDown(document.body, { key: 'Escape' })
  expect(root.dataset.inputModality).toBe('keyboard')

  fireEvent.pointerDown(document.body)
  fireEvent.click(document.body, { detail: 1 })
  expect(root.dataset.inputModality).toBe('pointer')
  fireEvent.click(document.body, { detail: 0 })
  expect(root.dataset.inputModality).toBe('keyboard')
  unmount()
})

it('removes its listeners and document attribute when the app unmounts', () => {
  const { unmount } = renderHook(useInputModality)
  unmount()
  fireEvent.pointerDown(document.body)
  expect(document.documentElement).not.toHaveAttribute('data-input-modality')
})
