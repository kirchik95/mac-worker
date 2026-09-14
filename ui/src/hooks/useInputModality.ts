import { useEffect } from 'react'

/** Shared by the app and its portals; keyboard/assistive actions skip motion. */
export function useInputModality() {
  useEffect(() => {
    const root = document.documentElement
    const previous = root.getAttribute('data-input-modality')
    const keyboard = () => {
      root.dataset.inputModality = 'keyboard'
    }
    const pointer = () => {
      root.dataset.inputModality = 'pointer'
    }
    const click = (event: MouseEvent) => {
      if (event.detail === 0) keyboard()
    }
    keyboard()
    document.addEventListener('keydown', keyboard, true)
    document.addEventListener('pointerdown', pointer, true)
    document.addEventListener('click', click, true)
    return () => {
      document.removeEventListener('keydown', keyboard, true)
      document.removeEventListener('pointerdown', pointer, true)
      document.removeEventListener('click', click, true)
      if (previous === null) root.removeAttribute('data-input-modality')
      else root.setAttribute('data-input-modality', previous)
    }
  }, [])
}
