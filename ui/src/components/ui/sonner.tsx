import type { CSSProperties } from 'react'
import { Toaster as Sonner } from 'sonner'
import './sonner.css'

export function Toaster() {
  return (
    <Sonner
      theme="light"
      className="mw-toaster"
      position="bottom-right"
      offset={24}
      mobileOffset={16}
      visibleToasts={1}
      closeButton
      swipeDirections={[]}
      style={
        {
          '--normal-bg': 'var(--card)',
          '--normal-text': 'var(--foreground)',
          '--normal-border': 'var(--border)',
          '--border-radius': '10px',
          '--width': '400px',
        } as CSSProperties
      }
    />
  )
}
