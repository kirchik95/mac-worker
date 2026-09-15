import type { ReactNode } from 'react'
import { Check, LoaderCircle, TriangleAlert } from 'lucide-react'

export type ActionState = 'idle' | 'pending' | 'success' | 'error'

/** Keep every label in the same grid cell so request feedback never resizes a button. */
export function ActionFeedback({
  state,
  idleLabel,
  pendingLabel,
  successLabel,
  errorLabel,
  idleIcon = <Check size={16} />,
}: {
  state: ActionState
  idleLabel: string
  pendingLabel: string
  successLabel: string
  errorLabel: string
  idleIcon?: ReactNode
}) {
  const states = [
    ['idle', idleLabel, idleIcon],
    [
      'pending',
      pendingLabel,
      <LoaderCircle key="pending" size={16} className="mw-action-spinner" />,
    ],
    ['success', successLabel, <Check key="success" size={16} />],
    ['error', errorLabel, <TriangleAlert key="error" size={16} />],
  ] as const
  return (
    <span className="mw-action-feedback" data-state={state}>
      {states.map(([value, label, icon]) => (
        <span key={value} data-visible={state === value} aria-hidden={state !== value}>
          <span aria-hidden="true">{icon}</span>
          {label}
        </span>
      ))}
    </span>
  )
}
