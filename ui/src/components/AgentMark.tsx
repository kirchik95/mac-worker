import type { ReactNode } from 'react'
import { agentLabel } from '@/lib/agents'

export function AgentMark({
  agent,
  size = 16,
  label = false,
}: {
  agent: string
  size?: number
  label?: boolean
}) {
  let mark: ReactNode
  if (agent === 'codex')
    mark = (
      <svg width={size} height={size} viewBox="0 0 24 24" aria-hidden="true" className="shrink-0">
        <path
          d="M22.282 9.821a5.985 5.985 0 0 0-.5157-4.911 6.046 6.046 0 0 0-6.51-2.9A6.065 6.065 0 0 0 4.981 4.182a5.985 5.985 0 0 0-3.998 2.9 6.046 6.046 0 0 0 .7427 7.097 5.98 5.98 0 0 0 .511 4.911 6.051 6.051 0 0 0 6.515 2.9A5.985 5.985 0 0 0 13.26 24a6.056 6.056 0 0 0 5.772-4.206 5.989 5.989 0 0 0 3.998-2.9 6.056 6.056 0 0 0-.7475-7.073zm-9.022 12.608a4.476 4.476 0 0 1-2.876-1.041l.1419-.0804 4.778-2.758a.7948.795 0 0 0 .3927-.6813v-6.737l2.02 1.169a.071.071 0 0 1 .038.052v5.583a4.504 4.504 0 0 1-4.495 4.494zm-9.661-4.125a4.471 4.471 0 0 1-.5346-3.014l.142.085 4.783 2.758a.7712.771 0 0 0 .7806 0l5.843-3.369v2.332a.804.08 0 0 1-.332.062L9.74 19.95a4.499 4.499 0 0 1-6.141-1.646zM2.341 7.896a4.485 4.485 0 0 1 2.365-1.973V11.6a.7664.766 0 0 0 .3879.677l5.814 3.354-2.02 1.169a.757.076 0 0 1-.071 0l-4.83-2.787A4.504 4.504 0 0 1 2.341 7.872zm16.596 3.856L13.104 8.364 15.119 7.2a.757.076 0 0 1 .071 0l4.83 2.791a4.494 4.494 0 0 1-.6765 8.104v-5.677a.79.79 0 0 0-.407-.667zm2.011-3.023l-.142-.0852-4.774-2.782a.7759.776 0 0 0-.7854 0L9.409 9.23V6.897a.662.066 0 0 1 .0284-.0615l4.83-2.787a4.499 4.499 0 0 1 6.68 4.66zM8.306 12.863l-2.02-1.164a.804.08 0 0 1-.038-.0567V6.074a4.499 4.499 0 0 1 7.376-3.454l-.142.081L8.704 5.459a.7948.795 0 0 0-.3927.681zm1.098-2.365l2.602-1.5 2.607 1.5v2.999l-2.597 1.5-2.607-1.5Z"
          fill="#0B0B0B"
        />
      </svg>
    )
  else if (agent === 'cursor')
    mark = (
      <svg width={size} height={size} viewBox="0 0 24 24" aria-hidden="true" className="shrink-0">
        <path
          d="M11.503.131 1.891 5.678a.84.84 0 0 0-.42.726v11.188c0 .3.162.575.42.724l9.609 5.55a1 1 0 0 0 .998 0l9.61-5.55a.84.84 0 0 0 .42-.724V6.404a.84.84 0 0 0-.42-.726L12.497.131a1.01 1.01 0 0 0-.996 0M2.657 6.338h18.55c.263 0 .43.287.297.515L12.23 22.918c-.062.107-.229.064-.229-.06V12.335a.59.59 0 0 0-.295-.51l-9.11-5.257c-.109-.063-.064-.23.061-.23"
          fill="#0B0B0B"
        />
      </svg>
    )
  else if (agent === 'opencode')
    mark = (
      <svg
        width={size}
        height={size}
        viewBox="128 96 256 320"
        aria-hidden="true"
        className="shrink-0"
      >
        <path d="M320 224V352H192V224H320Z" fill="#A8A6A5" />
        <path
          fillRule="evenodd"
          clipRule="evenodd"
          d="M384 416H128V96H384V416ZM320 160H192V352H320V160Z"
          fill="#1C1A1A"
        />
      </svg>
    )
  else
    mark = (
      <span
        aria-hidden="true"
        className="shrink-0 text-[11px] font-medium text-muted-foreground"
        style={{ width: size, textAlign: 'center' }}
      >
        {agent === 'claude' ? 'CC' : agent.slice(0, 2).toUpperCase()}
      </span>
    )
  return (
    <span className="inline-flex items-center gap-2">
      {mark}
      {label ? <span>{agentLabel(agent)}</span> : null}
    </span>
  )
}
