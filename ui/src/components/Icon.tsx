import type { ReactNode } from 'react'

import { cn } from '@/lib/utils'

/**
 * The line-icon set the artboards use. One 16-unit grid, one stroke weight, and
 * `currentColor` throughout, so an icon always takes the colour of the label it
 * sits beside and never introduces a palette of its own.
 */
const PATHS = {
  alert: (
    <>
      <path d="M8 2.1 14.6 13.4H1.4z" />
      <path d="M8 6.4v3" />
      <circle cx="8" cy="11.4" r=".9" fill="currentColor" stroke="none" />
    </>
  ),
  archive: <path d="M2 4.6h12M2.9 4.6v8.2c0 .6.5 1.1 1.1 1.1h8c.6 0 1.1-.5 1.1-1.1V4.6M5.2 2.2h5.6l1.6 2.4H3.6z" />,
  bell: (
    <>
      <path d="M12.6 6.2c0-2.5-2.1-4.5-4.6-4.5S3.4 3.7 3.4 6.2c0 4.1-1.6 5.3-1.6 5.3h12.4s-1.6-1.2-1.6-5.3" />
      <path d="M6.7 13.9a1.6 1.6 0 0 0 2.6 0" />
    </>
  ),
  branch: (
    <>
      <circle cx="4.6" cy="3.6" r="1.9" />
      <circle cx="4.6" cy="12.4" r="1.9" />
      <circle cx="11.6" cy="3.6" r="1.9" />
      <path d="M4.6 5.5v5M11.6 5.5v1.2c0 1.6-1.3 2.9-2.9 2.9H4.6" />
    </>
  ),
  broadcast: (
    <>
      <circle cx="8" cy="8" r="1.7" />
      <path d="M4.4 4.4a5 5 0 0 0 0 7.2M11.6 4.4a5 5 0 0 1 0 7.2M2.2 2.2a8.2 8.2 0 0 0 0 11.6M13.8 2.2a8.2 8.2 0 0 1 0 11.6" />
    </>
  ),
  broadcastOff: (
    <>
      <circle cx="8" cy="8" r="1.7" />
      <path d="M4.4 4.4a5 5 0 0 0 0 7.2M11.6 4.4a5 5 0 0 1 0 7.2" />
      <path d="M2.2 13.8 13.8 2.2" />
    </>
  ),
  check: (
    <>
      <circle cx="8" cy="8" r="6" />
      <path d="M5.4 8.2 7.2 10l3.5-3.9" />
    </>
  ),
  chevronDown: <path d="m4.2 6.2 3.8 3.8 3.8-3.8" />,
  chevronLeft: <path d="M9.5 3.5 5 8l4.5 4.5" />,
  chevronRight: <path d="M6.5 3.5 11 8l-4.5 4.5" />,
  clock: (
    <>
      <circle cx="8" cy="8" r="6" />
      <path d="M8 4.6V8l2.4 1.6" />
    </>
  ),
  clockOpen: (
    <>
      <path d="M13.4 8.6A5.6 5.6 0 1 1 8 2.4" />
      <path d="M8 4.8V8l2.2 1.5" />
    </>
  ),
  commit: (
    <>
      <circle cx="8" cy="8" r="2.8" />
      <path d="M2 8h3.2M10.8 8H14" />
    </>
  ),
  copy: (
    <>
      <rect x="5.6" y="5.6" width="8.2" height="8.2" rx="1.4" />
      <path d="M10.6 5.6V4c0-.8-.6-1.4-1.4-1.4H3.6c-.8 0-1.4.6-1.4 1.4v5.6c0 .8.6 1.4 1.4 1.4h2" />
    </>
  ),
  cpu: (
    <>
      <rect x="3.9" y="3.9" width="8.2" height="8.2" rx="1.2" />
      <path d="M6.3 1.6v2.3M9.7 1.6v2.3M6.3 12.1v2.3M9.7 12.1v2.3M1.6 6.3h2.3M1.6 9.7h2.3M12.1 6.3h2.3M12.1 9.7h2.3" />
    </>
  ),
  cpuBusy: (
    <>
      <rect x="3.9" y="3.9" width="8.2" height="8.2" rx="1.2" />
      <rect x="6.4" y="6.4" width="3.2" height="3.2" rx=".6" fill="currentColor" stroke="none" />
      <path d="M6.3 1.6v2.3M9.7 1.6v2.3M6.3 12.1v2.3M9.7 12.1v2.3M1.6 6.3h2.3M1.6 9.7h2.3M12.1 6.3h2.3M12.1 9.7h2.3" />
    </>
  ),
  cpuOff: (
    <>
      <rect x="3.9" y="3.9" width="8.2" height="8.2" rx="1.2" />
      <path d="M6.3 1.6v2.3M9.7 1.6v2.3M6.3 12.1v2.3M9.7 12.1v2.3M1.6 6.3h2.3M1.6 9.7h2.3M12.1 6.3h2.3M12.1 9.7h2.3" />
      <path d="m2.4 13.6 11.2-11.2" />
    </>
  ),
  download: <path d="M8 2.2v7.4M5 6.8 8 9.8l3-3M3 13.4h10" />,
  ellipsis: (
    <>
      <circle cx="3.4" cy="8" r="1.1" fill="currentColor" stroke="none" />
      <circle cx="8" cy="8" r="1.1" fill="currentColor" stroke="none" />
      <circle cx="12.6" cy="8" r="1.1" fill="currentColor" stroke="none" />
    </>
  ),
  eyeOff: (
    <>
      <path d="M6.3 3.5a6.6 6.6 0 0 1 7.9 4.5 7 7 0 0 1-1.5 2.5M4.1 4.9A6.9 6.9 0 0 0 1.8 8s1.9 4.7 6.2 4.7c1 0 1.9-.2 2.7-.6" />
      <path d="M2.4 13.6 13.6 2.4" />
    </>
  ),
  file: (
    <>
      <path d="M9 1.9H4.4c-.7 0-1.2.6-1.2 1.2v9.8c0 .7.5 1.2 1.2 1.2h7.2c.7 0 1.2-.5 1.2-1.2V5.6z" />
      <path d="M9 1.9v3.7h3.8" />
    </>
  ),
  fileCheck: (
    <>
      <path d="M9 1.9H4.4c-.7 0-1.2.6-1.2 1.2v9.8c0 .7.5 1.2 1.2 1.2h7.2c.7 0 1.2-.5 1.2-1.2V5.6z" />
      <path d="M9 1.9v3.7h3.8" />
      <path d="m5.9 10 1.4 1.4 2.8-3.2" />
    </>
  ),
  fileCode: (
    <>
      <path d="M9 1.9H4.4c-.7 0-1.2.6-1.2 1.2v9.8c0 .7.5 1.2 1.2 1.2h7.2c.7 0 1.2-.5 1.2-1.2V5.6z" />
      <path d="M9 1.9v3.7h3.8" />
      <path d="m6.6 8.6-1.2 1.3 1.2 1.2M9.4 8.6l1.2 1.3-1.2 1.2" />
    </>
  ),
  fileDiff: <path d="M4.2 13.4V6.2M4.2 6.2 2.1 8.4M4.2 6.2l2.1 2.2M11.8 2.6v7.2M11.8 9.8l2.1-2.2M11.8 9.8 9.7 7.6" />,
  fileText: (
    <>
      <path d="M9 1.9H4.4c-.7 0-1.2.6-1.2 1.2v9.8c0 .7.5 1.2 1.2 1.2h7.2c.7 0 1.2-.5 1.2-1.2V5.6z" />
      <path d="M9 1.9v3.7h3.8M5.4 8.6h5.2M5.4 11h3.4" />
    </>
  ),
  flag: <path d="M3.8 14V2.2M3.8 2.8h8.9l-2 3.2 2 3.2H3.8" />,
  funnel: <path d="M2 3.2h12l-4.6 5.4v4.6L6.6 11.4V8.6z" />,
  gauge: (
    <>
      <path d="M2.4 12.6a6.5 6.5 0 1 1 11.2 0" />
      <path d="m8 9.1 3-3.3" />
    </>
  ),
  grid: (
    <>
      <rect x="2.4" y="2.4" width="4.7" height="4.7" rx=".9" />
      <rect x="8.9" y="2.4" width="4.7" height="4.7" rx=".9" />
      <rect x="2.4" y="8.9" width="4.7" height="4.7" rx=".9" />
      <rect x="8.9" y="8.9" width="4.7" height="4.7" rx=".9" />
    </>
  ),
  hash: <path d="M6.3 2.2 4.9 13.8M11.4 2.2 10 13.8M2.6 5.6h11.2M2.2 10.4h11.2" />,
  history: (
    <>
      <path d="M2.4 8a5.6 5.6 0 1 0 1.7-4" />
      <path d="M2.3 2.4v3.2h3.2" />
      <path d="M8 5.4V8l1.9 1.3" />
    </>
  ),
  hourglass: <path d="M4 2.2h8M4 13.8h8M4.7 2.2c0 4 3.3 4.8 3.3 5.8s-3.3 1.8-3.3 5.8M11.3 2.2c0 4-3.3 4.8-3.3 5.8s3.3 1.8 3.3 5.8" />,
  linkOff: (
    <>
      <path d="M6.4 9.6 4.6 11.4a2.6 2.6 0 0 1-3.7-3.7l1.8-1.8M9.6 6.4l1.8-1.8a2.6 2.6 0 0 1 3.7 3.7l-1.8 1.8" />
      <path d="M2.4 13.6 13.6 2.4" />
    </>
  ),
  list: <path d="M2.5 4h11M2.5 8h11M2.5 12H9" />,
  messageQuestion: (
    <>
      <path d="M13.6 9.6c0 .7-.6 1.3-1.3 1.3H5.4L2.4 13.6V3.7c0-.7.6-1.3 1.3-1.3h8.6c.7 0 1.3.6 1.3 1.3z" />
      <path d="M6.6 5.6c0-1.2 3-1.2 3 .5 0 1.1-1.5 1-1.5 2.2" />
    </>
  ),
  pause: (
    <>
      <rect x="3.4" y="2.8" width="3" height="10.4" rx="1.1" />
      <rect x="9.6" y="2.8" width="3" height="10.4" rx="1.1" />
    </>
  ),
  pencil: <path d="M11.3 2.4 13.6 4.7 5.6 12.7 2.4 13.6l.9-3.2z" />,
  question: (
    <>
      <circle cx="8" cy="8" r="6" />
      <path d="M6.2 6.2c0-1.4 3.7-1.4 3.7.6 0 1.4-1.9 1.3-1.9 2.7" />
      <circle cx="8" cy="11.6" r=".85" fill="currentColor" stroke="none" />
    </>
  ),
  queue: (
    <>
      <path d="M6.2 3.6h7.4M6.2 8h7.4M6.2 12.4h4.6" />
      <circle cx="3" cy="3.6" r="1.1" fill="currentColor" stroke="none" />
      <circle cx="3" cy="8" r="1.1" fill="currentColor" stroke="none" />
      <circle cx="3" cy="12.4" r="1.1" fill="currentColor" stroke="none" />
    </>
  ),
  reply: (
    <>
      <path d="M6.6 3.4 2.8 7.2l3.8 3.8" />
      <path d="M2.8 7.2h6.6c2.1 0 3.8 1.7 3.8 3.8v1.6" />
    </>
  ),
  repeat: (
    <>
      <path d="M13.5 7.2A5.6 5.6 0 0 0 3.4 4.6M2.5 8.8a5.6 5.6 0 0 0 10.1 2.6" />
      <path d="M13.6 2.6v3.1h-3.1M2.4 13.4v-3.1h3.1" />
    </>
  ),
  shield: (
    <>
      <path d="M8 1.8 13.2 3.4v4.1c0 3.2-2.1 5.7-5.2 6.7-3.1-1-5.2-3.5-5.2-6.7V3.4z" />
      <path d="m5.9 7.7 1.5 1.5 2.8-3" />
    </>
  ),
  sliders: (
    <>
      <path d="M2.4 4.6h11.2M2.4 11.4h11.2" />
      <circle cx="6" cy="4.6" r="1.8" />
      <circle cx="10.4" cy="11.4" r="1.8" />
    </>
  ),
  spark: <path d="M8 1.8 9.4 6.6 14.2 8 9.4 9.4 8 14.2 6.6 9.4 1.8 8 6.6 6.6z" />,
  stack: <path d="M8 1.9 14.1 5.2 8 8.5 1.9 5.2zM1.9 10.6 8 13.9l6.1-3.3" />,
  terminal: (
    <>
      <rect x="1.8" y="2.6" width="12.4" height="10.8" rx="1.4" />
      <path d="m4.6 6.4 2.2 2.2-2.2 2.2M8.6 10.8h3" />
    </>
  ),
  toBottom: (
    <>
      <path d="m4.2 6.2 3.8 3.8 3.8-3.8" />
      <path d="M2.6 12.8h10.8" />
    </>
  ),
  wrench: <path d="M10.2 1.9a3.6 3.6 0 0 0-3.4 5.5l-4.9 4.9 1.9 1.9 4.9-4.9a3.6 3.6 0 0 0 4.6-4.6l-2.1 2.1-2-2z" />,
  x: <path d="M3.6 3.6 12.4 12.4M12.4 3.6 3.6 12.4" />,
  xCircle: (
    <>
      <circle cx="8" cy="8" r="6" />
      <path d="m6 6 4 4M10 6l-4 4" />
    </>
  ),
} satisfies Record<string, ReactNode>

export type IconName = keyof typeof PATHS

export function Icon({
  name,
  size = 13,
  className,
}: {
  name: IconName
  size?: number
  className?: string
}) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      strokeWidth={1.3}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      className={cn('shrink-0', className)}
    >
      {PATHS[name]}
    </svg>
  )
}
