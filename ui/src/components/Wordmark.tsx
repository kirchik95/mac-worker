/** The gold tile and wordmark from the Overview artboard. */
export function Wordmark() {
  return (
    <div className="flex w-64 shrink-0 items-center gap-3">
      <div className="flex size-8 shrink-0 items-center justify-center rounded-full bg-observatory-gold">
        <svg width="17" height="17" viewBox="0 0 24 24" aria-hidden="true">
          {[
            [3, 3],
            [14, 3],
            [3, 14],
            [14, 14],
          ].map(([x, y]) => (
            <rect
              key={`${x}-${y}`}
              x={x}
              y={y}
              width="7"
              height="7"
              rx="1.5"
              fill="none"
              stroke="rgb(21 23 25)"
              strokeWidth="1.6"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          ))}
        </svg>
      </div>
      <span className="text-[20px] leading-6 font-medium tracking-[-0.04em]">mac-worker</span>
    </div>
  )
}
