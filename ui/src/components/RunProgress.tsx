import { cn } from '@/lib/utils'
import type { RunRow } from '@/lib/api'

/** One segment per task in the run, as the artboard draws it. */
export function RunProgress({ run }: { run: RunRow }) {
  const { progress } = run
  const done = progress.closed
  const running = progress.active
  const segments = Array.from({ length: Math.max(progress.total, 1) }, (_, index) =>
    index < done ? 'done' : index < done + running ? 'running' : 'idle',
  )

  return (
    <section className="rounded-[10px] border bg-card p-5">
      <div className="flex items-baseline gap-3">
        <h2 className="min-w-0 flex-1 truncate text-[17px] leading-6">
          {run.name ?? 'Unnamed run'}
        </h2>
        <span className="shrink-0 font-mono text-sm text-muted-foreground">
          {done} / {progress.total}
        </span>
      </div>
      <p className="mt-1.5 text-xs text-muted-foreground">
        {done} completed · {running} running · parallel limit {run.max_parallel}
      </p>
      <div className="mt-4 flex gap-1.5">
        {segments.map((state, index) => (
          <span
            key={index}
            className={cn(
              'h-1.5 flex-1 rounded-full',
              state === 'done' && 'bg-observatory-accent-soft',
              state === 'running' && 'bg-observatory-accent-soft/50',
              state === 'idle' && 'bg-muted',
            )}
          />
        ))}
      </div>
    </section>
  )
}
