import { cn } from '@/lib/utils'

function Bar({ className }: { className?: string }) {
  return <span className={cn('block rounded bg-border', className)} />
}
export function Skeleton() {
  return (
    <div className="mw-page" aria-busy="true" aria-label="Loading the first snapshot">
      <Bar className="h-6 w-48" />
      <div className="mw-card-grid">
        {[0, 1, 2].map((index) => (
          <div key={index} className="mw-panel mw-panel-pad space-y-4">
            <Bar className="h-3 w-28" />
            <Bar className="h-5 w-4/5" />
            <Bar className="h-3 w-full" />
            <Bar className="mt-7 h-9 w-28" />
          </div>
        ))}
      </div>
      <Bar className="mt-1 h-6 w-28" />
      <div className="mw-card-grid">
        {[0, 1, 2].map((index) => (
          <div key={index} className="mw-panel flex gap-5 p-5">
            <Bar className="mt-2 h-24 w-24 shrink-0" />
            <div className="min-w-0 flex-1 space-y-4">
              <Bar className="h-6 w-24" />
              <Bar className="h-3 w-full" />
              <Bar className="h-3 w-4/5" />
              <Bar className="h-3 w-4/5" />
              <Bar className="h-3 w-3/5" />
            </div>
          </div>
        ))}
      </div>
      <Bar className="mt-5 h-6 w-32" />
      <div className="mw-panel divide-y">
        {[0, 1, 2].map((index) => (
          <div className="flex items-center gap-6 p-5" key={index}>
            <Bar className="h-5 w-24" />
            <Bar className="h-4 flex-1" />
            <Bar className="h-8 w-32" />
          </div>
        ))}
      </div>
    </div>
  )
}
