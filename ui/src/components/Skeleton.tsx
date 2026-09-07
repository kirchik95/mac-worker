import { cn } from '@/lib/utils'

/** A placeholder bar in the artboard's skeleton grammar. */
function Bar({ className }: { className?: string }) {
  return <span className={cn('block rounded-[2px] bg-observatory-line', className)} />
}

function WorkerSkeleton() {
  return (
    <article className="flex flex-col rounded-[10px] border bg-card p-5">
      <div className="flex items-center gap-3">
        <Bar className="h-[22px] w-7 rounded" />
        <Bar className="h-[18px] w-26" />
        <Bar className="ml-auto h-2.5 w-[74px]" />
      </div>
      <div className="mt-5 grid grid-cols-3 gap-6">
        {[
          ['w-[34px]', 'w-22 h-6'],
          ['w-[54px]', 'w-12 h-4'],
          ['w-[62px]', 'w-[70px] h-4'],
        ].map(([label, value], index) => (
          <div key={index} className="flex flex-col gap-2.5">
            <Bar className={cn('h-[9px]', label)} />
            <Bar className={value} />
          </div>
        ))}
      </div>
      <div className="mt-5 flex flex-col gap-3 border-t pt-4">
        <Bar className="h-3 w-[150px]" />
        <Bar className="h-2.5 w-26" />
      </div>
    </article>
  )
}

/** The page as it stands before the first snapshot arrives. */
export function Skeleton() {
  return (
    <div className="space-y-5" aria-busy="true" aria-label="Loading the first snapshot">
      <div className="flex items-center gap-5 pb-1">
        <Bar className="h-3 w-[268px]" />
        <Bar className="ml-auto h-3 w-24" />
        <Bar className="h-3 w-18" />
      </div>

      <div className="grid gap-5 md:grid-cols-2 xl:grid-cols-3">
        <WorkerSkeleton />
        <WorkerSkeleton />
        <WorkerSkeleton />
      </div>

      <div className="grid items-start gap-5 lg:grid-cols-[minmax(0,1fr)_minmax(0,2fr)]">
        <div className="space-y-5">
          <section className="rounded-[10px] border bg-card p-5">
            <div className="flex items-center gap-3">
              <Bar className="h-4 w-[74px]" />
              <Bar className="ml-auto h-[9px] w-[78px]" />
            </div>
            <div className="mt-5 flex flex-col gap-3.5">
              <Bar className="h-3.5 w-[210px]" />
              <div className="flex gap-5">
                <Bar className="h-2.5 w-20" />
                <Bar className="h-2.5 w-24" />
                <Bar className="h-2.5 w-22" />
              </div>
              <Bar className="h-2.5 w-[164px]" />
            </div>
          </section>

          <section className="flex items-center gap-3 rounded-[10px] border bg-card px-5 py-4">
            <Bar className="size-4 rounded" />
            <Bar className="h-3 w-[186px]" />
          </section>

          <section className="rounded-[10px] border bg-card p-5">
            <div className="flex items-center gap-3">
              <Bar className="h-3.5 w-[172px]" />
              <Bar className="ml-auto h-3 w-[34px]" />
            </div>
            <Bar className="mt-3.5 h-2.5 w-[238px]" />
            <div className="mt-3.5 flex gap-1.5">
              <Bar className="h-1.5 flex-1 rounded-full" />
              <Bar className="h-1.5 flex-1 rounded-full" />
              <Bar className="h-1.5 flex-1 rounded-full" />
            </div>
          </section>
        </div>

        <section className="flex h-78 flex-col rounded-[10px] border bg-card">
          <div className="flex items-start gap-6 p-5">
            <div className="flex flex-1 flex-col gap-3">
              <Bar className="h-[9px] w-[186px]" />
              <Bar className="h-5 w-[250px]" />
            </div>
            <div className="flex gap-8">
              <Bar className="h-3 w-[62px]" />
              <Bar className="h-3 w-[62px]" />
              <Bar className="h-3 w-[62px]" />
            </div>
          </div>
          <div className="flex items-center gap-6 border-y px-5 py-3">
            <Bar className="h-3 w-24" />
            <Bar className="h-3 w-22" />
            <Bar className="h-3 w-19" />
          </div>
          <div className="flex flex-col gap-3.5 p-5">
            {['w-72', 'w-52', 'w-86', 'w-43'].map((width, index) => (
              <div key={index} className={cn('flex items-center gap-6', index === 3 && 'opacity-55')}>
                <Bar className="h-2.5 w-15" />
                <Bar className="h-2.5 w-14" />
                <Bar className={cn('h-2.5', width)} />
              </div>
            ))}
          </div>
        </section>
      </div>
    </div>
  )
}
