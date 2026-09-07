import { CommandList } from '@/components/CommandList'
import { Icon } from '@/components/Icon'
import { duration } from '@/lib/format'
import { FACTS_TTL_MILLIS, factsAge, type Stall } from '@/lib/queue'

/**
 * Shown only when the queue has entries, no slot is running, and at least one
 * worker's agent facts have lapsed — the shape in which the pool goes quiet
 * with every machine up. It names the cause and the one command that clears it.
 */
export function StalledBanner({
  stall,
  unreachable,
  now,
}: {
  stall: Stall
  unreachable: string[]
  now: number
}) {
  const ages = stall.lapsed
    .map((worker) => {
      const age = factsAge(worker, now)
      return age == null ? null : `${worker.name} ${duration(age)} ago`
    })
    .filter((line): line is string => line != null)

  return (
    <section className="flex flex-wrap overflow-hidden rounded-[10px] border border-observatory-highlight-line bg-observatory-highlight">
      <span className="w-[3px] shrink-0 bg-primary" aria-hidden="true" />

      <div className="min-w-75 flex-1 space-y-3 px-6.5 py-5">
        <p className="flex items-center gap-2.5 text-primary">
          <Icon name="alert" size={15} />
          <span className="font-mono text-[11px] tracking-[0.1em] uppercase">
            Nothing has dispatched for {duration(stall.oldestWaitMillis)}
          </span>
        </p>
        <p className="max-w-165 text-xl leading-7 tracking-[-0.01em]">
          {stall.waiting} {stall.waiting === 1 ? 'entry is' : 'entries are'} waiting and no worker
          advertises an agent right now.
        </p>
        <p className="max-w-165 text-[13px] leading-5 text-muted-foreground">
          The pool learns what each agent can do from facts that expire{' '}
          {duration(FACTS_TTL_MILLIS)} after they are collected
          {ages.length > 0 ? `. Last collected: ${ages.join(', ')}` : ''}. The machines can be up
          and still take no work.
        </p>
      </div>

      <div className="w-full max-w-130 shrink-0 space-y-2.5 border-observatory-highlight-line px-6.5 py-5 lg:border-l">
        <p className="flex items-center gap-2 text-observatory-accent-soft">
          <Icon name="wrench" size={12} />
          <span className="font-mono text-[10px] tracking-[0.06em] uppercase">What fixes it</span>
        </p>
        <CommandList commands={['worker workers --refresh']} />
        <p className="text-xs leading-5 text-muted-foreground">
          {unreachable.length > 0
            ? `Refresh is all-or-nothing. It will fail while ${unreachable.join(', ')} ${
                unreachable.length === 1 ? 'is' : 'are'
              } unreachable — take ${
                unreachable.length === 1 ? 'it' : 'them'
              } out of the inventory first, or bring ${
                unreachable.length === 1 ? 'it' : 'them'
              } back.`
            : 'Refresh is all-or-nothing: every worker in the inventory must answer.'}
        </p>
      </div>
    </section>
  )
}
