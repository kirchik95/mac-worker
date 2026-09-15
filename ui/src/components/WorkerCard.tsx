import { useId, useState } from 'react'
import { ChevronRight, Info, Settings } from 'lucide-react'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { AgentMark } from '@/components/AgentMark'
import { MacIllustration } from '@/components/MacIllustration'
import { HerdrChip } from '@/components/HerdrChip'
import { WorkerObservation } from '@/components/WorkerObservation'
import { bytes, humanize, relativeTime, shortId } from '@/lib/format'
import { agentLabel } from '@/lib/agents'
import { activeJobIds, describeError, slotBusy, type AgentFact, type Worker } from '@/lib/api'
import './worker-card.css'

function WorkerAgent({
  agent,
  workerName,
  factsCurrent,
  activity,
  onSetup,
}: {
  agent: AgentFact
  workerName: string
  factsCurrent: boolean
  activity: string
  onSetup?: (worker?: string, agent?: string) => void
}) {
  const [open, setOpen] = useState(false)
  const triggerId = useId()
  const auth = agent.auth === 'authenticated'
    ? 'Signed in'
    : agent.auth === 'unauthenticated' ? 'Sign-in needed' : 'Sign-in status unknown'
  const login = factsCurrent
    ? auth
    : 'Sign-in unverified' + (agent.auth === 'unknown' ? '' : ' · last reported ' + auth.toLowerCase())

  return (
    <Tooltip open={open} onOpenChange={setOpen} triggerId={triggerId}>
      <TooltipTrigger
        id={triggerId}
        render={onSetup ? <button type="button" /> : <span role="img" tabIndex={0} />}
        className="mw-worker-agent-logo"
        data-tone={agent.auth === 'unknown' ? 'unknown' : factsCurrent && agent.auth === 'unauthenticated' ? 'sign-in-needed' : 'default'}
        aria-label={agentLabel(agent.name) + (onSetup ? ' settings for ' + workerName : ' on ' + workerName)}
        aria-describedby={open ? triggerId + '-description' : undefined}
        closeOnClick={Boolean(onSetup)}
        onClick={onSetup ? () => onSetup(workerName, agent.name) : undefined}
        onPointerUp={(event) => {
          if (!onSetup && event.pointerType !== 'mouse') {
            event.currentTarget.focus()
            setOpen(true)
          }
        }}
      >
        <AgentMark agent={agent.name} size={16} />
      </TooltipTrigger>
      <TooltipContent id={triggerId + '-description'} role="tooltip" className="mw-worker-tooltip">
        <strong>{agentLabel(agent.name)}</strong>{' '}
        <span>{login}</span>{' '}
        <span>{activity}</span>
      </TooltipContent>
    </Tooltip>
  )
}

export function WorkerCard({
  worker,
  now,
  onSetup,
  onSelectTask,
}: {
  worker: Worker
  now: number
  onSetup?: (worker?: string, agent?: string) => void
  onSelectTask?: (id: string) => void
}) {
  const offline = worker.health === 'unavailable'
  const known = !offline && worker.freshness === 'current' && worker.slot.state !== 'unknown'
  const busy = slotBusy(worker.slot)
  const running = known && busy > 0 && worker.active_task != null
  const task = worker.active_task
  const presence = offline ? 'offline' : !known ? 'stale' : busy > 0 ? 'running' : 'available'
  const jobs = activeJobIds(worker.slot)
  const facts = worker.agent_facts
  const factsCurrent = known && facts?.freshness === 'current'
  const agents = [...(facts?.agents ?? [])].sort(
    (left, right) => Number(left.auth === 'unknown') - Number(right.auth === 'unknown'),
  )
  const signIn = agents.filter((agent) => agent.auth === 'unauthenticated')
  const described = describeError(worker.error)
  return (
    <TooltipProvider delay={300} timeout={400}>
      <Collapsible render={<article />} className="mw-panel mw-worker-card">
        <div className="mw-worker-top">
          <div className="mw-worker-visual">
            <MacIllustration state={running ? 'working' : known ? 'idle' : 'unknown'} />
            <div className="mw-slots" aria-label={known ? 'Worker slots' : 'Last reported slots'}>
              {known ? (
                Array.from({ length: Math.min(worker.slot.capacity, 8) }, (_, index) => (
                  <span
                    key={index}
                    className="mw-slot"
                    data-busy={index < busy}
                    title={index < busy ? (jobs[index] ?? 'Occupied slot') : 'Free slot'}
                  >
                    {busy === 1 && index === 0 && task ? (
                      <AgentMark agent={task.agent} size={12} />
                    ) : (
                      <span aria-hidden="true">·</span>
                    )}
                    {index + 1}
                  </span>
                ))
              ) : (
                <span className="mw-slot">Unknown</span>
              )}
              {known && worker.slot.capacity > 8 ? (
                <span className="mw-help">+{worker.slot.capacity - 8}</span>
              ) : null}
            </div>
            <p className="mt-1 text-center text-[10px] text-muted-foreground">Slots</p>
          </div>
          <div className="mw-worker-copy">
            <div className="mw-worker-heading">
              <h2 className="mw-worker-name">{worker.name}</h2>
              <button
                type="button"
                className="mw-button mw-worker-settings"
                data-variant="ghost"
                data-size="icon"
                aria-label={'Agent settings for ' + worker.name}
                title="Agent settings"
                onClick={() => onSetup?.(worker.name, signIn[0]?.name)}
              >
                <Settings size={16} aria-hidden="true" />
              </button>
            </div>
            <div className="mw-worker-state-line">
              <p className="mw-worker-state">
                {!known
                  ? (offline ? 'Offline' : 'Stale') + ' · capacity unknown'
                  : busy > 0
                    ? (running ? 'Working' : 'Occupied') +
                      ' · ' +
                      busy +
                      ' of ' +
                      worker.slot.capacity +
                      ' slots busy'
                    : 'Idle · ' + Math.max(0, worker.slot.capacity - busy) + ' slots free'}
              </p>
              <WorkerObservation
                workerName={worker.name}
                observedAt={worker.observed_at_millis}
                current={known}
                now={now}
              />
            </div>
            {running && task ? (
              <p className="text-[13px] text-muted-foreground">
                {task.agent + ' · turn ' + task.turn_number}
              </p>
            ) : null}
            <div className="mt-3 space-y-1">
              {agents.length ? (
                <div className="mw-worker-agents">
                  <span>Agents:</span>
                  <div className="mw-worker-agent-logos">
                    {agents.map((agent) => {
                      const active = running && task?.agent === agent.name
                      const activity = active
                        ? 'Running current task · turn ' + task.turn_number
                        : !known ? 'Activity unverified'
                          : busy > 0 ? 'No reported active task' : 'Not running a task'
                      return (
                        <WorkerAgent
                          key={agent.name}
                          agent={agent}
                          workerName={worker.name}
                          factsCurrent={factsCurrent}
                          activity={activity}
                          onSetup={onSetup}
                        />
                      )
                    })}
                  </div>
                </div>
              ) : (
                <p className="mw-worker-agent">
                  <Info size={14} aria-hidden="true" />
                  Agent status not reported
                </p>
              )}
              {signIn.map((agent) => (
                <p
                  key={agent.name}
                  className={'mw-worker-agent ' + (factsCurrent ? '!text-warning' : '')}
                >
                  <AgentMark agent={agent.name} size={13} />
                  <span>
                    {agentLabel(agent.name)}{' '}
                    {factsCurrent ? 'needs sign-in on Mac' : 'sign-in unverified'}
                  </span>
                </p>
              ))}
              {factsCurrent && agents.some((agent) => agent.auth === 'unknown') ? (
                <p className="mw-help">Authentication not reported</p>
              ) : null}
            </div>
            <div className="mw-worker-actions">
              {signIn.length > 0 ? (
                <button
                  type="button"
                  className="mw-link font-medium underline underline-offset-4"
                  onClick={() => onSetup?.(worker.name, signIn[0].name)}
                >
                  Setup instructions
                </button>
              ) : null}
              <CollapsibleTrigger className="mw-link">
                Worker details
                <ChevronRight className="mw-disclosure-chevron" size={13} />
              </CollapsibleTrigger>
            </div>
          </div>
        </div>
        <CollapsibleContent keepMounted>
          <div className="mw-worker-diagnostics">
            <div className="mb-4 flex items-center justify-between gap-3">
              <span className="text-xs text-muted-foreground">{presence}</span>
              <HerdrChip herdr={worker.herdr} />
            </div>
            {!known ? (
              <p className="mb-3 text-sm">{described?.message ?? 'Observation is out of date'}</p>
            ) : (
              <p className="mb-3 text-sm">{task?.title ?? 'Ready for the next task'}</p>
            )}
            {described?.code ? (
              <p className="mb-3 font-mono text-xs text-destructive">{described.code}</p>
            ) : null}
            <dl className="grid grid-cols-3 gap-3">
              <div>
                <dt className="mw-help">{known ? 'CPU' : 'LAST CPU'}</dt>
                <dd>
                  {worker.system.cpu_busy_percent == null
                    ? '—'
                    : worker.system.cpu_busy_percent.toFixed(1) + '%'}
                </dd>
              </div>
              <div>
                <dt className="mw-help">MEMORY</dt>
                <dd>{humanize(worker.system.memory_pressure)}</dd>
              </div>
              <div>
                <dt className="mw-help">DISK FREE</dt>
                <dd>{bytes(worker.system.free_disk_bytes)}</dd>
              </div>
            </dl>
            <p className="mt-3 text-xs text-muted-foreground">
              {known
                ? busy + ' / ' + worker.slot.capacity + ' slots occupied'
                : 'Cached metrics · Last seen ' + relativeTime(worker.observed_at_millis, now)}
            </p>
            {jobs.length > 1 ? (
              <p className="mt-2 break-all font-mono text-xs">
                {jobs.map((id) => shortId(id, 8)).join(' · ')}
              </p>
            ) : null}
            {task ? (
              <div className="mt-4 border-t pt-4">
                <dl className="grid grid-cols-3 gap-3">
                  <div>
                    <dt className="mw-help">AGENT</dt>
                    <dd>{humanize(task.agent)}</dd>
                  </div>
                  <div>
                    <dt className="mw-help">MODEL</dt>
                    <dd>{task.model ?? 'Agent default'}</dd>
                  </div>
                  <div>
                    <dt className="mw-help">EFFORT</dt>
                    <dd>{task.effort ? humanize(task.effort) : 'Not reported'}</dd>
                  </div>
                </dl>
                <button
                  type="button"
                  className="mw-link mt-3"
                  onClick={() => onSelectTask?.(task.task_id)}
                >
                  Open task
                  <ChevronRight size={13} />
                </button>
              </div>
            ) : null}
          </div>
        </CollapsibleContent>
      </Collapsible>
    </TooltipProvider>
  )
}
