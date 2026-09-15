import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Switch } from '@/components/ui/switch'
import { AlertTriangle, ChevronRight } from 'lucide-react'
import { ActionFeedback } from '@/components/ActionFeedback'
import { AgentMark } from '@/components/AgentMark'
import { WorkerIcon } from '@/components/WorkerIcon'
import { CommandList } from '@/components/CommandList'
import { ModelPicker } from '@/components/ModelPicker'
import { agentBinary, agentLabel, agentVersion, connection } from '@/lib/agents'
import { humanize } from '@/lib/format'
import { modelOptionsFor } from '@/lib/modelOptions'
import {
  fetchAgentSettings,
  saveAgentSettings,
  type AgentSetting,
  type AgentSettings,
  type Snapshot,
} from '@/lib/api'

const DEFAULT = '__default__'

interface Draft {
  model: string | null
  effort: string | null
  fast: boolean | null
}
type DraftEntry = { values: Draft; revision: AgentSetting['revision'] }
type DraftUpdate = Draft | ((current: Draft) => Draft) | null

const draftOf = (setting: AgentSetting): Draft => ({
  model: setting.model,
  effort: setting.effort,
  fast: setting.fast,
})

const sameDraft = (a: Draft, b: Draft) =>
  a.model === b.model && a.effort === b.effort && a.fast === b.fast

const TONES: Record<string, string> = {
  connected: 'text-observatory-green',
  attention: 'text-warning',
  unknown: 'text-muted-foreground',
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col gap-1.5">
      <span className="text-[13px] text-muted-foreground">{label}</span>
      <span className="text-lg font-medium">{value}</span>
    </div>
  )
}

function Detail({
  worker,
  setting,
  version,
  permissions,
  envProfile,
  connectionLabel,
  onSaved,
  draftEntry,
  onDraftChange,
}: {
  worker: string
  setting: AgentSetting
  version: string | null
  permissions: string | null
  envProfile: string | null
  connectionLabel: string
  onSaved: (setting: AgentSetting) => void
  draftEntry?: DraftEntry
  onDraftChange: (update: DraftUpdate) => void
}) {
  const identity = `${worker}\0${setting.agent}`
  const [activity, setActivity] = useState<{
    identity: string
    busy: boolean
    message: string | null
  }>(() => ({ identity, busy: false, message: null }))
  const saveGeneration = useRef(0)

  if (activity.identity !== identity) {
    setActivity({ identity, busy: false, message: null })
  }
  const saved = useMemo(() => draftOf(setting), [setting])
  const draft = draftEntry?.values ?? saved
  const setDraft = (update: Exclude<DraftUpdate, null>) => {
    onDraftChange(update)
    setActivity({ identity, busy: false, message: null })
  }
  useEffect(
    () => () => {
      saveGeneration.current += 1
    },
    [worker, setting.agent],
  )
  const busy = activity.identity === identity ? activity.busy : false
  const message = activity.identity === identity ? activity.message : null
  const dirty = !sameDraft(draft, saved)

  const modelOptions = useMemo(() => modelOptionsFor(setting), [setting])
  const selectedModel = modelOptions.find((option) => option.id === draft.model) ?? null
  const cursor = setting.agent === 'cursor'
  const capabilitiesKnown = !cursor || selectedModel?.capabilities_known === true
  const effortOptions = selectedModel?.effort_options ?? (cursor ? [] : setting.effort_options)
  const fastSupported = (selectedModel?.fast_supported ?? (cursor ? false : setting.fast_supported))
    || (draft.model === setting.model && setting.fast === true)

  const save = useCallback(async () => {
    const generation = ++saveGeneration.current
    setActivity({ identity, busy: true, message: null })
    try {
      const next = await saveAgentSettings(worker, {
        agent: setting.agent,
        model: draft.model,
        effort: draft.effort,
        fast: fastSupported ? draft.fast : null,
        revision: draftEntry ? draftEntry.revision : setting.revision,
      })
      if (generation !== saveGeneration.current) return
      onSaved(next)
      onDraftChange(null)
      setActivity({ identity, busy: true, message: 'Saved.' })
    } catch (error) {
      if (generation !== saveGeneration.current) return
      // Keep the draft: a rejected revision must not lose the operator's edit.
      setActivity({
        identity,
        busy: true,
        message: error instanceof Error ? error.message : String(error),
      })
    } finally {
      if (generation === saveGeneration.current) {
        setActivity((current) =>
          current.identity === identity ? { ...current, busy: false } : current,
        )
      }
    }
  }, [
    identity,
    worker,
    setting.agent,
    setting.revision,
    draftEntry,
    draft,
    fastSupported,
    onSaved,
    onDraftChange,
  ])

  return (
    <section className="mw-settings-editor">
      <h2 className="text-xl leading-7 font-semibold">
        {agentLabel(setting.agent)} on {worker}
      </h2>
      <p className="mt-1 text-[13px] text-muted-foreground">
        Native defaults for new turns. Task settings can override them.
      </p>
      {connectionLabel !== 'Connected' ? (
        <div className="mw-banner my-6">
          <AlertTriangle size={18} aria-hidden="true" />
          <div className="flex-1">
            <p className="font-medium">
              {connectionLabel === 'Sign-in needed' ? 'Sign in to ' : 'Check '}
              {agentLabel(setting.agent)} on {worker}
            </p>
            <p className="mt-1">
              {connectionLabel === 'Sign-in needed'
                ? 'Authentication is required to run tasks on this Mac.'
                : 'The latest worker facts do not confirm an authenticated agent.'}
            </p>
          </div>
          <Collapsible className="w-full">
            <CollapsibleTrigger className="mw-button" data-variant="outline">
              <ChevronRight className="mw-disclosure-chevron" size={14} aria-hidden="true" />
              Setup instructions
            </CollapsibleTrigger>
            <CollapsibleContent keepMounted>
              <div className="mt-3 space-y-3">
                <p>Open a terminal on {worker} using its configured SSH target, then run:</p>
                <CommandList
                  commands={[
                    (
                      {
                        codex: 'codex login',
                        cursor: 'cursor-agent login',
                        opencode: 'opencode auth login',
                        claude: 'claude',
                      } as Record<string, string>
                    )[setting.agent] ?? agentBinary(setting.agent),
                  ]}
                />
                <p>After signing in, refresh the facts from your laptop:</p>
                <CommandList commands={['worker workers --refresh']} />
              </div>
            </CollapsibleContent>
          </Collapsible>
        </div>
      ) : null}
      {setting.message ? <p className="mw-banner mt-6">{setting.message}</p> : null}
      <div className="mw-settings-columns">
        {setting.writable ? (
          <div className="space-y-6">
            <h3 className="font-semibold">Default settings</h3>
            <div className="space-y-1.5">
              <Label className="text-[13px] text-muted-foreground">Model</Label>
              <ModelPicker
                options={modelOptions}
                value={draft.model}
                onValueChange={(model) => {
                  const target = modelOptions.find((option) => option.id === model)
                  const options = target?.effort_options ?? []
                  setDraft((current) => ({
                    model,
                    effort:
                      current.effort && options.includes(current.effort) ? current.effort : null,
                    fast: target?.fast_supported || (model === setting.model && setting.fast === true)
                      ? current.fast : null,
                  }))
                }}
                disabled={busy || !setting.writable}
              />
              {cursor ? (
                <p className="text-xs leading-5 text-muted-foreground">
                  {setting.model_catalog_source === 'live'
                    ? `Models available to Cursor on ${worker}.`
                    : `Showing models remembered on ${worker}. The full catalogue is unavailable.`}
                </p>
              ) : null}
            </div>

            <div className="space-y-1.5">
              <Label className="text-[13px] text-muted-foreground">Reasoning effort</Label>
              <Select
                value={draft.effort ?? DEFAULT}
                onValueChange={(next) =>
                  setDraft((current) => ({
                    ...current,
                    effort: next === DEFAULT || next == null ? null : next,
                  }))
                }
                disabled={busy || !setting.writable || effortOptions.length === 0}
              >
                <SelectTrigger aria-label="Reasoning effort" className="w-full">
                  <SelectValue>
                    {(value) => (value === DEFAULT ? 'Agent default' : humanize(String(value)))}
                  </SelectValue>
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value={DEFAULT}>Agent default</SelectItem>
                  {effortOptions.map((option) => (
                    <SelectItem key={option} value={option}>
                      {humanize(option)}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {effortOptions.length === 0 ? (
                <p className="text-xs text-muted-foreground">
                  {cursor
                    ? (capabilitiesKnown ? 'This model has no reasoning control.' : 'Reasoning options are unavailable on this Mac.')
                    : 'This agent does not expose a reasoning default.'}
                </p>
              ) : null}
            </div>
            <div className="flex items-center justify-between gap-4">
              <div>
                <Label htmlFor={`fast-${worker}-${setting.agent}`}>Fast mode</Label>
                <p className="mw-help mt-1">
                  {fastSupported ? 'Use the agent’s fast setting.'
                    : capabilitiesKnown ? 'Not supported by this model.'
                      : 'Fast support hasn’t been reported by this Mac.'}
                </p>
              </div>
              <div className="flex items-center gap-2">
                <Switch
                  id={`fast-${worker}-${setting.agent}`}
                  checked={draft.fast === true}
                  onCheckedChange={(checked) =>
                    setDraft((current) => ({ ...current, fast: checked }))
                  }
                  disabled={busy || !setting.writable || !fastSupported}
                />
                <span className="text-xs text-muted-foreground">{draft.fast ? 'On' : 'Off'}</span>
              </div>
            </div>
          </div>
        ) : (
          <div>
            <h3 className="mb-4 font-semibold">Read-only defaults</h3>
            <p className="mb-5 text-[13px] text-muted-foreground">
              This Mac has not reported editable defaults for {agentLabel(setting.agent)}.
            </p>
            <dl className="mw-facts grid-cols-[150px_minmax(0,1fr)]">
              <dt>Model</dt>
              <dd>{setting.model ?? 'Not reported'}</dd>
              <dt>Reasoning effort</dt>
              <dd>{setting.effort ? humanize(setting.effort) : 'Not reported'}</dd>
              <dt>Fast mode</dt>
              <dd>{setting.fast == null ? 'Not reported' : setting.fast ? 'On' : 'Off'}</dd>
            </dl>
          </div>
        )}
        <div className="mw-settings-facts">
          <h3 className="mb-6 font-semibold">Connection and context</h3>
          <dl className="mw-facts">
            <dt>Authentication</dt>
            <dd className={connectionLabel === 'Connected' ? '!text-success' : ''}>
              {connectionLabel}
            </dd>
            <dt>Project permissions</dt>
            <dd>{humanize(permissions)}</dd>
            <dt>Project profile</dt>
            <dd>{envProfile ?? 'Default'}</dd>
            <dt>CLI version</dt>
            <dd>{version ?? 'Not reported'}</dd>
          </dl>
        </div>
      </div>
      {setting.writable ? (
        <div className="mt-7 flex flex-wrap items-center gap-3 border-t pt-6">
          <p role="status" className="mr-auto text-[13px] text-muted-foreground">
            {message ?? (dirty ? 'Unsaved changes' : 'Task settings can override these defaults.')}
          </p>
          <Button
            variant="outline"
            disabled={!dirty || busy}
            onClick={() => {
              onDraftChange(null)
              setActivity({ identity, busy: false, message: null })
            }}
          >
            Cancel
          </Button>
          <Button
            aria-busy={busy}
            disabled={!dirty || busy || !setting.writable}
            onClick={() => void save()}
          >
            <ActionFeedback
              state={
                busy ? 'pending' : message === 'Saved.' ? 'success' : message ? 'error' : 'idle'
              }
              idleLabel="Save defaults"
              pendingLabel="Saving…"
              successLabel="Saved"
              errorLabel="Try again"
            />
          </Button>
        </div>
      ) : (
        <p className="mt-7 border-t pt-6 text-[13px] text-muted-foreground">
          Editing is unavailable for these native defaults.
        </p>
      )}
    </section>
  )
}

function ProjectDefaults({ snapshot }: { snapshot: Snapshot }) {
  const defaults = snapshot.project_defaults as Record<string, unknown> | null
  if (!defaults) return null

  const text = (key: string) => {
    const value = defaults[key]
    if (value == null) return '—'
    if (Array.isArray(value)) return value.map((entry) => humanize(String(entry))).join(', ')
    return humanize(String(value))
  }
  const timeout = defaults.timeout_seconds
  return (
    <section className="mw-panel mw-panel-pad">
      <h2 className="text-[18px] leading-6 font-medium tracking-[-0.02em]">
        Project launch defaults
      </h2>
      <p className="mt-1 text-xs text-muted-foreground">From .worker.toml · read-only</p>
      <div className="mt-5 grid gap-5 sm:grid-cols-4">
        <Field
          label="Task timeout"
          value={typeof timeout === 'number' ? `${Math.round(timeout / 60)} min` : '—'}
        />
        <Field label="Max follow-ups" value={text('max_followups')} />
        <Field label="Source" value={text('source')} />
        <Field label="Publication" value={text('publish')} />
      </div>
    </section>
  )
}

export function Settings({
  snapshot,
  initialWorker,
  initialAgent,
}: {
  snapshot: Snapshot
  initialWorker?: string
  initialAgent?: string
}) {
  const workers = snapshot.workers
  const [workerName, setWorkerName] = useState<string | null>(
    initialWorker && workers.some((entry) => entry.name === initialWorker)
      ? initialWorker
      : (workers[0]?.name ?? null),
  )
  const [settings, setSettings] = useState<AgentSettings | null>(null)
  const [selectedAgent, setSelectedAgent] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [drafts, setDrafts] = useState<Record<string, DraftEntry>>({})

  const contextWorker = workers.some((entry) => entry.name === initialWorker)
    ? initialWorker
    : undefined
  useEffect(() => {
    if (contextWorker) setWorkerName(contextWorker)
  }, [contextWorker])

  const worker = workers.find((entry) => entry.name === workerName)

  useEffect(() => {
    if (workerName == null) return
    let cancelled = false
    const controller = new AbortController()
    setSettings(null)
    setError(null)
    fetchAgentSettings(workerName, controller.signal)
      .then((payload) => {
        if (cancelled) return
        setSettings(payload)
        setSelectedAgent(
          payload.agents.some((entry) => entry.agent === initialAgent)
            ? initialAgent!
            : (payload.agents[0]?.agent ?? null),
        )
      })
      .catch((cause: unknown) => {
        if (cancelled || controller.signal.aborted) return
        setError(cause instanceof Error ? cause.message : String(cause))
      })
    return () => {
      cancelled = true
      controller.abort()
    }
  }, [workerName, initialAgent])

  // An unknown connection is not the same claim as "not connected": a fact older
  // than its TTL says nothing either way, and the summary must not pretend it does.
  const tallies = (settings?.agents ?? []).reduce<Record<string, number>>((counts, setting) => {
    const tone = connection(worker, setting.agent).tone
    counts[tone] = (counts[tone] ?? 0) + 1
    return counts
  }, {})
  const summary = [
    tallies.connected ? `${tallies.connected} connected` : null,
    tallies.attention ? `${tallies.attention} need attention` : null,
    tallies.unknown ? `${tallies.unknown} unknown` : null,
  ]
    .filter(Boolean)
    .join(' · ')
  const permissions = (snapshot.project_defaults as { permissions?: Record<string, string> } | null)
    ?.permissions
  const envProfile = (snapshot.project_defaults as { env_profile?: string | null } | null)
    ?.env_profile
  const mergeSaved = useCallback(
    (saved: AgentSetting) =>
      setSettings((current) =>
        current == null
          ? current
          : {
              agents: current.agents.map((agent) => (agent.agent === saved.agent ? saved : agent)),
            },
      ),
    [],
  )

  if (workers.length === 0) {
    return <p className="text-sm text-muted-foreground">No workers are configured.</p>
  }

  const setting = settings?.agents.find((entry) => entry.agent === selectedAgent) ?? null

  return (
    <div className="mw-page">
      <header className="mw-page-heading">
        <div>
          <h1 className="mw-page-title">Settings</h1>
          <p className="mw-page-description">Agent defaults on each Mac.</p>
        </div>
        <div className="flex items-center gap-3">
          <Label htmlFor="settings-worker" className="text-xs text-muted-foreground">
            Worker
          </Label>
          <Select value={workerName ?? ''} onValueChange={(next) => setWorkerName(next ?? null)}>
            <SelectTrigger id="settings-worker" className="w-52 max-w-full">
              <SelectValue className="min-w-0">
                {(value) => (
                  <span className="flex min-w-0 items-center gap-2.5">
                    <WorkerIcon className="size-5! text-muted-foreground" />
                    <span className="truncate">{String(value)}</span>
                  </span>
                )}
              </SelectValue>
            </SelectTrigger>
            <SelectContent align="end" alignItemWithTrigger={false} className="p-1">
              {workers.map((entry) => (
                <SelectItem
                  key={entry.name}
                  value={entry.name}
                  className="min-h-10 py-2 pl-2.5 text-[13px]"
                >
                  <WorkerIcon className="size-5! text-muted-foreground" />
                  {entry.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </header>
      <div className="mw-panel mw-settings">
        <aside className="mw-settings-sidebar" aria-label="Agents">
          <h2 className="mw-section-title px-3 pb-4">Agents</h2>
          <div className="space-y-2">
            {settings?.agents.map((entry) => {
              const state = connection(worker, entry.agent)
              return (
                <button
                  type="button"
                  key={entry.agent}
                  className="mw-agent-option"
                  aria-pressed={entry.agent === selectedAgent}
                  onClick={() => setSelectedAgent(entry.agent)}
                >
                  <AgentMark agent={entry.agent} size={22} />
                  <span className="min-w-0 flex-1">
                    <span className="font-medium">{agentLabel(entry.agent)}</span>
                    <span className={'mt-1 block whitespace-nowrap text-xs ' + TONES[state.tone]}>
                      • {state.label}
                    </span>
                  </span>
                  <ChevronRight size={14} aria-hidden="true" />
                </button>
              )
            })}
          </div>
          <p className="mw-help px-3 pt-5">{settings ? summary : 'Reading native defaults…'}</p>
        </aside>
        {error ? (
          <p className="p-7 text-sm text-destructive">{error}</p>
        ) : setting && workerName ? (
          <Detail
            worker={workerName}
            setting={setting}
            version={agentVersion(worker, setting.agent)}
            permissions={permissions?.[setting.agent] ?? null}
            envProfile={envProfile ?? null}
            connectionLabel={connection(worker, setting.agent).label}
            onSaved={mergeSaved}
            draftEntry={drafts[`${workerName}\0${setting.agent}`]}
            onDraftChange={(update) => {
              const key = `${workerName}\0${setting.agent}`
              setDrafts((current) => {
                const next = { ...current }
                if (update === null) delete next[key]
                else
                  next[key] = {
                    values:
                      typeof update === 'function'
                        ? update(current[key]?.values ?? draftOf(setting))
                        : update,
                    revision: current[key] ? current[key].revision : setting.revision,
                  }
                return next
              })
            }}
          />
        ) : (
          <div className="mw-settings-editor">
            <p className="text-muted-foreground">
              {settings ? 'No agent defaults reported by this Mac.' : 'Reading native defaults…'}
            </p>
          </div>
        )}
      </div>
      <ProjectDefaults snapshot={snapshot} />
    </div>
  )
}
