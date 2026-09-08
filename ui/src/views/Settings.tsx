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
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import {
  agentBinary,
  agentInitials,
  agentLabel,
  agentVersion,
  connection,
} from '@/lib/agents'
import { humanize, pad2 } from '@/lib/format'
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

const draftOf = (setting: AgentSetting): Draft => ({
  model: setting.model,
  effort: setting.effort,
  fast: setting.fast,
})

const sameDraft = (a: Draft, b: Draft) =>
  a.model === b.model && a.effort === b.effort && a.fast === b.fast

const TONES: Record<string, string> = {
  connected: 'text-observatory-green',
  attention: 'text-primary',
  unknown: 'text-muted-foreground',
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col gap-1.5">
      <span className="font-mono text-[11px] tracking-[0.06em] text-muted-foreground">{label}</span>
      <span className="text-sm">{value}</span>
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
}: {
  worker: string
  setting: AgentSetting
  version: string | null
  permissions: string | null
  envProfile: string | null
  connectionLabel: string
  onSaved: (setting: AgentSetting) => void
}) {
  const identity = `${worker}\0${setting.agent}`
  const [draft, setDraft] = useState<Draft>(() => draftOf(setting))
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
  useEffect(() => setDraft(draftOf(setting)), [setting.agent, setting.revision, setting])
  useEffect(
    () => () => {
      saveGeneration.current += 1
    },
    [worker, setting.agent],
  )
  const busy = activity.identity === identity ? activity.busy : false
  const message = activity.identity === identity ? activity.message : null
  const dirty = !sameDraft(draft, saved)

  const selectedModel = setting.model_options.find((option) => option.id === draft.model) ?? null
  const effortOptions = selectedModel?.effort_options ?? setting.effort_options
  const fastSupported = selectedModel?.fast_supported ?? setting.fast_supported

  const save = useCallback(async () => {
    const generation = ++saveGeneration.current
    setActivity({ identity, busy: true, message: null })
    try {
      const next = await saveAgentSettings(worker, {
        agent: setting.agent,
        model: draft.model,
        effort: draft.effort,
        fast: fastSupported ? draft.fast : null,
        revision: setting.revision,
      })
      if (generation !== saveGeneration.current) return
      onSaved(next)
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
  }, [identity, worker, setting.agent, setting.revision, draft, fastSupported, onSaved])

  return (
    <section className="rounded-[10px] border bg-card p-5">
      <h2 className="text-[18px] leading-6 font-medium tracking-[-0.02em]">
        {worker} / {agentBinary(setting.agent)}
      </h2>

      <div className="mt-5 grid gap-5 sm:grid-cols-2">
        <div className="space-y-1.5">
          <Label className="font-mono text-[11px] tracking-[0.06em] text-muted-foreground">
            MODEL
          </Label>
          <Select
            value={draft.model ?? DEFAULT}
            onValueChange={(next) => {
              const model = next === DEFAULT || next == null ? null : next
              const options =
                setting.model_options.find((option) => option.id === model)?.effort_options ?? []
              setDraft((current) => ({
                model,
                effort: current.effort && options.includes(current.effort) ? current.effort : null,
                fast: current.fast,
              }))
            }}
            disabled={!setting.writable}
          >
            <SelectTrigger className="w-full">
              <SelectValue>
                {(value) =>
                  value === DEFAULT
                    ? 'Agent default'
                    : (setting.model_options.find((option) => option.id === value)?.label ??
                      String(value))
                }
              </SelectValue>
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={DEFAULT}>Agent default</SelectItem>
              {setting.model_options.map((option) => (
                <SelectItem key={option.id} value={option.id}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div className="space-y-1.5">
          <Label className="font-mono text-[11px] tracking-[0.06em] text-muted-foreground">
            EFFORT
          </Label>
          <Select
            value={draft.effort ?? DEFAULT}
            onValueChange={(next) =>
              setDraft((current) => ({
                ...current,
                effort: next === DEFAULT || next == null ? null : next,
              }))
            }
            disabled={!setting.writable || effortOptions.length === 0}
          >
            <SelectTrigger className="w-full">
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
              {setting.agent} publishes no global effort setting.
            </p>
          ) : null}
        </div>
      </div>

      <div className="mt-5 grid gap-5 border-t pt-5 sm:grid-cols-3">
        <Field label="PERMISSIONS" value={humanize(permissions)} />
        <Field label="ENVIRONMENT PROFILE" value={envProfile ?? '—'} />
        <Field label="AUTHENTICATION" value={connectionLabel} />
        <Field label="CLI VERSION" value={version ?? '—'} />
        <div className="flex items-center gap-3">
          <Switch
            id={`fast-${worker}-${setting.agent}`}
            checked={draft.fast === true}
            onCheckedChange={(checked) => setDraft((current) => ({ ...current, fast: checked }))}
            disabled={!setting.writable || !fastSupported}
          />
          <Label htmlFor={`fast-${worker}-${setting.agent}`} className="text-sm">
            Fast
            {!fastSupported ? (
              <span className="ml-1 text-xs text-muted-foreground">not supported</span>
            ) : null}
          </Label>
        </div>
      </div>

      <div className="mt-5 flex items-center gap-3 border-t pt-5">
        <p className="flex-1 text-xs text-muted-foreground">
          {message ?? 'Task settings can override these defaults.'}
        </p>
        <Button
          variant="ghost"
          size="sm"
          disabled={!dirty || busy}
          onClick={() => {
            setDraft(saved)
            setActivity({ identity, busy: false, message: null })
          }}
        >
          Cancel
        </Button>
        <Button size="sm" disabled={!dirty || busy || !setting.writable} onClick={() => void save()}>
          {busy ? 'Saving…' : 'Save'}
        </Button>
      </div>
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
    <section className="rounded-[10px] border bg-card p-5">
      <h2 className="text-[18px] leading-6 font-medium tracking-[-0.02em]">
        Project launch defaults
      </h2>
      <p className="mt-1 text-xs text-muted-foreground">mac-worker · .worker.toml</p>
      <div className="mt-5 grid gap-5 sm:grid-cols-4">
        <Field
          label="TASK TIMEOUT"
          value={typeof timeout === 'number' ? `${Math.round(timeout / 60)} min` : '—'}
        />
        <Field label="MAX FOLLOW-UPS" value={text('max_followups')} />
        <Field label="SOURCE" value={text('source')} />
        <Field label="PUBLICATION" value={text('publish')} />
      </div>
    </section>
  )
}

export function Settings({ snapshot }: { snapshot: Snapshot }) {
  const workers = snapshot.workers
  const [workerName, setWorkerName] = useState<string | null>(workers[0]?.name ?? null)
  const [settings, setSettings] = useState<AgentSettings | null>(null)
  const [selectedAgent, setSelectedAgent] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

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
        setSelectedAgent(payload.agents[0]?.agent ?? null)
      })
      .catch((cause: unknown) => {
        if (cancelled || controller.signal.aborted) return
        setError(cause instanceof Error ? cause.message : String(cause))
      })
    return () => {
      cancelled = true
      controller.abort()
    }
  }, [workerName])

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
              agents: current.agents.map((agent) =>
                agent.agent === saved.agent ? saved : agent,
              ),
            },
      ),
    [],
  )

  if (workers.length === 0) {
    return <p className="text-sm text-muted-foreground">No workers are configured.</p>
  }

  const setting = settings?.agents.find((entry) => entry.agent === selectedAgent) ?? null

  return (
    <div className="space-y-5">
      <section className="rounded-[10px] border bg-card p-5">
        <div className="flex flex-wrap items-center gap-3">
          <h2 className="text-[18px] leading-6 font-medium tracking-[-0.02em]">Agents</h2>
          <span className="rounded border px-1.5 font-mono text-xs text-muted-foreground">
            {pad2(settings?.agents.length ?? 0)}
          </span>
          <p className="text-sm text-muted-foreground">
            {settings ? summary : 'Connections and latest launch settings'}
          </p>
          <div className="ml-auto flex items-center gap-2">
            <Label htmlFor="settings-worker" className="text-sm text-muted-foreground">
              Worker
            </Label>
            <Select value={workerName ?? ''} onValueChange={(next) => setWorkerName(next ?? null)}>
              <SelectTrigger id="settings-worker" className="w-[180px]">
                <SelectValue>{(value) => String(value)}</SelectValue>
              </SelectTrigger>
              <SelectContent>
                {workers.map((entry) => (
                  <SelectItem key={entry.name} value={entry.name}>
                    {entry.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>

        {error ? <p className="mt-4 text-sm text-destructive">{error}</p> : null}
        {settings == null && error == null ? (
          <p className="mt-4 text-sm text-muted-foreground">Reading native defaults…</p>
        ) : null}

        {settings ? (
          <Table className="mt-4">
            <TableHeader>
              <TableRow>
                <TableHead className="font-mono text-[11px] tracking-[0.06em]">AGENT</TableHead>
                <TableHead className="font-mono text-[11px] tracking-[0.06em]">CONNECTION</TableHead>
                <TableHead className="font-mono text-[11px] tracking-[0.06em]">MODEL</TableHead>
                <TableHead className="font-mono text-[11px] tracking-[0.06em]">EFFORT</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {settings.agents.map((entry) => {
                const state = connection(worker, entry.agent)
                return (
                  <TableRow
                    key={entry.agent}
                    onClick={() => setSelectedAgent(entry.agent)}
                    className="cursor-pointer"
                    data-state={entry.agent === selectedAgent ? 'selected' : undefined}
                  >
                    <TableCell>
                      <div className="flex items-center gap-3">
                        <span className="flex size-7 items-center justify-center rounded border font-mono text-[10px] text-muted-foreground">
                          {agentInitials(entry.agent)}
                        </span>
                        <span>
                          <span className="block text-sm">{agentLabel(entry.agent)}</span>
                          <span className="block text-xs text-muted-foreground">
                            {agentBinary(entry.agent)}
                          </span>
                        </span>
                      </div>
                    </TableCell>
                    <TableCell className={TONES[state.tone]}>{state.label}</TableCell>
                    <TableCell>{entry.model ?? 'Agent default'}</TableCell>
                    <TableCell>{entry.effort ? humanize(entry.effort) : 'Not reported'}</TableCell>
                  </TableRow>
                )
              })}
            </TableBody>
          </Table>
        ) : null}

        <p className="mt-4 text-xs text-muted-foreground">
          Model and effort are the agent's own defaults on this worker. A task can override them for
          one turn.
        </p>
      </section>

      {setting && workerName ? (
        <Detail
          worker={workerName}
          setting={setting}
          version={agentVersion(worker, setting.agent)}
          permissions={permissions?.[setting.agent] ?? null}
          envProfile={envProfile ?? null}
          connectionLabel={connection(worker, setting.agent).label}
          onSaved={mergeSaved}
        />
      ) : null}

      <ProjectDefaults snapshot={snapshot} />
    </div>
  )
}
