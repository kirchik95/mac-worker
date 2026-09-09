/** Shapes mirror the Rust dashboard snapshot projection; see src/dashboard/model.rs. */

export type Freshness = 'current' | 'stale' | 'unknown'
export type WorkerHealth = 'ready' | 'busy' | 'unavailable' | string
export type TaskState = 'queued' | 'active' | 'open' | 'closed' | 'abandoned' | 'lost'
export type AgentAuth = 'authenticated' | 'unauthenticated' | 'unknown'

/** Matches DashboardError in src/dashboard/model.rs: `{ "code", "message" }`. */
export type DashboardError = { code: string; message: string }

export interface AgentFact {
  name: string
  version: string | null
  auth: AgentAuth
  auth_by_profile: { profile: string; auth: AgentAuth }[]
}

export interface Worker {
  name: string
  health: WorkerHealth
  freshness: Freshness
  observed_at_millis: number | null
  hostname: string | null
  agent_facts: { collected_at_millis: number; freshness: Freshness; agents: AgentFact[] } | null
  slot: { state: string; capacity: number; active_job_id: string | null }
  capabilities: string[]
  missing_capabilities: string[]
  system: {
    free_disk_bytes: number | null
    total_disk_bytes: number | null
    memory_pressure: string | null
    swap_used_bytes: number | null
    cpu_busy_percent: number | null
  }
  error: DashboardError | null
  active_task: { task_id: string; title: string; agent: string; model: string | null; effort: string | null; turn_number: number } | null
}

export interface TaskRow {
  task_id: string
  run_id: string | null
  run_position: number | null
  title: string
  agent: string
  model: string | null
  effort: string | null
  permissions: string | null
  env_profile: string | null
  state: TaskState
  blocking_code: string | null
  last_outcome: { kind: string; reason?: string } | null
  worker: string | null
  branch: string | null
  turn_count: number
  runner: string | null
  freshness: Freshness
  created_at_millis: number
  updated_at_millis: number
  active_turn_id: string | null
}

export interface RunRow {
  run_id: string
  name: string | null
  max_parallel: number
  created_at_millis: number
  progress: Progress
}

export interface Progress {
  total: number
  queued: number
  active: number
  open: number
  closed: number
  failed_like: number
}

export type QueueEntryKind = 'batch' | 'task_turn'

/** One row of the admission queue; see DashboardQueueEntry in src/dashboard/model.rs. */
export interface QueueEntry {
  position: number
  job_id: string
  entry_kind: QueueEntryKind
  task_id: string | null
  turn_id: string | null
  run_id: string | null
  run_max_parallel: number | null
  pinned_worker: string | null
  project_id: string
  worktree_id: string
  project_label: string | null
  command_summary: { mode: string; arg_count: number | null }
  created_at_millis: number
  requirements: string[]
  blocking_code: string
}

export interface Snapshot {
  api_version: number
  revision: number
  generated_at_millis: number
  collection: { freshness: Freshness; errors: DashboardError[] }
  project_defaults: Record<string, unknown> | null
  tasks: TaskRow[]
  runs: RunRow[]
  progress: Progress
  workers: Worker[]
  queue: QueueEntry[]
  active_jobs: unknown[]
  recent_jobs: unknown[]
}

export class ApiError extends Error {
  readonly status: number

  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

export async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(path, { cache: 'no-store', signal })
  if (!response.ok) throw new ApiError(response.status, `${path} responded ${response.status}`)
  return (await response.json()) as T
}

export const fetchSnapshot = (signal?: AbortSignal) => getJson<Snapshot>('/api/v1/snapshot', signal)

export interface TurnRow {
  turn_number: number
  turn_id: string
  terminal: string | null
  outcome: { kind: string; reason?: string } | null
  agent_committed: boolean | null
  log_truncated: boolean
  started_at_millis: number | null
  ended_at_millis: number | null
}

export interface Question {
  text: string
  options: string[]
}

export interface TaskDetail {
  task: TaskRow
  project_id: string
  worktree_id: string
  base_oid: string | null
  head_oid: string | null
  session_present: boolean
  summary: string | null
  /** A question with no options is serialized as a bare string by the host. */
  questions: (string | Question)[]
  files_changed: string[]
  diff_stat: string | null
  fetch_command: string
  turns: TurnRow[]
  timeline: TurnRow[]
}

export interface ModelOption {
  id: string
  label: string
  effort_options: string[]
  fast_supported: boolean
}

export interface AgentSetting {
  agent: string
  model: string | null
  effort: string | null
  fast: boolean | null
  fast_supported: boolean
  effort_options: string[]
  model_options: ModelOption[]
  source: string | null
  revision: string
  writable: boolean
  message: string | null
}

export interface AgentSettings {
  agents: AgentSetting[]
}

export interface SaveSettings {
  agent: string
  model: string | null
  effort: string | null
  fast: boolean | null
  revision: string
}

export const fetchTaskDetail = (taskId: string, signal?: AbortSignal) =>
  getJson<TaskDetail>(`/api/v1/tasks/${encodeURIComponent(taskId)}`, signal)

export const fetchAgentSettings = (worker: string, signal?: AbortSignal) =>
  getJson<AgentSettings>(`/api/v1/workers/${encodeURIComponent(worker)}/agent-settings`, signal)

/** Saves one agent. The revision is the optimistic check the host enforces. */
export async function saveAgentSettings(worker: string, body: SaveSettings): Promise<AgentSetting> {
  const response = await fetch(`/api/v1/workers/${encodeURIComponent(worker)}/agent-settings`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-mac-worker-settings': '1',
    },
    body: JSON.stringify(body),
  })
  const payload: unknown = await response.json().catch(() => null)
  if (!response.ok) {
    const message =
      payload && typeof payload === 'object' && 'error' in payload
        ? JSON.stringify((payload as { error: unknown }).error)
        : `save responded ${response.status}`
    throw new ApiError(response.status, message)
  }
  return payload as AgentSetting
}

export const questionText = (question: string | Question) =>
  typeof question === 'string' ? question : question.text

export const questionOptions = (question: string | Question) =>
  typeof question === 'string' ? [] : question.options

/**
 * The host serializes DashboardError as `{ code, message }`. A legacy string is
 * still shown as the message so a worker error cannot crash the page (React #31).
 */
export function describeError(error: unknown): { code: string | null; message: string } | null {
  if (error == null) return null
  if (typeof error === 'string') return error ? { code: null, message: error } : null
  if (typeof error !== 'object') return null
  const record = error as { code?: unknown; message?: unknown }
  const code = typeof record.code === 'string' && record.code ? record.code : null
  const message = typeof record.message === 'string' && record.message ? record.message : null
  if (message) return { code, message }
  if (code) return { code, message: code }
  return null
}

export type LogStream = 'stdout' | 'stderr'

export interface LogChunk {
  stream: LogStream
  offset: number
  next_offset: number
  /** Base64; the host never sends raw bytes through JSON. */
  data: string
}

export const MAX_LOG_CHUNK_BYTES = 65_536

export const fetchTurnLog = (
  taskId: string,
  turnId: string,
  stream: LogStream,
  offset: number,
  signal?: AbortSignal,
) =>
  getJson<LogChunk>(
    `/api/v1/tasks/${encodeURIComponent(taskId)}/turns/${encodeURIComponent(turnId)}/logs` +
      `?stream=${stream}&offset=${offset}&limit=${MAX_LOG_CHUNK_BYTES}`,
    signal,
  )

export function decodeBase64(data: string): Uint8Array {
  const binary = atob(data)
  const bytes = new Uint8Array(binary.length)
  for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index)
  return bytes
}
