import type { AgentSettings, Progress, Snapshot, TaskDetail, TaskRow, Worker } from '@/lib/api'

const NOW = Date.now()
const id = (index: number) =>
  (Math.imul(index, 0x9e3779b1) >>> 0).toString(16).padStart(8, '0') +
  index.toString(16).padStart(24, '0')
const progress = (tasks: TaskRow[]): Progress => ({
  total: tasks.length,
  queued: tasks.filter((t) => t.state === 'queued').length,
  active: tasks.filter((t) => t.state === 'active').length,
  open: tasks.filter((t) => t.state === 'open').length,
  closed: tasks.filter((t) => t.state === 'closed').length,
  failed_like: tasks.filter((t) => t.state === 'lost' || t.state === 'abandoned').length,
})
export const wantsExample = () =>
  typeof window !== 'undefined' && new URLSearchParams(window.location.search).has('example')

/** Deterministic preview data. Every preview endpoint stays local, including mutations. */
export function exampleSnapshot(now = NOW): Snapshot {
  const titles = [
    'Choose the follow-up label',
    'Name the second test helper',
    'Cover the outcome filter parser',
    'Wire the queue projection into the UI',
    'Verify agent installation',
    'Finish task outcome summary',
    'Cursor smoke test',
    'Check the queue contract',
  ]
  const tasks = titles.map(
    (title, index): TaskRow => ({
      task_id: id(index + 1),
      title,
      run_id: index < 6 ? id(101) : id(index === 6 ? 102 : 103),
      run_position: index + 1,
      agent: index === 4 ? 'opencode' : index === 6 ? 'cursor' : 'codex',
      model: null,
      effort: null,
      permissions: 'workspace',
      env_profile: null,
      state:
        index < 3
          ? 'open'
          : index === 3 || index === 6
            ? 'active'
            : index === 7
              ? 'closed'
              : 'queued',
      blocking_code:
        index === 4 ? 'NO_WORKER_OFFERS:agent:opencode' : index === 5 ? 'RUN_MAX_PARALLEL' : null,
      last_outcome:
        index < 2 ? { kind: 'needs_input' } : index === 2 || index === 7 ? { kind: 'done' } : null,
      worker: index === 4 ? 'mini-1' : index === 5 ? null : index === 6 ? 'mini-2' : 'mini-1',
      branch: index === 2 ? 'task/' + id(3) : null,
      turn_count: index === 3 ? 2 : index === 4 || index === 5 ? 0 : 1,
      runner: 'live',
      freshness: 'current',
      created_at_millis: now - 172800000,
      updated_at_millis:
        now - (index < 2 ? 172800000 : index === 2 ? 120000 : index === 7 ? 7200000 : 252000),
      active_turn_id: index === 3 || index === 6 ? id(201 + index) : null,
      close_policy: 'never',
      review_state:
        index < 2
          ? 'waiting_on_you'
          : index === 2
            ? 'ready_for_review'
            : index === 7
              ? 'accepted'
              : 'not_reviewable',
    }),
  )
  const workers = ['mini-1', 'mini-2', 'mini-3'].map((name, index): Worker => {
    const active = tasks[index === 0 ? 3 : 6]
    return {
      name,
      health: index < 2 ? 'busy' : 'ready',
      freshness: 'current',
      observed_at_millis: now - 2000,
      hostname: name + '.local',
      agent_facts: {
        collected_at_millis: now - 2000,
        freshness: 'current',
        agents: ['codex', 'cursor', 'opencode'].map((agent) => ({
          name: agent,
          version: null,
          auth: agent === 'opencode' ? 'unauthenticated' : 'authenticated',
          auth_by_profile: [],
        })),
      },
      herdr: { state: 'available', version: '0.9.0', interactive_agents: index < 2 ? 1 : 0 },
      slot: {
        state: index < 2 ? 'busy' : 'idle',
        capacity: 2,
        busy: index < 2 ? 1 : 0,
        active_job_id: index < 2 ? active.active_turn_id : null,
        active_job_ids: index < 2 ? [active.active_turn_id!] : [],
      },
      capabilities: ['darwin-arm64', 'agent:codex', 'agent:cursor'],
      missing_capabilities: [],
      system: {
        free_disk_bytes: 160 * 1024 ** 3,
        total_disk_bytes: 245 * 1024 ** 3,
        memory_pressure: 'normal',
        swap_used_bytes: 0,
        cpu_busy_percent: index < 2 ? 38.4 : 3.2,
      },
      error: null,
      active_task:
        index < 2
          ? {
              task_id: active.task_id,
              title: active.title,
              agent: active.agent,
              model: active.model,
              effort: active.effort,
              turn_number: active.turn_count,
            }
          : null,
    }
  })
  const empty =
    typeof window !== 'undefined' &&
    new URLSearchParams(window.location.search).get('example') === 'empty'
  const dataTasks = empty ? [] : tasks
  if (empty)
    workers.forEach((worker) => {
      worker.health = 'ready'
      worker.active_task = null
      worker.slot = { state: 'idle', capacity: 2, busy: 0, active_job_id: null, active_job_ids: [] }
    })
  return {
    api_version: 1,
    revision: 42,
    generated_at_millis: now - 2000,
    collection: { freshness: 'current', errors: [] },
    project_defaults: {
      default_agent: 'codex',
      timeout_seconds: 2700,
      max_followups: 10,
      source: 'local',
      publish: ['fetch'],
      env_profile: null,
      permissions: { codex: 'workspace', cursor: 'unattended', opencode: 'unattended' },
    },
    tasks: dataTasks,
    workers,
    progress: progress(dataTasks),
    runs: empty
      ? []
      : ['Outcome filtering', 'Phase 5d acceptance', 'Queue contract'].map((name, index) => ({
          run_id: id(101 + index),
          name,
          max_parallel: index === 2 ? 1 : 2,
          created_at_millis: now - (index + 1) * 3600000,
          progress: progress(tasks.filter((task) => task.run_id === id(101 + index))),
        })),
    queue: [],
    active_jobs: [],
    recent_jobs: [],
  }
}

export function exampleDetail(taskId: string): TaskDetail {
  const task = exampleSnapshot().tasks.find((task) => task.task_id === taskId)
  if (!task) throw new Error('This task is not part of the example.')
  const waiting = task.review_state === 'waiting_on_you'
  const running = task.state === 'active'
  const turns = Array.from({ length: task.turn_count }, (_, index) => ({
    turn_number: index + 1,
    turn_id:
      running && index === task.turn_count - 1
        ? task.active_turn_id!
        : id(
            300 +
              (exampleSnapshot().tasks.findIndex((row) => row.task_id === taskId) + 1) * 10 +
              index,
          ),
    terminal: running && index === task.turn_count - 1 ? null : 'succeeded',
    outcome:
      running && index === task.turn_count - 1
        ? null
        : { kind: waiting || index < task.turn_count - 1 ? 'needs_input' : 'done' },
    agent_committed: !waiting,
    log_truncated: false,
    started_at_millis: task.updated_at_millis - 138000,
    ended_at_millis: running && index === task.turn_count - 1 ? null : task.updated_at_millis,
  }))
  return {
    task,
    project_id: 'mac-worker',
    worktree_id: 'main',
    base_oid: 'a6824fe'.padEnd(40, '0'),
    head_oid: task.review_state === 'ready_for_review' ? 'f4db1c2'.padEnd(40, '0') : null,
    session_present: true,
    summary: waiting
      ? 'The follow-up needs a naming decision before I can finish the change. Either option works; I’ll use your choice consistently in the CLI and task view.'
      : 'Added focused tests for outcome filtering, including empty input, aliases and invalid values.',
    questions: waiting
      ? [
          {
            text:
              task.task_id === id(1)
                ? 'Should the follow-up label use alpha or beta?'
                : 'Should the second test helper use gamma or delta?',
            options: task.task_id === id(1) ? ['alpha', 'beta'] : ['gamma', 'delta'],
          },
        ]
      : [],
    files_changed:
      waiting || running ? [] : ['src/task_outcome.rs', 'tests/task_outcome.rs', 'docs/usage.md'],
    diff_stat: waiting || running ? null : '3 files changed, +86 −12',
    fetch_command: 'worker task fetch ' + task.task_id,
    review_state: task.review_state,
    close_policy: task.close_policy,
    reported_checks:
      waiting || running
        ? []
        : [
            {
              name: 'Outcome parser tests',
              command: 'cargo test task_outcome',
              status: 'pass',
              detail: 'All targeted checks passed.',
              source: 'agent_reported',
            },
            {
              name: 'Formatting',
              command: 'cargo fmt --check',
              status: 'pass',
              detail: '',
              source: 'agent_reported',
            },
          ],
    fetched_head: null,
    fetched_ref: null,
    review_commands: [
      'worker task fetch ' + task.task_id,
      'worker task diff ' + task.task_id + ' --stat',
    ],
    turns,
    timeline: turns,
  }
}
export const EXAMPLE_LOG = [
  { time: '09:40:00', kind: 'agent', message: 'I’m wiring the queue projection into the UI.' },
  { time: '09:40:19', kind: 'read', message: 'ui/src/views/Overview.tsx' },
  { time: '09:41:35', kind: 'edit', message: 'ui/src/lib/queue.ts' },
  { time: '09:42:44', kind: 'run', message: 'npm test' },
  { time: '09:43:18', kind: 'check', message: 'Checking queue states and dispatch reasons.' },
  { time: '09:44:12', kind: 'agent', message: 'Running the remaining checks.' },
]
export function exampleSettings(): AgentSettings {
  return {
    agents: ['codex', 'cursor', 'opencode', 'claude'].map((agent) => ({
      agent,
      model: null,
      effort: agent === 'codex' ? 'high' : null,
      fast: agent === 'codex' ? false : null,
      fast_supported: agent === 'codex',
      effort_options: agent === 'codex' ? ['low', 'medium', 'high'] : [],
      model_options: [],
      source: 'native',
      revision: 'example',
      writable: agent === 'codex' || agent === 'cursor',
      message: null,
    })),
  }
}
export function exampleResponse(path: string): unknown {
  const url = new URL(path, 'http://example.local')
  if (url.pathname === '/api/v1/snapshot') return exampleSnapshot()
  if (url.pathname.endsWith('/agent-settings')) return exampleSettings()
  const match = url.pathname.match(/^\/api\/v1\/tasks\/([a-f0-9]{32})/)
  if (match && url.pathname.endsWith('/logs')) {
    const stream = url.searchParams.get('stream') ?? 'stdout'
    const offset = Number(url.searchParams.get('offset') ?? 0)
    const text =
      stream === 'stdout' ? EXAMPLE_LOG.map((event) => JSON.stringify(event)).join('\n') + '\n' : ''
    const bytes = new TextEncoder().encode(text)
    const data = bytes.subarray(Math.min(offset, bytes.length))
    return {
      stream,
      offset,
      next_offset: Math.max(offset, bytes.length),
      data: btoa(String.fromCharCode(...data)),
    }
  }
  if (match) return exampleDetail(match[1])
  throw new Error('No example data for this request.')
}
