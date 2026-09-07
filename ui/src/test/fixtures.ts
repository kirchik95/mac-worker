import type { AgentSettings, QueueEntry, Snapshot, TaskRow, Worker } from '@/lib/api'

export const MALICIOUS = '<img src=x onerror=1>'

export function worker(overrides: Partial<Worker> = {}): Worker {
  return {
    name: 'mini-1',
    health: 'ready',
    freshness: 'current',
    observed_at_millis: 1_000,
    hostname: 'mini-1.local',
    agent_facts: {
      collected_at_millis: 1_000,
      freshness: 'current',
      agents: [
        { name: 'codex', version: '0.153.2', auth: 'authenticated', auth_by_profile: [] },
        { name: 'claude', version: '2.1.252', auth: 'unknown', auth_by_profile: [] },
      ],
    },
    slot: { state: 'idle', capacity: 1, active_job_id: null },
    capabilities: ['darwin-arm64'],
    missing_capabilities: [],
    system: {
      free_disk_bytes: 150 * 1024 ** 3,
      total_disk_bytes: 240 * 1024 ** 3,
      memory_pressure: 'warn',
      swap_used_bytes: 0,
      cpu_busy_percent: null,
    },
    error: null,
    active_task: null,
    ...overrides,
  }
}

export function task(overrides: Partial<TaskRow> = {}): TaskRow {
  return {
    task_id: 'aaaaaaaabbbbbbbbccccccccdddddddd',
    run_id: null,
    run_position: null,
    title: 'Repair login',
    agent: 'codex',
    model: null,
    effort: null,
    permissions: 'workspace',
    env_profile: null,
    state: 'active',
    blocking_code: null,
    last_outcome: null,
    worker: 'mini-1',
    branch: 'task/aaaa',
    turn_count: 1,
    runner: 'live',
    freshness: 'current',
    created_at_millis: 1_000,
    updated_at_millis: 2_000,
    active_turn_id: null,
    ...overrides,
  }
}

export function queueEntry(overrides: Partial<QueueEntry> = {}): QueueEntry {
  return {
    position: 1,
    job_id: 'job-1',
    entry_kind: 'batch',
    task_id: 'aaaaaaaabbbbbbbbccccccccdddddddd',
    turn_id: null,
    run_id: null,
    run_max_parallel: null,
    pinned_worker: null,
    project_id: 'project',
    worktree_id: 'worktree',
    project_label: 'mac-worker',
    command_summary: { mode: 'agent', arg_count: 3 },
    created_at_millis: 1_000,
    requirements: ['agent:codex'],
    blocking_code: 'WAITING_FOR_DISPATCH',
    ...overrides,
  }
}

export function snapshot(overrides: Partial<Snapshot> = {}): Snapshot {
  return {
    api_version: 1,
    revision: 7,
    generated_at_millis: 3_000,
    collection: { freshness: 'current', errors: [] },
    project_defaults: null,
    tasks: [task()],
    runs: [],
    progress: { total: 1, queued: 0, active: 1, open: 0, closed: 0, failed_like: 0 },
    workers: [worker()],
    queue: [],
    active_jobs: [],
    recent_jobs: [],
    ...overrides,
  }
}

export function agentSettings(): AgentSettings {
  return {
    agents: [
      {
        agent: 'codex',
        model: 'gpt-6-astra',
        effort: 'xhigh',
        fast: false,
        fast_supported: true,
        effort_options: ['low', 'high', 'xhigh'],
        model_options: [
          {
            id: 'gpt-6-astra',
            label: 'GPT-6-Astra',
            effort_options: ['low', 'high', 'xhigh'],
            fast_supported: true,
          },
          { id: 'tiny', label: 'Tiny', effort_options: ['low'], fast_supported: false },
        ],
        source: 'native-codex',
        revision: 'rev-1',
        writable: true,
        message: null,
      },
      {
        agent: 'opencode',
        model: null,
        effort: null,
        fast: null,
        fast_supported: false,
        effort_options: [],
        model_options: [],
        source: 'native-opencode',
        revision: 'rev-2',
        writable: true,
        message: null,
      },
    ],
  }
}
