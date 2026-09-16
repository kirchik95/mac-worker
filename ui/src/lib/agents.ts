import type { AgentAuth, AgentFact, Worker } from '@/lib/api'

/** Display names and initials come from the Settings artboard. */
const AGENTS: Record<string, { label: string; initials: string; binary: string }> = {
  codex: { label: 'Codex', initials: 'CX', binary: 'codex' },
  cursor: { label: 'Cursor', initials: 'CU', binary: 'cursor-agent' },
  opencode: { label: 'OpenCode', initials: 'OC', binary: 'opencode' },
  claude: { label: 'Claude Code', initials: 'CL', binary: 'claude' },
}

export const agentLabel = (agent: string) => AGENTS[agent]?.label ?? agent
export const agentInitials = (agent: string) =>
  AGENTS[agent]?.initials ?? agent.slice(0, 2).toUpperCase()
export const agentBinary = (agent: string) => AGENTS[agent]?.binary ?? agent

/** The shelf and native settings must describe the same Cursor account. */
export function cursorProfile(worker: Worker | undefined, projectProfile?: string | null): string | null {
  return projectProfile ?? worker?.agent_facts?.agents
    .find((entry) => entry.name === 'cursor')?.auth_by_profile
    .find((entry) => entry.profile === 'agents')?.profile ?? null
}

/** The reported value only; callers still need to check observation freshness. */
export function reportedAgentAuth(fact: AgentFact, envProfile?: string | null): AgentAuth {
  return envProfile
    ? fact.auth_by_profile.find((entry) => entry.profile === envProfile)?.auth ?? 'unknown'
    : fact.auth
}

export interface Connection {
  label: string
  tone: 'connected' | 'attention' | 'unknown'
}

/**
 * A cached fact older than its TTL is not evidence of a live connection, so a
 * stale observation reports Unknown rather than the authentication it last saw.
 */
export function connection(worker: Worker | undefined, agent: string, envProfile?: string | null): Connection {
  const facts = worker?.agent_facts
  if (!worker || !facts || worker.freshness !== 'current' || facts.freshness !== 'current') {
    return { label: 'Unknown', tone: 'unknown' }
  }
  const fact: AgentFact | undefined = facts.agents.find((entry) => entry.name === agent)
  if (!fact) return { label: 'Not configured', tone: 'attention' }
  const auth = reportedAgentAuth(fact, envProfile)
  if (auth === 'authenticated') return { label: 'Connected', tone: 'connected' }
  if (auth === 'unauthenticated') return { label: 'Sign-in needed', tone: 'attention' }
  return { label: 'Unknown', tone: 'unknown' }
}

export const agentVersion = (worker: Worker | undefined, agent: string) =>
  worker?.agent_facts?.agents.find((entry) => entry.name === agent)?.version ?? null
