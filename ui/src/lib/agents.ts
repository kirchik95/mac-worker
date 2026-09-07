import type { AgentFact, Worker } from '@/lib/api'

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

export interface Connection {
  label: string
  tone: 'connected' | 'attention' | 'unknown'
}

/**
 * A cached fact older than its TTL is not evidence of a live connection, so a
 * stale observation reports Unknown rather than the authentication it last saw.
 */
export function connection(worker: Worker | undefined, agent: string): Connection {
  const facts = worker?.agent_facts
  if (!worker || !facts || worker.freshness !== 'current' || facts.freshness !== 'current') {
    return { label: 'Unknown', tone: 'unknown' }
  }
  const fact: AgentFact | undefined = facts.agents.find((entry) => entry.name === agent)
  if (!fact) return { label: 'Not configured', tone: 'attention' }
  if (fact.auth === 'authenticated') return { label: 'Connected', tone: 'connected' }
  if (fact.auth === 'unauthenticated') return { label: 'Sign-in needed', tone: 'attention' }
  return { label: 'Unknown', tone: 'unknown' }
}

export const agentVersion = (worker: Worker | undefined, agent: string) =>
  worker?.agent_facts?.agents.find((entry) => entry.name === agent)?.version ?? null
