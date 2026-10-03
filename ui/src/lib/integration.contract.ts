import type { ReviewState } from './api'

/** Frozen public sidecar contracts. T5 owns dashboard/API projection behavior. */
export const integrationCodes = [
  'INTEGRATION_AUTH_FAILED', 'INTEGRATION_NETWORK', 'INTEGRATION_POLICY_REJECTED',
  'INTEGRATION_TARGET_MISSING', 'INTEGRATION_BASE_NOT_ON_TARGET', 'INTEGRATION_WIP_BASE',
  'INTEGRATION_TARGET_MOVED_EXHAUSTED', 'INTEGRATION_CONFLICT_BUDGET_EXHAUSTED',
  'INTEGRATION_RESOLUTION_INCOMPLETE', 'INTEGRATION_CHECKS_FAILED', 'INTEGRATION_CHECKS_NOT_RUN',
  'INTEGRATION_RESOLVE_BLOCKED', 'INTEGRATION_FOLLOWUP_LIMIT', 'INTEGRATION_VERIFY_CHANGED_TREE',
  'INTEGRATION_VERIFY_TREE_MISMATCH', 'INTEGRATION_TURN_QUEUE_TIMEOUT', 'INTEGRATION_WORKER_OFFLINE',
  'INTEGRATION_WORKSPACE_MISSING', 'INTEGRATION_UNAVAILABLE', 'INTEGRATION_PUBLISH_TARGET_COLLISION',
  'INTEGRATION_CONFLICT_LIST_TOO_LARGE', 'INTEGRATION_STATE_INVALID', 'INTEGRATION_DEPENDENCY_BLOCKED',
  'INTEGRATION_DEPENDENCY_NOT_INTEGRATED', 'INTEGRATION_STOP_UNCONFIRMED', 'INTEGRATION_ALREADY_COMMITTED',
] as const
export type IntegrationCode = typeof integrationCodes[number]

export const integrationStatuses = [
  'armed', 'pending', 'fetching', 'resolving', 'verifying', 'commit_ready', 'pushing',
  'published', 'retry_wait', 'parked', 'integrated', 'blocked', 'revoked',
] as const
export type IntegrationStatus = typeof integrationStatuses[number]
export const workflowStates = ['queued', 'running', 'integrating', 'needs_you', 'done'] as const
export type WorkflowState = typeof workflowStates[number]
export const integrationPauseReasons = ['controller_drained', 'controller_disabled', 'helper_unavailable'] as const
export type IntegrationPauseReason = typeof integrationPauseReasons[number]
export type IntegrationDisposition = 'merged' | 'already_integrated'
export type IntegrationVerification = 'source_agent_report_only' | 'resolve_agent_report' | 'verify_agent_report'

export const MAX_TARGET_BYTES = 255
export const MAX_TARGET_DISPLAY_BYTES = 128
export const MAX_PUBLIC_SNAPSHOT_BYTES = 4096
export const MAX_FACTS_ANNOTATION_BYTES = 512
export const MAX_COMPOUND_FACTS_BYTES = 2048

export interface IntegrationSnapshot {
  schema_version: 1
  integration_id: string
  epoch: number
  revision: number
  target: string
  state: IntegrationStatus
  resume_state: IntegrationStatus | null
  pause_reason: IntegrationPauseReason | null
  source_turn_id: string
  source_head: string
  merge_oid: string | null
  observed_target_oid: string | null
  disposition: IntegrationDisposition | null
  attempts: number
  resolve_turns: number
  verify_turns: number
  blocked_code: IntegrationCode | null
  retry_exhausted: boolean
  retry_at_millis: number | null
  verification: IntegrationVerification
  updated_at_millis: number
}
export interface IntegrationFactsAnnotation {
  integration_id: string
  epoch: number
  revision: number
  state: IntegrationStatus
  code: IntegrationCode | null
  result_oid: string | null
}
export interface IntegrationView {
  integration?: IntegrationSnapshot
  workflow_state?: WorkflowState
  review_state: ReviewState
  attention: boolean
  requested_close: 'done' | 'never'
}

const encoder = new TextEncoder()
const invalid = (): never => { throw new Error('INTEGRATION_STATE_INVALID') }
const snapshotKeys = [
  'schema_version', 'integration_id', 'epoch', 'revision', 'target', 'state', 'resume_state',
  'pause_reason', 'source_turn_id', 'source_head', 'merge_oid', 'observed_target_oid', 'disposition',
  'attempts', 'resolve_turns', 'verify_turns', 'blocked_code', 'retry_exhausted', 'retry_at_millis',
  'verification', 'updated_at_millis',
]
function object(value: unknown, keys: readonly string[]): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return invalid()
  const row = value as Record<string, unknown>
  if (Object.keys(row).some(key => !keys.includes(key))) return invalid()
  return row
}
function size(value: unknown, max: number): void {
  if (encoder.encode(JSON.stringify(value)).length > max) invalid()
}
function integer(value: unknown, max = Number(0xffffffffffffffffn)): number {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 0 || value > max) return invalid()
  return value
}
function revision(value: unknown): number {
  const result = integer(value)
  return result === 0 ? invalid() : result
}
function text(value: unknown, pattern: RegExp): string {
  return typeof value === 'string' && pattern.test(value) ? value : invalid()
}
function choice<T extends string>(value: unknown, values: readonly T[]): T {
  return typeof value === 'string' && values.includes(value as T) ? value as T : invalid()
}
function nullable<T>(value: unknown, decode: (value: unknown) => T): T | null {
  return value === undefined || value === null ? null : decode(value)
}
function boolean(value: unknown): boolean {
  return typeof value === 'boolean' ? value : invalid()
}
const id = (value: unknown) => text(value, /^[0-9a-f]{8}-[0-9a-f]{4}-8[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/)
const oid = (value: unknown) => text(value, /^[0-9a-f]{40}$/)

export function decodeIntegrationSnapshot(value: unknown): IntegrationSnapshot {
  const row = object(value, snapshotKeys)
  if (row.schema_version !== 1) return invalid()
  if (typeof row.target !== 'string' || row.target.length === 0 ||
      Array.from(row.target).some(char => {
        const code = char.codePointAt(0)!
        return code < 32 || (code >= 127 && code <= 159)
      }) ||
      encoder.encode(row.target).length > MAX_TARGET_DISPLAY_BYTES) return invalid()
  const snapshot: IntegrationSnapshot = {
    schema_version: 1,
    integration_id: id(row.integration_id), epoch: integer(row.epoch, 4294967295), revision: revision(row.revision),
    target: row.target, state: choice(row.state, integrationStatuses),
    resume_state: nullable(row.resume_state, v => choice(v, integrationStatuses)),
    pause_reason: nullable(row.pause_reason, v => choice(v, integrationPauseReasons)),
    source_turn_id: text(row.source_turn_id, /^[0-9a-f]{32}$/), source_head: oid(row.source_head),
    merge_oid: nullable(row.merge_oid, oid), observed_target_oid: nullable(row.observed_target_oid, oid),
    disposition: nullable(row.disposition, v => choice(v, ['merged', 'already_integrated'] as const)),
    attempts: integer(row.attempts, 3), resolve_turns: integer(row.resolve_turns, 2), verify_turns: integer(row.verify_turns, 3),
    blocked_code: nullable(row.blocked_code, v => choice(v, integrationCodes)), retry_exhausted: boolean(row.retry_exhausted),
    retry_at_millis: nullable(row.retry_at_millis, integer),
    verification: choice(row.verification, ['source_agent_report_only', 'resolve_agent_report', 'verify_agent_report'] as const),
    updated_at_millis: integer(row.updated_at_millis),
  }
  const parked = snapshot.state === 'parked'
  const resumable = parked || snapshot.state === 'retry_wait'
  if (parked !== (snapshot.pause_reason !== null) || resumable !== (snapshot.resume_state !== null) ||
      (snapshot.resume_state !== null && ['armed', 'parked', 'retry_wait', 'integrated', 'blocked', 'revoked'].includes(snapshot.resume_state)) ||
      (snapshot.state === 'blocked' && snapshot.blocked_code === null) ||
      (snapshot.state === 'integrated' && (snapshot.disposition === null || (snapshot.merge_oid === null && snapshot.observed_target_oid === null))) ||
      (snapshot.disposition === 'already_integrated' && (snapshot.merge_oid !== null || snapshot.observed_target_oid === null)) ||
      (snapshot.disposition === 'merged' && snapshot.merge_oid === null)) return invalid()
  size(snapshot, MAX_PUBLIC_SNAPSHOT_BYTES)
  return snapshot
}

export function decodeIntegrationAnnotation(value: unknown): IntegrationFactsAnnotation {
  const row = object(value, ['integration_id', 'epoch', 'revision', 'state', 'code', 'result_oid'])
  const annotation: IntegrationFactsAnnotation = {
    integration_id: id(row.integration_id), epoch: integer(row.epoch, 4294967295), revision: revision(row.revision),
    state: choice(row.state, integrationStatuses), code: nullable(row.code, v => choice(v, integrationCodes)),
    result_oid: nullable(row.result_oid, oid),
  }
  size(annotation, MAX_FACTS_ANNOTATION_BYTES)
  return annotation
}
export function parseIntegrationAnnotation(text: string): IntegrationFactsAnnotation {
  if (encoder.encode(text).length > MAX_FACTS_ANNOTATION_BYTES) return invalid()
  try { return decodeIntegrationAnnotation(JSON.parse(text)) } catch { return invalid() }
}
export function decodeIntegrationView(value: unknown): IntegrationView {
  const row = object(value, ['integration', 'workflow_state', 'review_state', 'attention', 'requested_close'])
  const view: IntegrationView = {
    review_state: choice(row.review_state, ['not_reviewable', 'waiting_on_you', 'ready_for_review',
      'ready_for_follow_up', 'close_pending', 'accepted', 'closed_after_done', 'closed'] as const),
    attention: boolean(row.attention), requested_close: choice(row.requested_close, ['done', 'never'] as const),
  }
  if (row.integration != null) view.integration = decodeIntegrationSnapshot(row.integration)
  if (row.workflow_state != null) view.workflow_state = choice(row.workflow_state, workflowStates)
  size(view, MAX_PUBLIC_SNAPSHOT_BYTES + 512)
  return view
}
/** Companion reads must match identity, epoch, revision and all compact facts. */
export function integrationAnnotationConfirms(annotation: IntegrationFactsAnnotation, snapshot: IntegrationSnapshot): boolean {
  try {
    const fact = decodeIntegrationAnnotation(annotation)
    const full = decodeIntegrationSnapshot(snapshot)
    return fact.integration_id === full.integration_id && fact.epoch === full.epoch && fact.revision === full.revision &&
      fact.state === full.state && fact.code === full.blocked_code && fact.result_oid === (full.merge_oid ?? full.observed_target_oid)
  } catch { return false }
}
