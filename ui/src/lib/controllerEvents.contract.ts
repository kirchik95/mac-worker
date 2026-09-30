/** Frozen T1 wire contract. Runtime parsing belongs to the browser track.
 * Decimal sequence text is canonical u64; compare with BigInt, never Number.
 * Task/turn IDs use existing lowercase simple UUIDs; journal IDs are hyphenated.
 * Unknown well-formed kinds/schema versions invalidate all resources.
 */
import fixtures from './controllerEvents.fixtures.json'

export type EventCursor = { journal_id: string; seq: string }
export type JournalWindow = {
  journal_id: string
  oldest_seq: string
  head_seq: string
}
export type WireEvent = {
  schema_version: number
  journal_id: string
  seq: string
  time_millis: number
  kind: string
  data: Record<string, unknown>
}
export type SnapshotRequired = { reason: string; window: JournalWindow }
export type ViewerMessage =
  | { event: 'controller.event'; data: WireEvent }
  | { event: 'snapshot_required'; data: SnapshotRequired }
  | { event: 'snapshot_required'; data: { reason: 'unavailable'; code: string; window: null } }
  | { event: 'ready'; data: JournalWindow }
  | { event: 'snapshot.ready'; data: { revision: number } }
  | { event: 'heartbeat'; data: Record<string, never> }

/** Only controller.event carries an SSE id, formatted journal_id:seq.
 * snapshot.ready is a cache revision, and controls never advance the cursor.
 * Exact JSON event names and payload parity are verified by the Rust contract test.
 * These typed fixture payloads also participate in tsc -b without an asset build.
 */
export const controllerEventFixtureMessages: Record<string, ViewerMessage> = {
  bootstrap: { event: 'snapshot_required', data: fixtures.bootstrap.data },
  event_above_2pow53: { event: 'controller.event', data: fixtures.event_above_2pow53.data },
  'snapshot.ready': { event: 'snapshot.ready', data: fixtures['snapshot.ready'].data },
  heartbeat: { event: 'heartbeat', data: fixtures.heartbeat.data },
}
