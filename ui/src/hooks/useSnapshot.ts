import { useEffect, useRef, useState } from 'react'

import { useControllerEvents } from '@/hooks/ControllerEventsContext'
import { fetchSnapshot, SnapshotPendingError, type Snapshot } from '@/lib/api'
import { FALLBACK_POLL_MS, HEALTHY_ANTI_ENTROPY_MS } from '@/lib/controllerEvents'
import { exampleSnapshot, wantsExample } from '@/lib/exampleSnapshot'

export interface SnapshotState {
  snapshot: Snapshot | null
  error: string | null
  /** True once a poll has failed since the last success, so the UI can say so without blanking. */
  offline: boolean
  /** True when the page is showing the design's example snapshot, not the pool. */
  example: boolean
}

/** `?example` renders the artboard's snapshot so every state can be inspected. */
export { wantsExample } from '@/lib/exampleSnapshot'

/**
 * Polls the dashboard snapshot. A healthy event stream stretches the interval to
 * anti-entropy; stream failure, repair, and a missed watchdog return to 2s immediately.
 * Events only invalidate this cache — they never replace the snapshot body.
 */
export function useSnapshot(): SnapshotState {
  const example = wantsExample()
  const events = useControllerEvents()
  const [state, setState] = useState<SnapshotState>(() =>
    example
      ? { snapshot: exampleSnapshot(), error: null, offline: false, example: true }
      : { snapshot: null, error: null, offline: false, example: false },
  )
  const latest = useRef<Snapshot | null>(null)
  const kickRef = useRef<(count: number) => void>(() => {})
  const rescheduleRef = useRef<() => void>(() => {})
  const healthyRef = useRef(events.healthy)
  const offlineRef = useRef(false)

  useEffect(() => {
    if (example) return
    let cancelled = false
    let timer: ReturnType<typeof setTimeout> | null = null
    offlineRef.current = false
    let active = false
    let queued = 0
    const controller = new AbortController()

    const delay = () =>
      healthyRef.current && !offlineRef.current ? HEALTHY_ANTI_ENTROPY_MS : FALLBACK_POLL_MS

    const schedule = () => {
      if (cancelled || timer !== null || active) return
      timer = setTimeout(() => {
        timer = null
        enqueue(1)
      }, delay())
    }

    const enqueue = (count: number) => {
      if (cancelled || count <= 0) return
      if (timer !== null) {
        clearTimeout(timer)
        timer = null
      }
      queued += count
      pump()
    }

    const pump = () => {
      if (cancelled || active || queued === 0) return
      queued -= 1
      void poll()
    }

    const poll = async () => {
      if (cancelled || active) return
      active = true
      try {
        const snapshot = await fetchSnapshot(controller.signal)
        if (cancelled) return
        latest.current = snapshot
        offlineRef.current = false
        setState({ snapshot, error: null, offline: false, example: false })
      } catch (error) {
        if (cancelled || controller.signal.aborted) return
        if (error instanceof SnapshotPendingError) {
          offlineRef.current = false
          setState((current) => ({
            snapshot: current.snapshot,
            error: null,
            offline: false,
            example: false,
          }))
        } else {
          offlineRef.current = true
          setState({
            snapshot: latest.current,
            error: error instanceof Error ? error.message : String(error),
            offline: true,
            example: false,
          })
        }
      } finally {
        active = false
      }
      if (cancelled) return
      if (queued > 0) pump()
      else schedule()
    }

    kickRef.current = enqueue
    rescheduleRef.current = () => {
      if (timer === null || active) return
      clearTimeout(timer)
      timer = null
      schedule()
    }
    enqueue(1)
    return () => {
      cancelled = true
      kickRef.current = () => {}
      rescheduleRef.current = () => {}
      controller.abort()
      if (timer !== null) clearTimeout(timer)
    }
  }, [example])

  const epoch = events.snapshotEpoch
  const seenEpoch = useRef(epoch)
  useEffect(() => {
    if (example) return
    const delta = epoch - seenEpoch.current
    seenEpoch.current = epoch
    if (delta > 0) kickRef.current(delta)
  }, [epoch, example])

  const healthy = events.healthy
  const seenHealthy = useRef(healthy)
  useEffect(() => {
    healthyRef.current = healthy
    if (example) return
    if (seenHealthy.current === healthy) return
    seenHealthy.current = healthy
    rescheduleRef.current()
  }, [healthy, example])

  return state
}
