import { useEffect, useRef, useState } from 'react'

import { fetchSnapshot, SnapshotPendingError, type Snapshot } from '@/lib/api'
import { exampleSnapshot } from '@/lib/exampleSnapshot'

const POLL_INTERVAL_MS = 2000

export interface SnapshotState {
  snapshot: Snapshot | null
  error: string | null
  /** True once a poll has failed since the last success, so the UI can say so without blanking. */
  offline: boolean
  /** True when the page is showing the design's example snapshot, not the pool. */
  example: boolean
}

/** `?example` renders the artboard's snapshot so every state can be inspected. */
export const wantsExample = () =>
  typeof window !== 'undefined' && new URLSearchParams(window.location.search).has('example')

/** Polls the dashboard snapshot. The server has no push channel, as in the Rust client. */
export function useSnapshot(): SnapshotState {
  const example = wantsExample()
  const [state, setState] = useState<SnapshotState>(() =>
    example
      ? { snapshot: exampleSnapshot(), error: null, offline: false, example: true }
      : { snapshot: null, error: null, offline: false, example: false },
  )
  const latest = useRef<Snapshot | null>(null)

  useEffect(() => {
    if (example) return
    let cancelled = false
    let timer: ReturnType<typeof setTimeout> | null = null
    const controller = new AbortController()

    const schedule = () => {
      if (cancelled) return
      timer = setTimeout(() => {
        timer = null
        void poll()
      }, POLL_INTERVAL_MS)
    }

    const poll = async () => {
      try {
        const snapshot = await fetchSnapshot(controller.signal)
        if (cancelled) return
        latest.current = snapshot
        setState({ snapshot, error: null, offline: false, example: false })
      } catch (error) {
        if (cancelled || controller.signal.aborted) return
        if (error instanceof SnapshotPendingError) {
          setState((current) => ({
            snapshot: current.snapshot,
            error: null,
            offline: false,
            example: false,
          }))
        } else {
          setState({
            snapshot: latest.current,
            error: error instanceof Error ? error.message : String(error),
            offline: true,
            example: false,
          })
        }
      }
      if (!cancelled) schedule()
    }

    void poll()
    return () => {
      cancelled = true
      controller.abort()
      if (timer !== null) clearTimeout(timer)
    }
  }, [example])

  return state
}
