import {
  createContext,
  useContext,
  useEffect,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from 'react'

import { createControllerEvents, type EventClientOptions } from '@/lib/controllerEvents'

export type EventsStore = {
  healthy: boolean
  snapshotEpoch: number
  globalTaskEpoch: number
  version: number
  taskEpoch: (taskId: string) => number
  subscribe: (listener: () => void) => () => void
  invalidate: (taskIds: string[] | null) => void
  setHealthy: (healthy: boolean) => void
}

export type ControllerEventsSnapshot = {
  healthy: boolean
  snapshotEpoch: number
  taskEpoch: (taskId: string) => number
}

const IDLE_SNAPSHOT: ControllerEventsSnapshot = {
  healthy: false,
  snapshotEpoch: 0,
  taskEpoch: () => 0,
}

export function createEventsStore(): EventsStore {
  let version = 0
  let healthy = false
  let snapshotEpoch = 0
  let globalTaskEpoch = 0
  const taskEpochs = new Map<string, number>()
  const listeners = new Set<() => void>()
  const emit = () => {
    version += 1
    for (const listener of listeners) listener()
  }
  return {
    get healthy() {
      return healthy
    },
    get snapshotEpoch() {
      return snapshotEpoch
    },
    get globalTaskEpoch() {
      return globalTaskEpoch
    },
    get version() {
      return version
    },
    taskEpoch(taskId: string) {
      return globalTaskEpoch + (taskEpochs.get(taskId) ?? 0)
    },
    subscribe(listener: () => void) {
      listeners.add(listener)
      return () => listeners.delete(listener)
    },
    invalidate(taskIds: string[] | null) {
      snapshotEpoch += 1
      if (taskIds === null) globalTaskEpoch += 1
      else for (const id of taskIds) taskEpochs.set(id, (taskEpochs.get(id) ?? 0) + 1)
      emit()
    },
    setHealthy(next: boolean) {
      if (healthy === next) return
      healthy = next
      emit()
    },
  }
}

const IDLE_STORE = createEventsStore()
const ControllerEventsContext = createContext<EventsStore | null>(null)

export function useControllerEvents(): ControllerEventsSnapshot {
  const store = useContext(ControllerEventsContext) ?? IDLE_STORE
  const version = useSyncExternalStore(
    store.subscribe,
    () => store.version,
    () => store.version,
  )
  if (store === IDLE_STORE) return IDLE_SNAPSHOT
  void version
  return {
    healthy: store.healthy,
    snapshotEpoch: store.snapshotEpoch,
    taskEpoch: (taskId: string) => store.taskEpoch(taskId),
  }
}

function inertEventSource(url: string): EventSource {
  return {
    url,
    readyState: 2,
    withCredentials: false,
    onerror: null,
    onmessage: null,
    onopen: null,
    CONNECTING: 0,
    OPEN: 1,
    CLOSED: 2,
    addEventListener() {},
    removeEventListener() {},
    dispatchEvent() {
      return false
    },
    close() {},
  } as EventSource
}

function defaultMakeSource(url: string): EventSource {
  if (typeof EventSource === 'undefined') return inertEventSource(url)
  return new EventSource(url)
}

export function ControllerEventsProvider({
  children,
  makeSource,
  now,
  setTimer,
  clearTimer,
}: {
  children: ReactNode
  makeSource?: EventClientOptions['makeSource']
  now?: EventClientOptions['now']
  setTimer?: EventClientOptions['setTimer']
  clearTimer?: EventClientOptions['clearTimer']
}) {
  const [store] = useState(createEventsStore)

  useEffect(() => {
    const client = createControllerEvents({
      makeSource: makeSource ?? defaultMakeSource,
      now: now ?? (() => Date.now()),
      setTimer: setTimer ?? ((fn, delay) => window.setTimeout(fn, delay) as unknown as number),
      clearTimer: clearTimer ?? ((id) => window.clearTimeout(id)),
      invalidate: (taskIds) => store.invalidate(taskIds),
      health: (healthy) => store.setHealthy(healthy),
    })
    const onVisibility = () => {
      if (document.visibilityState === 'visible') store.invalidate(null)
    }
    document.addEventListener('visibilitychange', onVisibility)
    client.start()
    return () => {
      document.removeEventListener('visibilitychange', onVisibility)
      client.stop()
    }
  }, [clearTimer, makeSource, now, setTimer, store])

  return <ControllerEventsContext.Provider value={store}>{children}</ControllerEventsContext.Provider>
}
