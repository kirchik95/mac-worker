export function relativeTime(millis: number | null | undefined, now = Date.now()): string {
  if (millis == null) return '—'
  const seconds = Math.round((now - millis) / 1000)
  if (!Number.isFinite(seconds)) return '—'
  if (seconds < 0) return 'just now'
  if (seconds < 60) return `${seconds}s ago`
  const minutes = Math.round(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.round(minutes / 60)
  if (hours < 48) return `${hours}h ago`
  return `${Math.round(hours / 24)}d ago`
}

export function bytes(value: number | null | undefined): string {
  if (value == null) return '—'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let size = value
  let unit = 0
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024
    unit += 1
  }
  // The artboards print storage with one decimal from GB upwards.
  const decimal = unit >= 3 || (size < 10 && unit > 0)
  return `${decimal ? size.toFixed(1) : Math.round(size)} ${units[unit]}`
}

export const shortId = (value: string | null | undefined, length = 8) =>
  value ? value.slice(0, length) : '—'

export const humanize = (value: string | null | undefined) =>
  value ? value.replace(/[_-]/g, ' ').replace(/^./, (c) => c.toUpperCase()) : '—'

/** The artboards show counters zero-padded to two digits. */
export const pad2 = (value: number) => String(value).padStart(2, '0')

export function clockTime(millis: number | null | undefined): string {
  if (millis == null) return '--:--:--'
  const date = new Date(millis)
  return [date.getHours(), date.getMinutes(), date.getSeconds()]
    .map((part) => String(part).padStart(2, '0'))
    .join(':')
}

/** A span, as the artboards print one: 44s, 22m, 3h 12m, 2d 4h. */
export function duration(millis: number | null | undefined): string {
  if (millis == null || !Number.isFinite(millis)) return '—'
  const seconds = Math.max(0, Math.round(millis / 1000))
  if (seconds < 60) return `${seconds}s`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return minutes % 60 === 0 ? `${hours}h` : `${hours}h ${minutes % 60}m`
  const days = Math.floor(hours / 24)
  return hours % 24 === 0 ? `${days}d` : `${days}d ${hours % 24}h`
}
