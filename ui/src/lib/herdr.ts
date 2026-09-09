import type { DashboardHerdr } from '@/lib/api'

/** Chip copy for a worker's herdr fact. The count is load, never a title. */
export function herdrChip(herdr: DashboardHerdr | null | undefined): string {
  if (herdr == null) return 'herdr: unknown'
  const staleAgo =
    herdr.stale && herdr.age_millis != null ? ` · ${staleAgeAgo(herdr.age_millis)}` : ''
  const agents =
    !herdr.stale && herdr.interactive_agents != null && herdr.interactive_agents > 0
      ? ` · ${herdr.interactive_agents} agent${herdr.interactive_agents === 1 ? '' : 's'}`
      : ''
  const suffix = staleAgo || agents
  switch (herdr.state) {
    case 'available':
      return herdr.version ? `herdr ${herdr.version}${suffix}` : `herdr${suffix}`
    case 'not_installed':
      return `no herdr${suffix}`
    case 'no_socket':
      return `herdr: no socket${suffix}`
    case 'no_response':
      return `herdr: no response${suffix}`
    default:
      return 'herdr: unknown'
  }
}

/** Minutes stay minutes through two hours so 4120 s is `69m ago`, not `1h ago`. */
export function staleAgeAgo(ageMillis: number): string {
  if (!Number.isFinite(ageMillis) || ageMillis < 0) return 'just now'
  const seconds = Math.round(ageMillis / 1000)
  if (seconds < 60) return `${seconds}s ago`
  const minutes = Math.round(seconds / 60)
  if (minutes < 120) return `${minutes}m ago`
  const hours = Math.round(minutes / 60)
  if (hours < 48) return `${hours}h ago`
  return `${Math.round(hours / 24)}d ago`
}
