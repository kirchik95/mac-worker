import type { DashboardHerdr } from '@/lib/api'

/** Chip copy for a worker's herdr fact. The count is load, never a title. */
export function herdrChip(herdr: DashboardHerdr | null | undefined): string {
  if (herdr == null) return 'herdr: unknown'
  const agents =
    herdr.interactive_agents != null && herdr.interactive_agents > 0
      ? ` · ${herdr.interactive_agents} agent${herdr.interactive_agents === 1 ? '' : 's'}`
      : ''
  switch (herdr.state) {
    case 'available':
      return herdr.version ? `herdr ${herdr.version}${agents}` : `herdr${agents}`
    case 'not_installed':
      return 'no herdr'
    case 'no_socket':
      return 'herdr: no socket'
    case 'no_response':
      return 'herdr: no response'
    default:
      return 'herdr: unknown'
  }
}
