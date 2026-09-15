import type { AgentSetting, ModelOption } from '@/lib/api'

/** Choices are scoped to the selected Mac and its authenticated native agent. */
export function modelOptionsFor(setting: AgentSetting): ModelOption[] {
  const options = [...setting.model_options]
  if (setting.model && !options.some((option) => option.id === setting.model)) {
    options.push({
      id: setting.model,
      label: setting.model,
      effort_options: setting.effort_options,
      fast_supported: setting.fast_supported,
    })
  }
  return options
}
