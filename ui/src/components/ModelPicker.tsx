import { useMemo, useRef, useState } from 'react'
import { Combobox } from '@base-ui/react/combobox'
import { Check, ChevronDown } from 'lucide-react'

interface ModelChoice {
  id: string | null
  label: string
}

interface ModelPickerProps {
  options: readonly { id: string; label: string }[]
  value: string | null
  onValueChange: (value: string | null) => void
  disabled?: boolean
}

const AGENT_DEFAULT: ModelChoice = { id: null, label: 'Agent default' }

export function ModelPicker({ options, value, onValueChange, disabled }: ModelPickerProps) {
  const inputRef = useRef<HTMLInputElement>(null)
  const [instant, setInstant] = useState(false)
  const items = useMemo(() => {
    const choices: ModelChoice[] = [AGENT_DEFAULT, ...options]
    if (value !== null && !options.some((option) => option.id === value)) {
      choices.push({ id: value, label: value })
    }
    return choices
  }, [options, value])
  const selected = items.find((item) => item.id === value) ?? AGENT_DEFAULT

  return (
    <Combobox.Root
      items={items}
      value={selected}
      disabled={disabled}
      itemToStringLabel={(item) => item.label}
      itemToStringValue={(item) => item.id ?? ''}
      isItemEqualToValue={(item, selectedItem) => item.id === selectedItem.id}
      filter={(item, query) => {
        const search = query.trim().toLowerCase()
        return (
          item.label.toLowerCase().includes(search) || !!item.id?.toLowerCase().includes(search)
        )
      }}
      onValueChange={(item) => {
        if (item) onValueChange(item.id)
      }}
      onOpenChange={(_, details) => setInstant(details.event.type.startsWith('key'))}
    >
      <Combobox.Trigger
        aria-label="Model"
        data-slot="select-trigger"
        className="mw-select flex w-full items-center justify-between gap-2 text-left"
      >
        <span data-slot="select-value" className="min-w-0 flex-1 truncate">
          <Combobox.Value />
        </span>
        <ChevronDown
          className="mw-select-chevron pointer-events-none size-4 shrink-0 text-muted-foreground"
          style={instant ? { transitionDuration: '0ms' } : undefined}
          aria-hidden="true"
        />
      </Combobox.Trigger>
      <Combobox.Portal>
        <Combobox.Positioner align="start" sideOffset={4} className="isolate z-50">
          <Combobox.Popup
            aria-label="Choose model"
            initialFocus={inputRef}
            className="mw-select-popup flex max-h-[min(20rem,var(--available-height))] w-(--anchor-width) min-w-36 flex-col overflow-hidden rounded-lg bg-popover text-popover-foreground shadow-md ring-1 ring-foreground/10"
            style={instant ? { transitionDuration: '0ms' } : undefined}
          >
            <div className="shrink-0 border-b border-border p-2">
              <Combobox.Input
                ref={inputRef}
                aria-label="Search models"
                placeholder="Search models…"
                autoComplete="off"
                spellCheck={false}
                className="mw-input h-9!"
              />
            </div>
            <Combobox.Empty>
              <p className="px-3 py-6 text-center text-[13px] text-muted-foreground">
                No models found.
              </p>
            </Combobox.Empty>
            <Combobox.List className="min-h-0 overflow-y-auto overscroll-contain p-1 empty:p-0">
              {(item: ModelChoice) => (
                <Combobox.Item
                  key={item.id === null ? 'default' : `model:${item.id}`}
                  value={item}
                  className="relative flex min-h-9 cursor-default items-center rounded-md py-1 pr-8 pl-2.5 text-[13px] outline-hidden select-none data-highlighted:bg-accent data-highlighted:text-accent-foreground"
                >
                  <span className="min-w-0 truncate">{item.label}</span>
                  <Combobox.ItemIndicator className="pointer-events-none absolute right-2 flex size-4 items-center justify-center">
                    <Check size={16} aria-hidden="true" />
                  </Combobox.ItemIndicator>
                </Combobox.Item>
              )}
            </Combobox.List>
          </Combobox.Popup>
        </Combobox.Positioner>
      </Combobox.Portal>
    </Combobox.Root>
  )
}
