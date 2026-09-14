import { Collapsible as Primitive } from '@base-ui/react/collapsible'
import { cn } from '@/lib/utils'

const Collapsible = Primitive.Root
const CollapsibleTrigger = Primitive.Trigger

function CollapsibleContent({ className, ...props }: Primitive.Panel.Props) {
  return (
    <Primitive.Panel
      className={(state) =>
        cn('mw-disclosure-panel', typeof className === 'function' ? className(state) : className)
      }
      render={(panelProps, state) => <div {...panelProps} inert={!state.open} />}
      {...props}
    />
  )
}

export { Collapsible, CollapsibleTrigger, CollapsibleContent }
