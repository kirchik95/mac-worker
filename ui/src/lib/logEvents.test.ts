import { describe, expect, it } from 'vitest'

import { parseLogEvents } from './logEvents'

describe('parseLogEvents', () => {
  it('reads the kind and message out of an agent event', () => {
    const events = parseLogEvents(
      '{"type":"item.completed","item":{"type":"command_execution","command":"cargo test"}}',
    )
    expect(events).toEqual([{ time: null, kind: 'command_execution', message: 'cargo test' }])
  })

  it('keeps a plain line verbatim rather than dropping it', () => {
    expect(parseLogEvents('warning: unused variable')).toEqual([
      { time: null, kind: null, message: 'warning: unused variable' },
    ])
  })

  it('keeps a malformed JSON line verbatim', () => {
    const line = '{"type":"broken"'
    expect(parseLogEvents(line)[0].message).toBe(line)
  })

  it('reads a timestamp when the agent supplies one', () => {
    const events = parseLogEvents('{"type":"turn","timestamp":1788736047000,"message":"started"}')
    expect(events[0].time).toMatch(/^\d{2}:\d{2}:\d{2}$/)
    expect(events[0].message).toBe('started')
  })

  it('ignores blank lines', () => {
    expect(parseLogEvents('a\n\n\nb')).toHaveLength(2)
  })
})
