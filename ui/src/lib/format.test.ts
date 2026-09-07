import { describe, expect, it } from 'vitest'

import { duration, bytes, humanize, relativeTime, shortId } from './format'

describe('relativeTime', () => {
  const now = 1_000_000_000

  it('reports every scale it supports', () => {
    expect(relativeTime(now - 5_000, now)).toBe('5s ago')
    expect(relativeTime(now - 120_000, now)).toBe('2m ago')
    expect(relativeTime(now - 3 * 3_600_000, now)).toBe('3h ago')
    expect(relativeTime(now - 5 * 86_400_000, now)).toBe('5d ago')
  })

  it('never renders a missing or future timestamp as a negative age', () => {
    expect(relativeTime(null, now)).toBe('—')
    expect(relativeTime(undefined, now)).toBe('—')
    expect(relativeTime(now + 10_000, now)).toBe('just now')
  })
})

describe('bytes', () => {
  it('scales and keeps one decimal only where it reads', () => {
    expect(bytes(512)).toBe('512 B')
    expect(bytes(1536)).toBe('1.5 KB')
    expect(bytes(150 * 1024 ** 3)).toBe('150.0 GB')
    expect(bytes(null)).toBe('—')
  })
})

describe('shortId and humanize', () => {
  it('truncates ids and leaves short ones alone', () => {
    expect(shortId('0123456789abcdef')).toBe('01234567')
    expect(shortId('abc')).toBe('abc')
    expect(shortId(null)).toBe('—')
  })

  it('turns wire spellings into prose', () => {
    expect(humanize('needs_input')).toBe('Needs input')
    expect(humanize('timed-out')).toBe('Timed out')
    expect(humanize(null)).toBe('—')
  })
})

describe('duration', () => {
  it('prints a span, not a distance from now', () => {
    expect(duration(44_000)).toBe('44s')
    expect(duration(22 * 60_000)).toBe('22m')
    expect(duration(3 * 3_600_000 + 12 * 60_000)).toBe('3h 12m')
    expect(duration(2 * 86_400_000 + 4 * 3_600_000)).toBe('2d 4h')
  })

  it('drops an empty remainder rather than printing 3h 0m', () => {
    expect(duration(3 * 3_600_000)).toBe('3h')
    expect(duration(86_400_000)).toBe('1d')
  })

  it('has no span to print when the host reported no time', () => {
    expect(duration(null)).toBe('—')
    expect(duration(Number.NaN)).toBe('—')
  })
})
