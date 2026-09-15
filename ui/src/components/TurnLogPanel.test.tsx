import { fireEvent, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { useTurnLog, type TurnLog } from '@/hooks/useTurnLog'
import { TurnLogPanel } from './TurnLogPanel'

vi.mock('@/hooks/useTurnLog', () => ({ useTurnLog: vi.fn() }))

const useTurnLogMock = vi.mocked(useTurnLog)
const props = {
  taskId: 'task-1',
  turnId: 'turn-1',
  turnNumber: 1,
  live: true,
  truncated: false,
}

let contentHeight: number

function output(text: string, error: string | null = null): TurnLog {
  return { text, offset: new TextEncoder().encode(text).byteLength, error }
}

function viewport() {
  return screen.getByRole('log')
}

function scrollTo(top: number) {
  viewport().scrollTop = top
  fireEvent.scroll(viewport())
}

beforeEach(() => {
  contentHeight = 1000
  // JSDOM does not lay out overflow. These dimensions model a 200px viewport.
  vi.spyOn(HTMLElement.prototype, 'scrollHeight', 'get').mockImplementation(() => contentHeight)
  vi.spyOn(HTMLElement.prototype, 'clientHeight', 'get').mockReturnValue(200)
  useTurnLogMock.mockReset()
  useTurnLogMock.mockReturnValue(output('first line'))
})

afterEach(() => vi.restoreAllMocks())

describe('TurnLogPanel', () => {
  it('opens at the latest output and follows appended text while already at the bottom', () => {
    const { rerender } = render(<TurnLogPanel {...props} />)

    expect(viewport()).toHaveAccessibleName('Turn 1 stdout output')
    expect(viewport()).toHaveAttribute('tabindex', '0')
    expect(viewport().scrollTop).toBe(800)

    contentHeight = 1200
    useTurnLogMock.mockReturnValue(output('first line\nnew line'))
    rerender(<TurnLogPanel {...props} />)

    expect(viewport().scrollTop).toBe(1000)
    expect(screen.getByText('new line')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Jump to latest' })).not.toBeInTheDocument()
  })

  it('preserves the reading position when output arrives after scrolling upward', () => {
    const { rerender } = render(<TurnLogPanel {...props} />)
    scrollTo(320)

    contentHeight = 1400
    useTurnLogMock.mockReturnValue(output('first line\nnew line\nlatest line'))
    rerender(<TurnLogPanel {...props} />)

    expect(viewport().scrollTop).toBe(320)
    expect(screen.getByRole('button', { name: 'Jump to latest' })).toBeInTheDocument()
    expect(screen.getByText(/Reading earlier output/)).toBeInTheDocument()
  })

  it('jumps immediately by keyboard and resumes following later output', async () => {
    const user = userEvent.setup()
    const { rerender } = render(<TurnLogPanel {...props} />)
    scrollTo(100)
    contentHeight = 1200
    useTurnLogMock.mockReturnValue(output('first line\nnew line'))
    rerender(<TurnLogPanel {...props} />)

    screen.getByRole('button', { name: 'Jump to latest' }).focus()
    await user.keyboard('{Enter}')

    expect(viewport().scrollTop).toBe(1000)
    expect(viewport()).toHaveFocus()
    expect(screen.queryByRole('button', { name: 'Jump to latest' })).not.toBeInTheDocument()

    contentHeight = 1500
    useTurnLogMock.mockReturnValue(output('first line\nnew line\nlatest line'))
    rerender(<TurnLogPanel {...props} />)
    expect(viewport().scrollTop).toBe(1300)
  })

  it('resumes following when the reader scrolls manually back to the bottom', () => {
    const { rerender } = render(<TurnLogPanel {...props} />)
    scrollTo(320)
    scrollTo(800)

    expect(screen.queryByRole('button', { name: 'Jump to latest' })).not.toBeInTheDocument()
    contentHeight = 1200
    useTurnLogMock.mockReturnValue(output('first line\nnew line'))
    rerender(<TurnLogPanel {...props} />)

    expect(viewport().scrollTop).toBe(1000)
  })

  it.each([
    ['task', { taskId: 'task-2' }],
    ['turn', { turnId: 'turn-2', turnNumber: 2 }],
  ])('resets following for a different %s, even when its text matches', (_identity, next) => {
    const { rerender } = render(<TurnLogPanel {...props} />)
    scrollTo(320)
    rerender(<TurnLogPanel {...props} {...next} />)

    expect(viewport().scrollTop).toBe(800)
    expect(screen.queryByRole('button', { name: 'Jump to latest' })).not.toBeInTheDocument()

    contentHeight = 1200
    useTurnLogMock.mockReturnValue(output('first line\nnew identity output'))
    rerender(<TurnLogPanel {...props} {...next} />)
    expect(viewport().scrollTop).toBe(1000)
  })

  it('resets following and reads the chosen stream when switching stdout and stderr', async () => {
    const user = userEvent.setup()
    useTurnLogMock.mockImplementation((_task, _turn, stream) => output(`${stream} line`))
    render(<TurnLogPanel {...props} />)
    scrollTo(320)

    await user.click(screen.getByRole('button', { name: 'stderr' }))

    expect(viewport()).toHaveAccessibleName('Turn 1 stderr output')
    expect(screen.getByText('stderr line')).toBeInTheDocument()
    expect(screen.queryByText('stdout line')).not.toBeInTheDocument()
    expect(viewport().scrollTop).toBe(800)
    expect(screen.queryByRole('button', { name: 'Jump to latest' })).not.toBeInTheDocument()
  })

  it('keeps the empty and error messages and the worker retention command available', () => {
    useTurnLogMock.mockReturnValue(output(''))
    const { rerender } = render(<TurnLogPanel {...props} />)
    expect(screen.getByText('Waiting for output…')).toBeInTheDocument()

    rerender(<TurnLogPanel {...props} live={false} />)
    expect(screen.getByText('This turn recorded no output on this stream.')).toBeInTheDocument()

    useTurnLogMock.mockReturnValue(output('', 'connection lost'))
    rerender(<TurnLogPanel {...props} live={false} truncated />)
    expect(screen.getByText('Cannot read the log: connection lost')).toBeInTheDocument()
    expect(screen.getByText('worker task logs task-1 --turn 1')).toBeInTheDocument()
    expect(
      screen.getByText('The host truncated this log; the full text stays on the worker.'),
    ).toBeInTheDocument()
  })
})
