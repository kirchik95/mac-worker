import { useEffect, useId, useRef, useState } from 'react'

/** Original Paper Mac artwork; only the observed activity signal changes. */
export function MacIllustration({ state }: { state: 'working' | 'idle' | 'unknown' }) {
  const id = useId()
  const ref = useRef<SVGSVGElement>(null)
  const previous = useRef(state)
  const [visible, setVisible] = useState(true)
  const [completed, setCompleted] = useState(false)
  useEffect(() => {
    let inView = true
    const sync = () => setVisible(inView && document.visibilityState !== 'hidden')
    const observer =
      typeof IntersectionObserver === 'undefined'
        ? null
        : new IntersectionObserver(([entry]) => {
            inView = entry.isIntersecting
            sync()
          })
    if (ref.current) observer?.observe(ref.current)
    document.addEventListener('visibilitychange', sync)
    sync()
    return () => {
      observer?.disconnect()
      document.removeEventListener('visibilitychange', sync)
    }
  }, [])
  useEffect(() => {
    const finished = previous.current === 'working' && state === 'idle'
    previous.current = state
    setCompleted(finished)
    if (finished) {
      const timer = setTimeout(() => setCompleted(false), 650)
      return () => clearTimeout(timer)
    }
  }, [state])
  return (
    <svg
      viewBox="0 0 240 205"
      className="mw-mac"
      data-state={state}
      data-animate={visible}
      data-completed={completed}
      aria-hidden="true"
      ref={ref}
    >
      <defs>
        <linearGradient id={id + '-top'} x1="0.2" y1="0" x2="0.7" y2="1">
          <stop offset="0" stopColor="#F1F3F6" />
          <stop offset=".55" stopColor="#DFE3E8" />
          <stop offset="1" stopColor="#D6DBE1" />
        </linearGradient>
        <linearGradient id={id + '-front'} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0" stopColor="#D8DDE3" />
          <stop offset=".45" stopColor="#C2C9D1" />
          <stop offset="1" stopColor="#A4ACB5" />
        </linearGradient>
        <linearGradient id={id + '-side'} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0" stopColor="#A6AEB7" />
          <stop offset=".5" stopColor="#9199A3" />
          <stop offset="1" stopColor="#79818B" />
        </linearGradient>
      </defs>
      <ellipse className="mw-mac-shadow-outer" cx="120" cy="176" rx="104" ry="14" />
      <ellipse className="mw-mac-shadow-inner" cx="120" cy="175" rx="64" ry="8" />
      <g className="mw-mac-body">
        <path
          d="M19.7 102A23.3 11.7 0 0 0 26.5 110.2L103.5 148.8A23.3 11.7 0 0 0 120 152.2L120 159.2A23.3 11.7 0 0 1 103.5 155.8L26.5 117.2A23.3 11.7 0 0 1 19.7 109Z"
          fill="#2B2F34"
        />
        <path
          d="M120 152.2A23.3 11.7 0 0 0 136.5 148.8L213.5 110.2A23.3 11.7 0 0 0 220.3 102L220.3 109A23.3 11.7 0 0 1 213.5 117.2L136.5 155.8A23.3 11.7 0 0 1 120 159.2Z"
          fill="#1E2226"
        />
        <path
          d="M20.3 105.7v5.2M22.3 108.3v5.2M25.5 110.6v5.2M29.4 112.6v5.2M33.3 114.6v5.2M37.3 116.5v5.2M41.2 118.5v5.2M45.2 120.5v5.2M49.2 122.5v5.2M53.1 124.5v5.2M57.1 126.4v5.2M61 128.4v5.2M65 130.4v5.2M69 132.4v5.2M72.9 134.4v5.2M76.9 136.3v5.2M80.8 138.3v5.2M84.8 140.3v5.2M88.8 142.3v5.2M92.7 144.3v5.2M96.7 146.2v5.2M100.6 148.2v5.2M104.6 150.2v5.2M109.2 151.7v5.2M114.5 152.7v5.2M120 153.1v5.2"
          fill="none"
          stroke="#4A5058"
          strokeWidth="0.9"
        />
        <path
          d="M125.5 152.7v5.2M130.8 151.7v5.2M135.4 150.2v5.2M139.4 148.2v5.2M143.3 146.2v5.2M147.3 144.3v5.2M151.2 142.3v5.2M155.2 140.3v5.2M159.2 138.3v5.2M163.1 136.3v5.2M167.1 134.4v5.2M171 132.4v5.2M175 130.4v5.2M179 128.4v5.2M182.9 126.4v5.2M186.9 124.5v5.2M190.8 122.5v5.2M194.8 120.5v5.2M198.8 118.5v5.2M202.7 116.5v5.2M206.7 114.6v5.2M210.6 112.6v5.2M214.5 110.6v5.2M217.7 108.3v5.2M219.7 105.7v5.2"
          fill="none"
          stroke="#383D44"
          strokeWidth="0.9"
        />
        <path
          d="M19.7 66A23.3 11.7 0 0 0 26.5 74.2L103.5 112.8A23.3 11.7 0 0 0 120 116.2L120 152.2A23.3 11.7 0 0 1 103.5 148.8L26.5 110.2A23.3 11.7 0 0 1 19.7 102Z"
          fill={'url(#' + id + '-front)'}
        />
        <path
          d="M120 116.2A23.3 11.7 0 0 0 136.5 112.8L213.5 74.2A23.3 11.7 0 0 0 220.3 66L220.3 102A23.3 11.7 0 0 1 213.5 110.2L136.5 148.8A23.3 11.7 0 0 1 120 152.2Z"
          fill={'url(#' + id + '-side)'}
        />
        <path
          d="M19.7 102A23.3 11.7 0 0 0 26.5 110.2L103.5 148.8A23.3 11.7 0 0 0 120 152.2A23.3 11.7 0 0 0 136.5 148.8L213.5 110.2A23.3 11.7 0 0 0 220.3 102"
          fill="none"
          stroke="#787F88"
          strokeWidth="1.2"
        />
        <path
          d="M19.7 66A23.3 11.7 0 0 0 26.5 74.2L103.5 112.8A23.3 11.7 0 0 0 120 116.2A23.3 11.7 0 0 0 136.5 112.8L213.5 74.2A23.3 11.7 0 0 0 220.3 66A23.3 11.7 0 0 0 213.5 57.8L136.5 19.2A23.3 11.7 0 0 0 103.5 19.2L26.5 57.8A23.3 11.7 0 0 0 19.7 66Z"
          fill={'url(#' + id + '-top)'}
        />
        <path
          d="M19.7 66A23.3 11.7 0 0 0 26.5 74.2L103.5 112.8A23.3 11.7 0 0 0 120 116.2A23.3 11.7 0 0 0 136.5 112.8L213.5 74.2A23.3 11.7 0 0 0 220.3 66"
          fill="none"
          stroke="#FAFBFC"
        />
        <path d="M33.6 96.6l-3.2 1.6v8.4l3.2-1.6z" fill="#20242A" />
        <path d="M46.1 102.8l-3.2 1.6v8.4l3.2-1.6z" fill="#20242A" />
        <circle className="mw-mac-glow" cx="84.1" cy="126.1" r="6.5" />
        <circle className="mw-mac-led" cx="84.1" cy="126.1" r="1.9" />
        <ellipse cx="94.2" cy="131.1" rx="2.4" ry="2.1" fill="#1C2026" />
      </g>
    </svg>
  )
}
