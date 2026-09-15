export function dashboardProxyOrigin(
  origin: string | undefined,
  host: string | undefined,
  target: string,
  encrypted = false,
): string | undefined {
  if (!origin || !host) return origin

  try {
    const incoming = new URL(`${encrypted ? 'https' : 'http'}://${host}`)
    // Require a canonical loopback Host and its exact browser Origin. In
    // particular, URL parsing must not turn a malformed Host into a trusted one.
    if (
      incoming.host !== host ||
      !['127.0.0.1', 'localhost', '[::1]'].includes(incoming.hostname) ||
      origin !== incoming.origin
    ) {
      return origin
    }
    const destination = new URL(target)
    if (destination.protocol === 'http:' || destination.protocol === 'https:') {
      return destination.origin
    }
  } catch {
    // An unverified header must reach the backend unchanged.
  }
  return origin
}
