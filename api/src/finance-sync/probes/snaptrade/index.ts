export interface SnapTradeProbeHolding {
  readonly id: string
  readonly asOf: string | null
}

export interface SnapTradeProbePage {
  readonly holdings: readonly SnapTradeProbeHolding[]
  readonly hasMore: boolean
  readonly complete: boolean
}

export interface SnapTradeProbeTransport {
  fetchHoldingsPage(offset: number, limit: number): Promise<SnapTradeProbePage>
}

export interface SnapTradeProbeResult {
  readonly probe: 'snaptrade-g0'
  readonly outcome: 'observed' | 'unstable' | 'partial' | 'failed_closed'
  readonly projectionEnabled: false
  readonly activationAllowed: false
  readonly evidence: {
    readonly pagination: 'offset'
    readonly pagesObserved: number
    readonly holdingsObserved: number
    readonly boundaryStable: boolean
    readonly holdingsComplete: boolean
    readonly sourceAsOf: string | null
    readonly freshnessSeconds: number | null
  }
}

export async function runSnapTradeG0Probe(
  transport: SnapTradeProbeTransport,
  options: { readonly pageSize?: number; readonly observedAt?: string } = {},
): Promise<SnapTradeProbeResult> {
  const pageSize = options.pageSize ?? 100
  let pagesObserved = 0
  let holdingsObserved = 0
  let boundaryStable = true
  let holdingsComplete = true
  const asOfValues: number[] = []
  try {
    if (!Number.isInteger(pageSize) || pageSize <= 0) throw new Error('invalid page size')
    let offset = 0
    let priorLastId: string | null = null
    while (true) {
      const page = await transport.fetchHoldingsPage(offset, pageSize)
      validate(page)
      pagesObserved++
      holdingsObserved += page.holdings.length
      holdingsComplete &&= page.complete
      if (priorLastId !== null && page.holdings[0]?.id !== priorLastId) boundaryStable = false
      for (const holding of page.holdings) {
        if (holding.asOf !== null) {
          const timestamp = Date.parse(holding.asOf)
          if (!Number.isFinite(timestamp)) throw new Error('invalid as-of')
          asOfValues.push(timestamp)
        }
      }
      if (!page.hasMore) break
      priorLastId = page.holdings.at(-1)?.id ?? null
      if (!priorLastId) throw new Error('empty continuation page')
      offset += pageSize
    }
    // Re-read the final offset boundary: offset pagination can mutate between calls.
    if (offset > 0) {
      const sentinel = await transport.fetchHoldingsPage(offset, pageSize)
      validate(sentinel)
      const expected = priorLastId
      if (sentinel.holdings[0]?.id !== expected) boundaryStable = false
      holdingsComplete &&= sentinel.complete
    }
    return make(!holdingsComplete ? 'partial' : !boundaryStable ? 'unstable' : 'observed')
  } catch {
    return make('failed_closed')
  }

  function make(outcome: SnapTradeProbeResult['outcome']): SnapTradeProbeResult {
    const sourceMs = asOfValues.length ? Math.min(...asOfValues) : null
    const observedMs = Date.parse(options.observedAt ?? new Date().toISOString())
    return {
      probe: 'snaptrade-g0', outcome, projectionEnabled: false, activationAllowed: false,
      evidence: {
        pagination: 'offset', pagesObserved, holdingsObserved, boundaryStable, holdingsComplete,
        sourceAsOf: sourceMs === null ? null : new Date(sourceMs).toISOString(),
        freshnessSeconds: sourceMs === null || !Number.isFinite(observedMs) ? null : Math.max(0, Math.floor((observedMs - sourceMs) / 1000)),
      },
    }
  }
}

function validate(page: SnapTradeProbePage): void {
  if (!page || !Array.isArray(page.holdings) || typeof page.hasMore !== 'boolean' || typeof page.complete !== 'boolean') {
    throw new Error('invalid page')
  }
}
