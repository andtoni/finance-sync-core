export interface MercuryProbeItem {
  readonly id: string
  readonly accountId: string
  readonly status: string | null
  readonly relatedTransactionId: string | null
}

export interface MercuryProbePage {
  readonly items: readonly MercuryProbeItem[]
  readonly nextCursor: string | null
}

export interface MercuryProbeTransport {
  fetchOrganisationPage(cursor: string | null): Promise<MercuryProbePage>
  refetchTransaction(transactionId: string): Promise<MercuryProbeItem>
}

export interface MercuryWebhookFixture {
  readonly transactionId: string
  readonly payload?: unknown
}

export interface MercuryProbeResult {
  readonly probe: 'mercury-g0'
  readonly outcome: 'observed' | 'failed_closed'
  readonly projectionEnabled: false
  readonly activationAllowed: false
  readonly evidence: {
    readonly cursorScope: 'organisation'
    readonly pagesObserved: number
    readonly recordsObserved: number
    readonly nativeStateCertain: boolean
    readonly relationshipCertain: boolean
    readonly webhookUse: 'wake-and-refetch' | 'not-observed'
    readonly refetchObserved: boolean
  }
}

const knownStates = new Set(['pending', 'sent', 'posted', 'cancelled', 'failed'])

export async function runMercuryG0Probe(
  transport: MercuryProbeTransport,
  fixture: { readonly webhook?: MercuryWebhookFixture } = {},
): Promise<MercuryProbeResult> {
  let pagesObserved = 0
  let recordsObserved = 0
  let nativeStateCertain = true
  let relationshipCertain = true
  let refetchObserved = false
  const seen = new Set<string>()
  try {
    let cursor: string | null = null
    do {
      const page = await transport.fetchOrganisationPage(cursor)
      if (!page || !Array.isArray(page.items)) throw new Error('invalid page')
      pagesObserved++
      recordsObserved += page.items.length
      for (const item of page.items) {
        if (!item.status || !knownStates.has(item.status)) nativeStateCertain = false
        if (item.relatedTransactionId === null) relationshipCertain = false
      }
      cursor = page.nextCursor
      if (cursor !== null && seen.has(cursor)) throw new Error('cursor loop')
      if (cursor !== null) seen.add(cursor)
    } while (cursor !== null)

    if (fixture.webhook) {
      const refreshed = await transport.refetchTransaction(fixture.webhook.transactionId)
      if (!refreshed?.id) throw new Error('invalid refetch')
      refetchObserved = true
    }
    return result('observed')
  } catch {
    return result('failed_closed')
  }

  function result(outcome: MercuryProbeResult['outcome']): MercuryProbeResult {
    return {
      probe: 'mercury-g0', outcome, projectionEnabled: false, activationAllowed: false,
      evidence: {
        cursorScope: 'organisation', pagesObserved, recordsObserved,
        nativeStateCertain, relationshipCertain,
        webhookUse: fixture.webhook ? 'wake-and-refetch' : 'not-observed', refetchObserved,
      },
    }
  }
}
