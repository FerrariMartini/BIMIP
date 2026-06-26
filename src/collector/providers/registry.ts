export const TRADE_PROVIDER_METADATA = {
  binance: { live: true },
  mock: { live: false },
} as const;

export type TradeProvider = keyof typeof TRADE_PROVIDER_METADATA;

export const LIVE_PROVIDERS = (
  Object.entries(TRADE_PROVIDER_METADATA) as [
    TradeProvider,
    (typeof TRADE_PROVIDER_METADATA)[TradeProvider],
  ][]
)
  .filter(([, meta]) => meta.live)
  .map(([name]) => name);
