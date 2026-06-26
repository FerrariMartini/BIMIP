export interface MockSourceConfig {
  symbols: string[];
  tradeIntervalMs: number;
}

export function loadMockSourceConfig(symbols: string[]): MockSourceConfig {
  return {
    symbols,
    tradeIntervalMs: Number(process.env.MOCK_TRADE_INTERVAL_MS) || 1000,
  };
}
