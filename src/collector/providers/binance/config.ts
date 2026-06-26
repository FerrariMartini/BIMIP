export interface BinanceSourceConfig {
  symbols: string[];
  wsUrl: string;
}

export function loadBinanceSourceConfig(
  symbols: string[],
): BinanceSourceConfig {
  if (!process.env.BINANCE_WS_URL) throw new Error("BINANCE_WS_URL missing");
  return {
    symbols,
    wsUrl: process.env.BINANCE_WS_URL,
  };
}
