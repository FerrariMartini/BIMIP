import { LIVE_PROVIDERS, type TradeProvider } from "./providers/registry";

export type CollectorMode = "live" | "mock";

export interface CollectorConfig {
  mode: CollectorMode;
  provider: TradeProvider;
  symbols: string[];
}

function resolveProvider(mode: CollectorMode): TradeProvider {
  if (mode === "mock") {
    return "mock";
  }

  const envProvider = process.env.TRADE_PROVIDER ?? "binance";

  if (!LIVE_PROVIDERS.includes(envProvider as TradeProvider)) {
    throw new Error(
      `Unknown TRADE_PROVIDER "${envProvider}". Supported live providers: ${LIVE_PROVIDERS.join(", ")}`,
    );
  }

  return envProvider as TradeProvider;
}

function parseSymbols(value = "BTCUSDT"): string[] {
  return value
    .split(",")
    .map((symbol) => symbol.trim().toUpperCase())
    .filter(Boolean);
}

export function loadCollectorConfig(): CollectorConfig {
  const mode = process.env.COLLECTOR_MODE === "live" ? "live" : "mock";

  return {
    mode,
    provider: resolveProvider(mode),
    symbols: parseSymbols(process.env.SYMBOLS),
  };
}
