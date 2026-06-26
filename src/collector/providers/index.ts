import type { Readable } from "node:stream";

import type { CollectorConfig } from "../config";
import { createBinanceTradeSource } from "./binance";
import { loadBinanceSourceConfig } from "./binance/config";
import { createMockTradeSource } from "./mock";
import { loadMockSourceConfig } from "./mock/config";

export function createTradeSource(config: CollectorConfig): Readable {
  switch (config.provider) {
    case "binance":
      return createBinanceTradeSource(loadBinanceSourceConfig(config.symbols));
    case "mock":
      return createMockTradeSource(loadMockSourceConfig(config.symbols));
  }
}

export type { TradeProvider } from "./registry";
