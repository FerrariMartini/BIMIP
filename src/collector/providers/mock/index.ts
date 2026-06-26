import { Readable } from "node:stream";

import type { RawTrade } from "../../../schema/raw_trade";
import type { MockSourceConfig } from "./config";

function randomBetween(min: number, max: number): number {
  return min + Math.random() * (max - min);
}

function createMockTrade(symbol: string, tradeId: number): RawTrade {
  const price = randomBetween(60_000, 70_000);
  const quantity = randomBetween(0.001, 0.05);
  const timestamp = Date.now();
  const side = Math.random() > 0.5 ? "sell" : "buy";

  return {
    tradeId,
    symbol,
    price,
    quantity,
    timestamp,
    side,
    exchange: "mock",
  };
}

export function createMockTradeSource(config: MockSourceConfig): Readable {
  let tradeId = 1;
  let timer: NodeJS.Timeout | undefined;
  let closed = false;

  const source = new Readable({
    objectMode: true,
    read() {
      // Events are pushed on an interval.
    },
  });

  function emitTrades(): void {
    for (const symbol of config.symbols) {
      source.push(createMockTrade(symbol, tradeId));
      tradeId += 1;
    }
  }

  console.log(
    `[collector:mock] generating trades every ${config.tradeIntervalMs}ms for ${config.symbols.join(", ")}`,
  );

  emitTrades();
  timer = setInterval(emitTrades, config.tradeIntervalMs);

  source.on("close", () => {
    closed = true;
    if (timer) {
      clearInterval(timer);
    }
  });

  source.on("error", () => {
    if (!closed && timer) {
      clearInterval(timer);
    }
  });

  return source;
}
