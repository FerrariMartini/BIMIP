import type {
  BinanceStreamMessage,
  BinanceTradeEvent,
} from "../../../schema/binance_trade_event";
import type { RawTrade } from "../../../schema/raw_trade";

function isWrappedMessage(
  message: BinanceStreamMessage,
): message is { stream: string; data: BinanceTradeEvent } {
  return "data" in message;
}

export function extractBinanceTrade(
  message: BinanceStreamMessage,
): BinanceTradeEvent {
  const event = isWrappedMessage(message) ? message.data : message;

  if (event.e !== "trade") {
    throw new Error(`Unexpected Binance event type: ${event.e}`);
  }

  return event;
}

export function normalizeBinanceTrade(event: BinanceTradeEvent): RawTrade {
  return {
    tradeId: event.t,
    symbol: event.s,
    price: Number(event.p),
    quantity: Number(event.q),
    timestamp: event.T,
    side: event.m ? "sell" : "buy",
    exchange: "binance",
  };
}

export function parseBinanceTradeMessage(message: BinanceStreamMessage): RawTrade {
  return normalizeBinanceTrade(extractBinanceTrade(message));
}
