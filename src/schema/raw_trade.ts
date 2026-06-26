export type TradeSide = "buy" | "sell";

export interface RawTrade {
  tradeId: number;
  symbol: string;
  price: number;
  quantity: number;
  timestamp: number;
  side: TradeSide;
  exchange: string;
}
