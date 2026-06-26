export interface BinanceTradeEvent {
  e: "trade";
  E: number;
  s: string;
  t: number;
  p: string;
  q: string;
  T: number;
  m: boolean;
}

export type BinanceStreamMessage =
  | BinanceTradeEvent
  | {
      stream: string;
      data: BinanceTradeEvent;
    };
