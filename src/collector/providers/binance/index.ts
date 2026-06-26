import { Readable } from "node:stream";
import WebSocket from "ws";

import type { BinanceStreamMessage } from "../../../schema/binance_trade_event";
import { backoffDelayMs, createStableConnectionTimer } from "../shared/backoff";
import type { BinanceSourceConfig } from "./config";
import { parseBinanceTradeMessage } from "./normalize";

const HEARTBEAT_INTERVAL_MS = 30_000;
const HEARTBEAT_TIMEOUT_MS = 10_000;

function buildStreamUrl(config: BinanceSourceConfig): string {
  const streams = config.symbols
    .map((symbol) => `${symbol.toLowerCase()}@trade`)
    .join("/");

  if (config.symbols.length === 1) {
    return `${config.wsUrl}/ws/${streams}`;
  }

  return `${config.wsUrl}/stream?streams=${streams}`;
}

export function createBinanceTradeSource(config: BinanceSourceConfig): Readable {
  let ws: WebSocket | undefined;
  let reconnectAttempt = 0;
  let reconnectTimer: NodeJS.Timeout | undefined;
  let heartbeatTimer: NodeJS.Timeout | undefined;
  let heartbeatTimeout: NodeJS.Timeout | undefined;
  let stableConnectionTimer: NodeJS.Timeout | undefined;
  let closed = false;

  const source = new Readable({
    objectMode: true,
    read() {
      // Events are pushed when the WebSocket receives messages.
    },
  });

  function clearHeartbeat(): void {
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = undefined;
    }

    if (heartbeatTimeout) {
      clearTimeout(heartbeatTimeout);
      heartbeatTimeout = undefined;
    }
  }

  function clearStableConnectionTimer(): void {
    if (stableConnectionTimer) {
      clearTimeout(stableConnectionTimer);
      stableConnectionTimer = undefined;
    }
  }

  function scheduleReconnect(reason: string): void {
    if (closed) {
      return;
    }

    clearHeartbeat();
    clearStableConnectionTimer();

    if (ws) {
      ws.removeAllListeners();
      ws.terminate();
      ws = undefined;
    }

    const delay = backoffDelayMs(reconnectAttempt);
    reconnectAttempt += 1;

    console.log(`[collector:binance] reconnecting in ${delay}ms (${reason})`);

    reconnectTimer = setTimeout(() => {
      connect();
    }, delay);
  }

  function startHeartbeat(socket: WebSocket): void {
    clearHeartbeat();

    heartbeatTimer = setInterval(() => {
      if (socket.readyState !== WebSocket.OPEN) {
        return;
      }

      heartbeatTimeout = setTimeout(() => {
        scheduleReconnect("heartbeat timeout");
      }, HEARTBEAT_TIMEOUT_MS);

      socket.ping();
    }, HEARTBEAT_INTERVAL_MS);
  }

  function connect(): void {
    if (closed) {
      return;
    }

    const url = buildStreamUrl(config);
    ws = new WebSocket(url);

    ws.on("open", () => {
      console.log(`[collector:binance] connected to ${url}`);
      startHeartbeat(ws!);

      stableConnectionTimer = createStableConnectionTimer(() => {
        reconnectAttempt = 0;
        console.log("[collector:binance] connection stable, backoff reset");
      });
    });

    ws.on("pong", () => {
      if (heartbeatTimeout) {
        clearTimeout(heartbeatTimeout);
        heartbeatTimeout = undefined;
      }
    });

    ws.on("message", (data) => {
      try {
        const message = JSON.parse(data.toString()) as BinanceStreamMessage;
        source.push(parseBinanceTradeMessage(message));
      } catch (error) {
        console.error("[collector:binance] failed to parse message", error);
      }
    });

    ws.on("close", () => {
      scheduleReconnect("connection closed");
    });

    ws.on("error", (error) => {
      console.error("[collector:binance] socket error", error);
      scheduleReconnect("socket error");
    });
  }

  connect();

  source.on("close", () => {
    closed = true;

    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
    }

    clearHeartbeat();
    clearStableConnectionTimer();

    if (ws) {
      ws.removeAllListeners();
      ws.terminate();
    }
  });

  return source;
}
