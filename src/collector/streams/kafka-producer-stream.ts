import { Writable } from "node:stream";
import type { Producer } from "kafkajs";

import { TOPICS } from "../../kafka/topics";
import type { RawTrade } from "../../schema/raw_trade";

const MAX_RETRIES = 5;
const BASE_RETRY_MS = 250;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export class KafkaProducerStream extends Writable {
  private pending = 0;

  constructor(private readonly producer: Producer) {
    super({
      objectMode: true,
      highWaterMark: 1000,
    });
  }

  getBufferedAmount(): number {
    return this.pending;
  }

  _write(
    trade: RawTrade,
    _encoding: BufferEncoding,
    callback: (error?: Error | null) => void,
  ): void {
    this.pending += 1;
    void this.publishWithRetry(trade)
      .then(() => {
        this.pending -= 1;
        callback();
      })
      .catch((error: unknown) => {
        this.pending -= 1;
        callback(error instanceof Error ? error : new Error(String(error)));
      });
  }

  private async publishWithRetry(trade: RawTrade): Promise<void> {
    let lastError: Error | undefined;

    for (let attempt = 0; attempt < MAX_RETRIES; attempt += 1) {
      try {
        await this.producer.send({
          topic: TOPICS.RAW_TRADES,
          messages: [
            {
              key: null,
              value: JSON.stringify(trade),
            },
          ],
        });
        return;
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        await sleep(BASE_RETRY_MS * 2 ** attempt);
      }
    }

    throw lastError ?? new Error("Failed to publish trade to Kafka");
  }
}
