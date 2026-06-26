import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { Producer } from "kafkajs";

import { createProducer, ensureTopics } from "../kafka";
import type { CollectorConfig } from "./config";
import { loadCollectorConfig } from "./config";
import { createTradeSource } from "./providers";
import { KafkaProducerStream } from "./streams/kafka-producer-stream";

const BACKPRESSURE_HIGH_WATER_MARK = 0.8;
const BACKPRESSURE_LOW_WATER_MARK = 0.2;

export interface CollectorHandle {
  stop: () => Promise<void>;
}

function wireBackpressure(
  source: Readable,
  sink: KafkaProducerStream,
): NodeJS.Timeout {
  return setInterval(() => {
    const capacity = sink.writableHighWaterMark;
    const buffered = sink.writableLength + sink.getBufferedAmount();
    const ratio = buffered / capacity;

    if (ratio > BACKPRESSURE_HIGH_WATER_MARK && !source.isPaused()) {
      source.pause();
      console.warn("[collector] upstream paused due to Kafka backpressure");
    }

    if (ratio < BACKPRESSURE_LOW_WATER_MARK && source.isPaused()) {
      source.resume();
      console.log("[collector] upstream resumed");
    }
  }, 250);
}

async function startPipeline(
  config: CollectorConfig,
  producer: Producer,
): Promise<CollectorHandle> {
  const source = createTradeSource(config);
  const sink = new KafkaProducerStream(producer);
  const backpressureTimer = wireBackpressure(source, sink);

  const pipelinePromise = pipeline(source, sink).catch((error) => {
    console.error("[collector] pipeline failed", error);
  });

  console.log(
    `[collector] started (mode=${config.mode}, provider=${config.provider})`,
  );

  return {
    stop: async () => {
      clearInterval(backpressureTimer);
      source.destroy();
      sink.destroy();
      await pipelinePromise.catch(() => undefined);
    },
  };
}

export async function startCollector(
  config: CollectorConfig = loadCollectorConfig(),
): Promise<CollectorHandle & { producer: Producer }> {
  if (config.symbols.length === 0) {
    throw new Error("SYMBOLS must include at least one trading pair");
  }

  await ensureTopics();
  const producer = await createProducer();
  const handle = await startPipeline(config, producer);

  return {
    ...handle,
    producer,
  };
}
