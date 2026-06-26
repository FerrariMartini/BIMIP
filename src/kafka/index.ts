import { Kafka, type Producer, logLevel } from "kafkajs";

import { TOPIC_DEFINITIONS } from "./topics";

function parseBrokers(value = "localhost:9092"): string[] {
  return value
    .split(",")
    .map((broker) => broker.trim())
    .filter(Boolean);
}

export function createKafka(): Kafka {
  return new Kafka({
    clientId: "cmip",
    brokers: parseBrokers(process.env.KAFKA_BROKERS),
    logLevel: logLevel.WARN,
  });
}

export async function ensureTopics(kafka = createKafka()): Promise<void> {
  const admin = kafka.admin();

  await admin.connect();
  try {
    const existing = new Set(await admin.listTopics());
    const missing = TOPIC_DEFINITIONS.filter(
      (definition) => !existing.has(definition.topic),
    );

    if (missing.length > 0) {
      await admin.createTopics({
        topics: missing.map((definition) => ({
          topic: definition.topic,
          numPartitions: definition.numPartitions,
          replicationFactor: definition.replicationFactor,
          configEntries: definition.configEntries.map((entry) => ({
            name: entry.name,
            value: entry.value,
          })),
        })),
      });
    }
  } finally {
    await admin.disconnect();
  }
}

export async function createProducer(kafka = createKafka()): Promise<Producer> {
  const producer = kafka.producer({
    allowAutoTopicCreation: false,
    retry: {
      retries: 5,
    },
  });

  await producer.connect();
  return producer;
}
