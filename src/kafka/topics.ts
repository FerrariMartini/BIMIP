const DAY_MS = 24 * 60 * 60 * 1000;

export const TOPICS = {
  RAW_TRADES: "crypto.raw-trades",
  MARKET_WINDOWS: "crypto.market-windows",
  ANOMALIES: "crypto.anomalies",
  INSIGHTS: "crypto.insights",
  DLQ: "crypto.dlq",
} as const;

export const TOPIC_DEFINITIONS = [
  {
    topic: TOPICS.RAW_TRADES,
    numPartitions: 6,
    replicationFactor: 1,
    configEntries: [{ name: "retention.ms", value: String(7 * DAY_MS) }],
  },
  {
    topic: TOPICS.MARKET_WINDOWS,
    numPartitions: 6,
    replicationFactor: 1,
    configEntries: [{ name: "retention.ms", value: String(DAY_MS) }],
  },
  {
    topic: TOPICS.ANOMALIES,
    numPartitions: 6,
    replicationFactor: 1,
    configEntries: [{ name: "retention.ms", value: String(30 * DAY_MS) }],
  },
  {
    topic: TOPICS.INSIGHTS,
    numPartitions: 6,
    replicationFactor: 1,
    configEntries: [{ name: "retention.ms", value: String(30 * DAY_MS) }],
  },
  {
    topic: TOPICS.DLQ,
    numPartitions: 1,
    replicationFactor: 1,
    configEntries: [{ name: "retention.ms", value: String(7 * DAY_MS) }],
  },
] as const;
