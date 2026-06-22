# Architecture Decision Records

This document captures the open architectural questions for the Bitcoin Market Intelligence Platform.
Each section presents the decision to be made, the available options, and the final choice after analysis.

---

## ADR-01 — Market Data Source

### Context

The system needs a real-time stream of Bitcoin trade events. The choice of exchange and API endpoint defines the shape of raw data and the reconnection strategy.

### Options Considered

| Option | Protocol | Auth Required | Cost |
|---|---|---|---|
| Binance WebSocket (`btcusdt@trade`) | WebSocket | No | Free |
| Coinbase Advanced Trade WebSocket | WebSocket | Yes (API Key) | Free |
| Kraken WebSocket v2 | WebSocket | No | Free |

### Reconnection Strategy Options

| Strategy | Description | Drawback |
|---|---|---|
| Fixed Retry | Reconnect after a fixed interval (e.g., always 5s) | Hammers the server under sustained outages |
| Exponential Backoff | Double the delay on each attempt: 1s → 2s → 4s → 8s → 30s (capped) | Thundering herd if many clients fail simultaneously |
| Exponential Backoff with Jitter | Random noise added to each backoff interval: `delay = random(0, min(cap, base * 2^attempt))` | Minimal — industry standard |

### Heartbeat Strategy

WebSocket connections can become "zombie connections": TCP stays open but data stops flowing. A dual-heartbeat approach covers both sides:

| Layer | Direction | Frequency | Purpose |
|---|---|---|---|
| Provider Heartbeat | Binance sends `PING` frame, collector must reply `PONG` | Every ~3 minutes | Binance closes connection if no PONG received within 10 min |
| System Heartbeat | Collector sends `PING` frame to Binance | Every 30 seconds | Detects zombie connections before data loss occurs |

### Decision

**Exchange:** Binance — `wss://stream.binance.com:9443/ws/btcusdt@trade`

**Reconnection:** Exponential Backoff with Jitter
- Base delay: 1s
- Multiplier: 2x per attempt
- Cap: 30s
- Max attempts: unlimited (keep retrying until connection is restored)
- Reset attempt counter after a stable connection lasting > 60s

**Heartbeat:** Dual heartbeat
- Respond to Binance `PING` frames with `PONG` immediately
- Send system-level `PING` every 30s; if no `PONG` received within 10s, treat as disconnected and trigger reconnection

**Raw event shape from Binance (`btcusdt@trade` stream):**
```json
{
  "e": "trade",
  "E": 1716000000000,
  "s": "BTCUSDT",
  "t": 3521456789,
  "p": "67450.12",
  "q": "0.003",
  "T": 1716000000000,
  "m": true
}
```
Mapped fields: `t` → tradeId, `p` → price, `q` → quantity, `T` → timestamp, `m` → isBuyerMarketMaker (side)

**Mock / Test Mode:** The collector must support a mock mode that replays pre-recorded trade events from a local file or generates synthetic events at a configurable rate. This enables local development and testing without a live Binance connection. Mock mode is activated via environment variable (`COLLECTOR_MODE=mock`).

---

## ADR-02 — Kafka Topology

### Context

Kafka is the backbone of the event pipeline. The topic structure, partition count, consumer groups, and serialization format define the contract between all services.

### Persistence Model

**Model A — Storage as dedicated sink consumer.** The Storage module is the only module that writes to the database. All compute modules (Aggregator, Detector, AI) are stateless with respect to the database — they only produce to Kafka. Storage subscribes to all topics and materializes events into the database.

This makes Kafka the **source of truth**: the database is a derived, queryable view. If the database is lost, events can be replayed from Kafka to rebuild it.

### Decision

**Topics:**

| Topic | Producer | Consumers | Retention |
|---|---|---|---|
| `btc.raw-trades` | Collector | Aggregator, Storage | 7 days |
| `btc.market-windows` | Aggregator | Detector, Storage | 1 day |
| `btc.anomalies` | Detector | AI Service, Storage | 30 days |
| `btc.insights` | AI Service | Storage | 30 days |

Topic names are prefixed with `btc.` to namespace them and avoid collisions in a shared Kafka cluster.

**Partitions:** 3 partitions per topic (production-like baseline — enables future horizontal scaling and demonstrates partition assignment concepts, even running with a single consumer in v1).

**Replication factor:** 1 (single broker for local development). Production would use 3.

**Consumer groups:**

| Group ID | Module | Topics consumed |
|---|---|---|
| `btc.aggregator` | Aggregator | `btc.raw-trades` |
| `btc.detector` | Detector | `btc.market-windows` |
| `btc.ai` | AI Service | `btc.anomalies` |
| `btc.storage` | Storage | `btc.raw-trades`, `btc.market-windows`, `btc.anomalies`, `btc.insights` |

Having named consumer groups is critical: `btc.aggregator` and `btc.storage` both consume `btc.raw-trades` independently — each group maintains its own offset, so neither interferes with the other.

**Serialization:** JSON (UTF-8). Readable, no schema registry required, sufficient for this scale.

**Producer config (production-like):**
```
acks: 'all'               — wait for all in-sync replicas to acknowledge
compression: 'snappy'     — reduces network and disk usage
retries: 5                — retry on transient failures
```

**Consumer config (production-like):**
```
enable.auto.commit: false   — manual offset commit only after successful processing
max.poll.interval.ms: 30000 — max time between polls before consumer is considered dead
session.timeout.ms: 10000   — heartbeat timeout
```

**Offset commit strategy:** At-least-once delivery. Offset is committed only after the message is successfully processed (written to DB or forwarded to next topic). Duplicate events on crash are tolerated — Storage uses idempotent writes (upsert by event ID) to handle redelivery safely.

**Dead Letter Queue:** Failed messages after all retries are published to `btc.dlq` with the original topic, payload, and error reason for manual inspection.

**Partition key:** The Collector must set `key = symbol` (e.g., `"BTCUSDT"`) when producing to `btc.raw-trades`. Kafka routes messages with the same key deterministically to the same partition. This guarantees that all trades for a symbol are processed by the same Aggregator instance in order — which is required for correct window calculation. Without this, multiple instances would each receive a partial subset of trades, producing incomplete and incorrect windows.

---

## ADR-03 — Database

### Context

The system needs to persist raw trades, aggregated windows, detected anomalies, and AI-generated insights. The database choice impacts query performance for time-series data and the complexity of the schema.

### Options Considered

| Option | Type | Time-Series Native | Complexity |
|---|---|---|---|
| PostgreSQL + TimescaleDB | Relational + Time-Series extension | Yes | Medium |
| InfluxDB | Purpose-built Time-Series | Yes | Low setup, limited query model |
| MongoDB | Document | No | Low |
| PostgreSQL (vanilla) | Relational | No | Low |

### Decision

**Database:** PostgreSQL + TimescaleDB

TimescaleDB is a PostgreSQL extension that converts time-series tables into hypertables — automatically partitioned by time, with native functions like `time_bucket()` for window aggregation queries. It retains full SQL for relational tables (`anomalies`, `insights`), so there is no need to operate two separate databases.

**Why not the alternatives:**
- InfluxDB: rigid data model makes storing ML features in `anomalies` awkward; SQL is limited
- MongoDB: no native time-series support; range queries on `trades` require careful indexing with no compression benefit
- PostgreSQL vanilla: sufficient for this volume, but misses `time_bucket()` and hypertable compression which are worth learning for production financial systems

**Persistence model:** Storage is the only module that writes to the database (Model A from ADR-02). All other modules are DB-agnostic — they only interact with Kafka.

**Tables and retention:**

| Table | Type | Retention | Notes |
|---|---|---|---|
| `trades` | Hypertable | 90 days | Raw events for backtesting, replay, ML ground truth |
| `windows` | Hypertable | 180 days | Closed aggregation windows — stored even though derived, avoids recalculation |
| `anomalies` | Regular table | Forever | Includes full window features for future ML training |
| `insights` | Regular table | Forever | AI output linked to anomaly |

**Schema:**

```sql
-- Raw trades (hypertable, partitioned by timestamp)
CREATE TABLE trades (
  id          BIGINT        NOT NULL,
  symbol      TEXT          NOT NULL,
  price       NUMERIC(18,8) NOT NULL,
  quantity    NUMERIC(18,8) NOT NULL,
  side        TEXT          NOT NULL,  -- 'buy' | 'sell'
  exchange    TEXT          NOT NULL,
  timestamp   TIMESTAMPTZ   NOT NULL,
  PRIMARY KEY (id, timestamp)
);
SELECT create_hypertable('trades', 'timestamp');

-- Closed aggregation windows (hypertable)
CREATE TABLE windows (
  id               UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
  symbol           TEXT          NOT NULL,
  window_size      TEXT          NOT NULL,  -- '1m' | '5m' | '15m'
  window_start     TIMESTAMPTZ   NOT NULL,
  window_end       TIMESTAMPTZ   NOT NULL,
  open_price       NUMERIC(18,8) NOT NULL,
  close_price      NUMERIC(18,8) NOT NULL,
  high_price       NUMERIC(18,8) NOT NULL,
  low_price        NUMERIC(18,8) NOT NULL,
  avg_price        NUMERIC(18,8) NOT NULL,
  volume           NUMERIC(18,8) NOT NULL,
  trade_count      INTEGER       NOT NULL,
  price_change_pct NUMERIC(8,4)  NOT NULL,
  volume_delta     NUMERIC(18,8) NOT NULL,
  UNIQUE (symbol, window_start, window_end, window_size)
  -- Uniqueness enforced for two reasons:
  -- 1. Idempotent inserts (ON CONFLICT DO NOTHING) handle Kafka at-least-once redelivery
  -- 2. Prevents duplicate rows from corrupting the Detector's Z-score baseline queries
);
SELECT create_hypertable('windows', 'window_start');

-- Detected anomalies (includes full feature set for future ML training)
CREATE TABLE anomalies (
  id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  window_id        UUID        REFERENCES windows(id),
  type             TEXT        NOT NULL,  -- 'HighVolume' | 'PriceShock' | 'Momentum'
  severity         TEXT        NOT NULL,  -- 'low' | 'medium' | 'high'
  trigger_value    NUMERIC     NOT NULL,
  threshold_value  NUMERIC     NOT NULL,

  -- Raw window metrics at detection time
  price_change_pct      NUMERIC(8,4),   -- open → close % change
  max_drawdown_pct      NUMERIC(8,4),   -- max intra-window price pullback (high → low / high)
  price_volatility      NUMERIC(8,4),   -- stddev of trade prices within window
  volume_change_pct     NUMERIC(8,4),   -- volume vs previous window
  volume_acceleration   NUMERIC(8,4),   -- rate of volume change (first half vs second half)
  trade_count           INTEGER,
  buy_sell_ratio        NUMERIC(8,4),   -- buy volume / total volume (requires side on trades)
  trend_direction       TEXT,           -- 'up' | 'down' | 'sideways'

  -- Z-scores at detection time (computed values, not recalculated)
  z_score_volume        NUMERIC(8,4),
  z_score_price_change  NUMERIC(8,4),
  z_score_trade_count   NUMERIC(8,4),

  -- Baseline stats at detection time — critical for ML: reconstructs what the model saw
  -- Without these, you cannot recover the exact baseline that triggered the anomaly
  baseline_mean_volume        NUMERIC(18,8),
  baseline_stddev_volume      NUMERIC(18,8),
  baseline_mean_price_change  NUMERIC(8,4),
  baseline_stddev_price_change NUMERIC(8,4),

  detected_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- AI-generated insights
CREATE TABLE insights (
  id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  anomaly_id   UUID        REFERENCES anomalies(id),
  content      TEXT        NOT NULL,
  model        TEXT        NOT NULL,
  tokens_used  INTEGER,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**Idempotent writes:** Storage uses `INSERT ... ON CONFLICT DO NOTHING` on both `trades` (by `id + timestamp`) and `windows` (by `symbol + window_start + window_end + window_size`) to handle at-least-once Kafka redelivery without duplicates. Duplicate window rows would silently corrupt the Detector's Z-score baseline by inflating rolling averages.

---

## ADR-04 — Aggregation Window State Management

### Context

The aggregation service must maintain state for open time windows (e.g., a 1-minute window currently accumulating trades). This is the most technically complex decision because it defines crash recovery, horizontal scalability, and backpressure behavior.

### Options Considered

| Option | State Storage | Crash Recovery | Horizontal Scale | Complexity |
|---|---|---|---|---|
| In-memory only | Process memory | State lost on crash | No (single instance) | Low |
| In-memory + Redis | Redis | Full recovery | Yes (Redis as shared state) | Medium |
| Kafka Streams state store | RocksDB (embedded) | Full recovery via Kafka | Yes (partition-based) | High |
| DB polling (micro-batch) | Database | Full recovery | Yes (row-level locking) | Medium |

### Horizontal Scalability Analysis

Stateful windowed aggregation cannot be naively scaled horizontally: if multiple Aggregator instances each receive a subset of trades for the same symbol, they produce partial and incorrect windows.

The correct solutions for horizontal scale are:
- **Redis shared state** — all instances update shared atomic accumulators (`HINCRBYFLOAT`); any instance can finalize a window when the clock triggers. No DB required on the hot path. This is the natural evolution of in-memory for Node.js.
- **Two-stage aggregation** — partial aggregators produce partial sums to a merge aggregator (Map-Reduce over streams). Used by Apache Flink and Kafka Streams natively.
- **DB polling (micro-batch)** — Aggregator queries raw trades from DB, processes in batches, uses row locks. This abandons the streaming model, introduces write-then-read latency, and makes the DB the critical path bottleneck. Not recommended.

### Decision

**v1: In-memory, single instance**

The Aggregator maintains window state entirely in process memory. At ~150 trades/second for BTCUSDT, a single Node.js instance handles this with negligible CPU usage — arithmetic operations are orders of magnitude faster than the I/O bottlenecks (Kafka, DB writes).

**Window type:** Tumbling windows (fixed, non-overlapping). Each window closes at a fixed boundary (e.g., :00, :01, :02...) regardless of trade volume.

**Window close trigger:** Wall clock timer. A `setInterval` aligned to the window boundary closes the current accumulator and publishes the closed window to `btc.market-windows`. Event watermarks are not used in v1.

**Crash recovery:** On restart, the in-progress window is lost. The Aggregator resumes from the next window boundary. This is acceptable for v1 — at most one partial window is lost per crash, and the DB retains all previously closed windows.

**In-memory accumulator structure:**
```typescript
interface WindowAccumulator {
  symbol:       string
  windowSize:   '1m' | '5m' | '15m'
  windowStart:  number
  sum:          number
  count:        number
  min:          number
  max:          number
  firstPrice:   number
  lastPrice:    number
  volume:       number
}
```

**Known limitation:** This design does not scale horizontally for the Aggregator. A second instance consuming the same Kafka partition is not possible without coordination — Kafka's consumer group protocol assigns each partition to exactly one instance, so this limitation is enforced at the infrastructure level in v1.

**v2 evolution path: Redis shared state**

When horizontal scaling becomes necessary (multiple high-volume symbols, multiple exchanges), the migration path is:
1. Replace in-memory accumulators with Redis `HINCRBYFLOAT` / `HINCRBY` operations (atomic, no locks needed)
2. Window finalization uses a Redis `SET NX` lock so exactly one instance publishes the closed window
3. Any number of Aggregator instances can process trades for any symbol without coordination overhead
4. No changes required to Detector, Storage, or AI modules — only the Aggregator internals change

---

## ADR-05 — Anomaly Detection Thresholds

### Context

The detection service needs to compare current market metrics against baselines to decide if an anomaly occurred. The definition of "baseline" and how thresholds are managed directly affects detection quality and false-positive rates.

### Options Considered

| Approach | Baseline Source | Adaptability | Complexity |
|---|---|---|---|
| Static thresholds | Config file / env vars | None | Low |
| Rolling average (last N windows) | In-memory or Redis | Moderate | Medium |
| Z-score (Redis baseline) | Redis rolling history | High | Medium |
| Z-score (DB query) | TimescaleDB historical data | High | Medium-High |

### Decision

**Approach:** Z-score with Redis as baseline state store, static thresholds as cold-start fallback.

**Why Redis over DB for the hot path:** Every closed window triggers a baseline lookup. At one window per minute, a DB query is feasible, but Redis at < 1ms latency keeps the Detector fully in the event-driven path without I/O blocking. The DB retains all windows as the source of truth for historical analysis and ML — Redis holds only the working set (last 60 windows).

**Baseline window:** 60 windows per window size per symbol. For 1-minute windows this equals 1 hour of market context.

**Z-score threshold:** Z > 2.5 — approximately 99% of normal values fall below this, minimizing false positives while catching genuine outliers.

**Redis data structure:**
```
Key:   btc:baseline:{symbol}:{windowSize}    e.g. btc:baseline:BTCUSDT:1m
Type:  Sorted Set (ZSET)
Score: window_start timestamp (epoch ms)
Value: JSON { volume, price_change_pct, trade_count, avg_price }

On each window received:
  ZADD   key  <timestamp>  <json>     — append new window
  ZREMRANGEBYRANK  key  0  -62        — keep only last 60 entries
  ZRANGE  key  0  -1  WITHSCORES     — fetch all for baseline calculation
```

**Detection rules:**
```
z_volume = (volume - mean_volume) / stddev_volume
z_price  = (price_change_pct - mean_price_change) / stddev_price_change
z_count  = (trade_count - mean_count) / stddev_count

HighVolumeDetected:   z_volume > 2.5
PriceShockDetected:   z_price  > 2.5
MomentumDetected:     z_volume > 2.0 AND z_price > 1.5 AND z_count > 1.5
```

**Cold start fallback:** When Redis has fewer than `MIN_SAMPLES=30` entries for a key (first run or Redis restart), the Detector falls back to static thresholds configured via environment variables. Once 30 samples are accumulated, it switches automatically to Z-score detection.

```
DETECTOR_FALLBACK_PRICE_SHOCK_PCT=3.0
DETECTOR_FALLBACK_HIGH_VOLUME_MULTIPLIER=2.5
DETECTOR_MIN_SAMPLES=30
```

**Multiple anomalies per window:** A single closed window can trigger multiple anomaly types simultaneously. Each detected anomaly is published as a separate event to `btc.anomalies`. All anomalies trigger AI analysis.

**AI trigger:** All detected anomalies trigger an AI insight, regardless of severity. Severity is computed and stored for future ML use but does not gate the AI call in v1.

**Feature storage for ML:** Every anomaly event published to Kafka and persisted in the `anomalies` table includes the full window feature set (z-scores, raw values, baseline mean/stddev at detection time). This constitutes labeled training data for a future ML-based detector.

---

## ADR-06 — AI Service

### Context

When an anomaly is detected, the AI service generates a human-readable market analysis. The provider, prompt design, and cost control strategy need to be defined before implementation.

### Options Considered

| Provider | Model | Cost | Latency |
|---|---|---|---|
| OpenAI | GPT-4o / GPT-4o-mini | Medium / Low | Low |
| Anthropic | Claude Haiku / Sonnet | Low / Medium | Low |
| Local (Ollama) | LLaMA 3, Mistral | Free | High (hardware dependent) |

### Decision

**Provider:** OpenAI GPT-4o-mini — low cost (~$0.15/1M input tokens), low latency, sufficient quality for structured market analysis output.

**Call strategy: 30-second batch window**

The AI Service buffers incoming anomaly events for 30 seconds per symbol. When the timer fires, all buffered anomalies are sent in a single API call with full combined context. This produces richer analysis when multiple anomaly types occur simultaneously (e.g., PriceShock + HighVolume + Momentum in the same market event) and keeps API costs predictable.

```
t=0s    PriceShock arrives   ──┐
t=15s   HighVolume arrives   ──┤── buffer per symbol
t=25s   Momentum arrives     ──┘
t=30s   timer fires          ──► single OpenAI call with all 3 anomalies as context
                                  ──► 1 insight published to btc.insights
```

If only one anomaly arrives and no others follow within 30s, the call fires immediately at timer expiry with single-anomaly context.

**Unavailability handling:** If the OpenAI API is unavailable or returns an error, the AI Service logs the failure, publishes nothing to `btc.insights`, and does not block the pipeline. The anomaly is already persisted in the `anomalies` table. The missing insight is a known gap, not a pipeline failure.

**Prompt template:**

```
System:
You are a Bitcoin market analyst. Provide concise, objective analysis based only on the data provided.
Respond in JSON with fields: explanation, possible_causes (array), risk_assessment.

User:
{count} market anomaly event(s) detected for {symbol} at {timestamp}.

Detected anomalies:
{foreach anomaly}
- {type} (z-score: {z_score:.2f}) — {metric}: {value} vs baseline {baseline_mean} ± {baseline_stddev}
{/foreach}

Window context (last 1 minute):
- Price: {open_price} → {close_price} ({price_change_pct:+.2f}%)
- Volume: {volume} (z={z_volume:.2f})
- Trade count: {trade_count} (z={z_count:.2f})
- Trend: {trend_direction}

Provide: explanation of what is happening, possible causes, and risk assessment.
```

**Response schema:**
```typescript
interface InsightContent {
  explanation:     string   // 2-3 sentences describing the market event
  possible_causes: string[] // 2-4 likely causes
  risk_assessment: string   // brief risk level and context
}
```

**Async execution:** Insight generation is fully asynchronous. The AI Service consumes from `btc.anomalies`, batches events in memory, fires the API call, and publishes to `btc.insights` — all without blocking any other module. A slow or failing AI call has zero impact on trade ingestion, aggregation, or detection.

**Kafka offset commit during batch window:** While anomalies are buffered in the 30-second window, their Kafka offsets are not yet committed. If the process crashes mid-buffer, Kafka redelivers all buffered anomalies on restart — they are rebuffered and the API call is eventually made. Offsets are committed only after the batch call completes (success or terminal failure). No anomalies are silently dropped due to a crash during buffering.

**AI provider error classification:**

Not all API errors should be handled the same way. The AI Service classifies errors before deciding to retry, skip, or alert:

| HTTP Status | Meaning | Action |
|---|---|---|
| `429 Too Many Requests` | Rate limited — temporary | Retry with exponential backoff + jitter; respect `Retry-After` header if present |
| `500 Internal Server Error` | OpenAI transient failure | Retry up to 3x with exponential backoff + jitter (base 1s, cap 4s) |
| `503 Service Unavailable` | OpenAI outage | Retry + increment circuit breaker failure counter |
| `403 Forbidden` | Invalid API key or quota exhausted | Do NOT retry — log as critical alert, skip all subsequent calls until resolved |
| `400 Bad Request` | Malformed prompt payload | Do NOT retry — log full payload to DLQ for inspection, skip insight |
| Timeout (> 30s) | Network or provider latency | Retry once; if second attempt also times out, skip and increment circuit breaker counter |

**Token budget:** Each call is estimated at ~300-500 input tokens + ~200 output tokens. At GPT-4o-mini pricing, cost per insight is approximately $0.0001. Even at 1000 insights/day this is ~$0.10/day.

---

## ADR-07 — Project Structure

### Context

The architecture diagram shows 4 services (Collector, Storage, Aggregation, Detection) plus an AI Service. The project structure must define whether these run as independent processes in a monorepo or as a modular monolith.

### Options Considered

| Option | Structure | Deployment | Complexity |
|---|---|---|---|
| Modular Monolith | Single Node.js process, internal modules | Single process | Low |
| Monorepo (npm workspaces) | Multiple packages, each a service | Multiple processes | Medium |
| Multi-repo | Separate Git repositories | Multiple processes | High |

### Decision

**Structure:** Modular Monolith — single Node.js process with well-isolated internal modules.

Each module has clear boundaries and communicates via internal interfaces. This allows the system to be refactored into independent services later without architectural rework, as the module contracts are already well-defined.

**Schema format:** TypeScript interfaces — no additional tooling required, sufficient for this scale and learning scope.

**Project layout:**

```
bitcoin/
├── src/
│   ├── collector/           — WebSocket ingestion, Kafka producer
│   │   ├── binance/         — live Binance WebSocket client
│   │   ├── mock/            — mock/replay mode for tests
│   │   └── index.ts
│   ├── aggregator/          — window state management, window events
│   │   └── index.ts
│   ├── detector/            — anomaly detection logic, anomaly events
│   │   └── index.ts
│   ├── ai/                  — AI provider client, insight generation
│   │   └── index.ts
│   ├── storage/             — database writes for all event types
│   │   └── index.ts
│   ├── kafka/               — Kafka producer/consumer setup, topic definitions
│   │   └── index.ts
│   ├── schemas/             — TypeScript interfaces for all message types
│   │   └── index.ts
│   ├── logger/              — Pino logger configuration
│   │   └── index.ts
│   └── main.ts              — entry point, wires all modules together
├── infra/
│   └── docker-compose.yml   — Kafka (KRaft), PostgreSQL, Prometheus, Grafana
├── tests/
│   ├── unit/
│   └── integration/
├── .env.example
└── package.json
```

**Migration path to microservices:** Because each module is self-contained with its own Kafka consumer/producer, each can be extracted into an independent process by moving it to a separate package and pointing it at the same Kafka cluster — no logic changes required.

---

## ADR-08 — Observability Stack

### Context

The README specifies logs, metrics, and health checks as requirements. The tooling choice affects how the system is monitored in development and production.

### Proposed Stack

| Concern | Tool | Justification |
|---|---|---|
| Structured logging | Pino | Fastest Node.js logger, JSON output, low overhead |
| Metrics | Prometheus (via `prom-client`) | Standard, integrates with Grafana |
| Visualization | Grafana | Pairs with Prometheus, free OSS |
| Distributed tracing | OpenTelemetry | Vendor-neutral, future-proof |
| Health checks | HTTP `/health` endpoint per service | Simple, works with Docker and k8s |

### Key Metrics to Expose

- `trades_received_total` — counter
- `trades_per_second` — gauge
- `kafka_consumer_lag` — gauge (per topic/partition)
- `anomalies_detected_total` — counter (labeled by type)
- `ai_requests_total` — counter (labeled by status: success/error)
- `window_processing_duration_ms` — histogram

### Decision

**Logging:** Pino — structured JSON logs, lowest overhead of any Node.js logger. Every log entry includes `module`, `symbol`, `traceId` (UUID per trade batch) for correlation.

**Metrics:** Prometheus via `prom-client`. Each module exposes metrics; the main process aggregates and serves at `GET /metrics` (Prometheus scrape endpoint).

**Visualization:** Prometheus `/metrics` endpoint only for v1. Grafana added in v2 when dashboards become useful for observing live behavior.

**Distributed tracing:** Not in v1. The system is a monolith — Pino structured logs with correlation IDs provide sufficient observability without the overhead of OpenTelemetry instrumentation.

**Health check:** `GET /health` returns `{ status: "ok", uptime, kafkaConnected, dbConnected, redisConnected }`.

**Metrics registry:**

| Metric | Type | Labels |
|---|---|---|
| `btc_trades_received_total` | Counter | `symbol`, `exchange` |
| `btc_trades_per_second` | Gauge | `symbol` |
| `btc_kafka_consumer_lag` | Gauge | `topic`, `partition`, `group` |
| `btc_windows_closed_total` | Counter | `symbol`, `window_size` |
| `btc_anomalies_detected_total` | Counter | `symbol`, `type` |
| `btc_ai_requests_total` | Counter | `status` (success/error/skipped) |
| `btc_ai_batch_size` | Histogram | — |
| `btc_window_processing_duration_ms` | Histogram | `window_size` |
| `btc_websocket_reconnections_total` | Counter | — |

---

## ADR-09 — Local Development Environment

### Context

The system depends on Kafka, TimescaleDB, and Redis. These must run locally without cloud dependencies.

### Decision

**Kafka mode:** KRaft (no Zookeeper) — simpler setup, one less container, the default since Kafka 3.x.

**Docker Compose stack:**

```yaml
services:
  kafka:          # Bitnami Kafka (KRaft mode), ports 9092
  timescaledb:    # timescale/timescaledb-ha, port 5432
  redis:          # redis:alpine, port 6379
  kafka-ui:       # provectuslabs/kafka-ui, port 8080
                  # — inspect topics, consumer group lag, messages in browser
```

**Kafka UI (Provectus):** Included from day one. Inspecting topic contents, consumer group offsets, and message payloads in the browser is essential for learning and debugging the event pipeline — far more productive than CLI tools during development.

**Prometheus:** Not in Docker Compose for v1. The `/metrics` endpoint is scraped manually or tested with curl during development.

**Environment variables** (`.env.example`):
```
KAFKA_BROKERS=localhost:9092
POSTGRES_URL=postgresql://bitcoin:bitcoin@localhost:5432/bitcoin
REDIS_URL=redis://localhost:6379
BINANCE_WS_URL=wss://stream.binance.com:9443/ws/btcusdt@trade
COLLECTOR_MODE=live           # live | mock
OPENAI_API_KEY=sk-...
DETECTOR_MIN_SAMPLES=30
DETECTOR_ZSCORE_THRESHOLD=2.5
AI_BATCH_WINDOW_MS=30000
```

---

## ADR-10 — Backpressure Strategy

### Context

The system receives high-frequency trade events from WebSocket and must avoid overwhelming downstream stages. This is directly tied to the Node.js Streams learning objective.

### Decision

Backpressure is implemented from day one using Node.js Streams as the core pipeline primitive. Each module stage is a `Transform` stream — backpressure propagates automatically when a downstream stage is slow.

**Pipeline stages:**

```
Binance WebSocket
    │  (Readable)
    ▼
CollectorStream       — Transform: parse and normalize raw trade JSON
    │
    ▼
KafkaProducerStream   — Writable: produce to btc.raw-trades
                        highWaterMark: 1000 messages
                        pause() when producer internal buffer > 80% capacity
                        resume() when buffer drains below 20%
```

**Kafka consumer backpressure:**

```typescript
consumer.on('message', async (message) => {
  consumer.pause()              // stop polling immediately
  await processMessage(message) // do the work
  consumer.resume()             // allow next message
})
```

This ensures the consumer never accumulates a queue of unprocessed messages in memory — it processes one at a time and pulls the next only when ready. For the Aggregator (pure arithmetic), this adds no meaningful latency. For Storage (DB writes), it prevents write queue buildup.

**AI Service batch buffer:** The 30-second batch window (ADR-06) naturally acts as backpressure for the AI API — anomalies accumulate in a bounded buffer and are flushed at a controlled rate regardless of detection bursts.

**Event shedding:** Not acceptable for trades (financial data integrity). Acceptable only for AI insights under sustained API failure — the circuit breaker (ADR-11) handles this case.

---

## ADR-11 — Reliability and Error Handling

### Context

The non-functional requirement states "no events should be lost during normal operation." This requires defining retry policies, dead-letter queues, and error boundaries per module.

### Decision

**At-least-once delivery** end-to-end. Idempotent writes in Storage handle redelivery without duplicates (upsert by event ID, already defined in ADR-03).

**Per-module error policy:**

| Module | Error type | Action |
|---|---|---|
| Collector (WebSocket) | Connection drop | Exponential backoff with jitter reconnect (ADR-01) |
| Collector (Kafka produce) | Broker unavailable | Retry 5x with exponential backoff + jitter; if all fail, buffer in memory up to 10k messages, then pause WebSocket |
| Storage (DB write) | Transient error | Retry 3x with exponential backoff + jitter (base 1s, cap 4s); on final failure → DLQ |
| Aggregator | Any processing error | Log + skip trade; window continues with remaining trades |
| Detector | Any processing error | Log + skip window; do not publish anomaly for that window |
| AI Service | `429` rate limit | Retry with exponential backoff + jitter; respect `Retry-After` header if present |
| AI Service | `500` / `503` transient | Retry up to 3x with exponential backoff + jitter (base 1s, cap 4s); increment circuit breaker counter |
| AI Service | `403` forbidden | Do NOT retry — log as critical alert, disable AI calls until API key is resolved |
| AI Service | `400` bad request | Do NOT retry — send payload to `btc.dlq` for manual inspection |
| AI Service | Timeout (> 30s) | Retry once; on second timeout skip insight and increment circuit breaker counter |
| Any consumer | Unhandled exception | Log error, do NOT commit offset → Kafka redelivers the message |

**Dead Letter Queue:** `btc.dlq` topic receives messages that exhausted all retries. Each DLQ message includes:
```json
{
  "originalTopic": "btc.raw-trades",
  "originalPayload": "...",
  "errorMessage": "...",
  "failedAt": "2024-01-01T00:00:00Z",
  "attemptCount": 3
}
```

**Circuit breaker for AI:** After 5 consecutive OpenAI API failures, the AI Service enters open state — all incoming anomalies are logged and skipped without attempting API calls. After 120 seconds, it enters half-open state and retries one call. If successful, it resets to closed.

---

## ADR-12 — Output Interface

### Context

The README shows the pipeline ending at "Insights" but does not define how users or external systems consume the output.

### Decision

**REST API + SSE (Server-Sent Events)** — served from the same Node.js process as a new `api` module within the monolith.

**Why SSE over WebSocket for real-time push:**
The output stream is unidirectional — the server pushes events, clients only consume. SSE is purpose-built for this pattern and is more resilient than WebSocket in three specific ways:

1. **Native reconnection with resume:** The browser `EventSource` API reconnects automatically on drop. Each event carries an `id` field; on reconnect the browser sends `Last-Event-ID` and the server replays missed events from that point — no events lost during transient disconnections.
2. **Proxy and firewall compatibility:** SSE is plain HTTP (`text/event-stream`). WebSocket requires a protocol upgrade that some corporate proxies, CDNs, and load balancers block or require special configuration for.
3. **HTTP/2 multiplexing:** SSE over HTTP/2 allows multiple event streams over a single TCP connection natively. WebSocket is incompatible with HTTP/2 multiplexing.

WebSocket would be the correct choice only if clients needed to send data back in real-time (e.g., subscribe to specific symbols, set thresholds on-the-fly). That is out of scope for v1.

**REST API** — historical queries:

```
GET /api/v1/anomalies              — list anomalies (filter by type, date range)
GET /api/v1/anomalies/:id          — get anomaly with its insight
GET /api/v1/insights               — list AI insights
GET /api/v1/windows?size=1m        — list closed windows
GET /health                        — health check
GET /metrics                       — Prometheus scrape endpoint
```

**SSE endpoint** — real-time push:

```
GET /api/v1/events
Content-Type: text/event-stream

Server emits events as they occur:

  id: 1741
  event: trade
  data: {"tradeId":3521456789,"price":67450.12,"quantity":0.003,"side":"buy"}

  id: 1742
  event: anomaly
  data: {"type":"PriceShock","symbol":"BTCUSDT","zScore":3.1,"detectedAt":"..."}

  id: 1743
  event: insight
  data: {"anomalyId":"...","explanation":"Strong buying pressure..."}

  : heartbeat        ← comment line every 30s to keep connection alive

Reconnect with resume:
  Client drops after id: 1742
  Browser reconnects automatically:  GET /api/v1/events  Last-Event-ID: 1742
  Server resumes from id: 1743 — no events missed
```

Event IDs are monotonically increasing integers stored in Redis (`INCR btc:event:seq`), shared across all server instances. On reconnect, the server queries the DB for events after `Last-Event-ID` to replay missed events.

**Framework:** Fastify with `@fastify/reply-from` for streaming responses. SSE requires no additional plugin — it is a standard HTTP response with `Transfer-Encoding: chunked`.

---

## Summary Table

| ADR | Topic | Status |
|---|---|---|
| ADR-01 | Market Data Source | **Decided** |
| ADR-02 | Kafka Topology | **Decided** |
| ADR-03 | Database | **Decided** |
| ADR-04 | Aggregation Window State | **Decided** |
| ADR-05 | Anomaly Detection Thresholds | **Decided** |
| ADR-06 | AI Service | **Decided** |
| ADR-07 | Project Structure | **Decided** |
| ADR-08 | Observability Stack | **Decided** |
| ADR-09 | Local Development Environment | **Decided** |
| ADR-10 | Backpressure Strategy | **Decided** |
| ADR-11 | Reliability and Error Handling | **Decided** |
| ADR-12 | Output Interface | **Decided** |
