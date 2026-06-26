# Crypto Market Intelligence Platform

## Overview

Crypto Market Intelligence Platform is a real-time event processing system designed to ingest live cryptocurrency trading activity, detect market anomalies, identify trend changes, and generate AI-powered market insights.

The platform supports multiple symbols simultaneously (e.g., BTCUSDT, ETHUSDT, SOLUSDT) and continuously processes public market events, transforming raw trade data into actionable information for traders, analysts, and researchers.

Rather than attempting to predict the future price of assets directly, the platform focuses on identifying relevant market signals and providing contextual explanations that support trading decisions.

---

# Business Goal

Build a real-time Crypto Market Intelligence Platform capable of:

* Ingesting live cryptocurrency trading events for multiple symbols.
* Detecting abnormal market behavior per symbol.
* Identifying trend changes.
* Generating AI-powered market insights.
* Providing historical analysis of detected events.

The platform should operate continuously, processing events as they arrive and transforming raw market data into business intelligence.

---

# Functional Requirements

## Trade Ingestion

The system must consume real-time cryptocurrency trade events from public market data providers for one or more configured symbols.

Each event should contain information such as:

* Trade ID
* Price
* Quantity
* Timestamp
* Side (Buy/Sell when available)

---

## Historical Storage

The system must persist all received events for future analysis.

Historical data should support:

* Event replay
* Trend analysis
* Backtesting
* Auditability

---

## Market Aggregation

The system must continuously generate aggregated metrics over time windows.

Examples:

### 1 Minute Window

* Average Price
* Maximum Price
* Minimum Price
* Total Volume
* Number of Trades

### 5 Minute Window

* Average Price
* Volume Delta
* Price Variation

### 15 Minute Window

* Trend Indicators
* Relative Volume Analysis

---

## Anomaly Detection

The platform must detect abnormal market behavior.

Examples:

### High Volume Event

Triggered when:

* Current volume exceeds a configurable threshold relative to historical averages.

---

### Price Shock Event

Triggered when:

* Price variation exceeds a predefined percentage within a short time window.

---

### Momentum Event

Triggered when:

* Price increases rapidly.
* Trade volume increases simultaneously.
* Trade frequency increases.

---

## AI Market Analysis

When an anomaly is detected, the platform must generate an AI-powered analysis.

Example:

Input:

* Price Change
* Volume Change
* Trade Count Change
* Trend Indicators

Output:

* Human-readable explanation
* Market context
* Possible causes
* Risk assessment

Example:

"The market is experiencing strong buying pressure accompanied by unusually high trading volume, which may indicate the beginning of a bullish momentum movement."

---

# Non-Functional Requirements

## Scalability

The system must support continuous processing of high-frequency market events.

---

## Reliability

No events should be lost during normal operation.

---

## Observability

The platform must expose:

* Logs
* Metrics
* Health Checks

Examples:

* Events Processed
* Events Per Second
* Kafka Consumer Lag
* Anomalies Detected

---

## Performance

The system should process events with minimal latency.

Target:

* Near Real-Time Processing

---

# Business Events

The system is event-driven.

Core business events:

* TradeReceived
* MarketWindowClosed
* HighVolumeDetected
* PriceShockDetected
* MomentumDetected
* InsightGenerated

---

# Architecture Decisions

All architectural decisions are documented in [ARCHITECTURE.md](ARCHITECTURE.md) as ADRs (Architecture Decision Records). Each record covers context, options considered, tradeoffs, and the final decision for topics including Kafka topology, database, window state management, anomaly detection, AI provider, backpressure, and reliability.

---

# Architecture

## System Overview

```text
                        Binance WebSocket
                  (btcusdt@trade, ethusdt@trade ...)
                               │
                               ▼
                    ┌─────────────────────┐
                    │      Collector       │  Node.js Readable Stream
                    │  Exponential Backoff │  Dual heartbeat (30s)
                    │  Mock mode for tests │
                    └──────────┬──────────┘
                               │  round-robin (no partition key)
                               ▼
                    ╔══════════════════════╗
                    ║  crypto.raw-trades   ║  Kafka (6 partitions, 7d retention)
                    ╚═══════╤══════════════╝
                            │
            ┌───────────────┼───────────────┐
            │               │               │
            ▼               ▼               │
  ┌──────────────┐  ┌──────────────┐        │
  │  Aggregator  │  │   Storage    │        │
  │  Redis HASH  │  │  TimescaleDB │        │
  │  HINCRBYFLOAT│  │  (trades)    │        │
  │  SET NX lock │  └──────────────┘        │
  └──────┬───────┘                          │
         │                                  │
         │  tumbling windows (1m, 5m, 15m)  │
         │  window close via SET NX lock    │
         ▼                                  │
╔══════════════════════════╗                │
║  crypto.market-windows   ║  Kafka (1d)    │
╚══════════╤═══════════════╝                │
           │                                │
     ┌─────┴────────┐                       │
     │              │                       │
     ▼              ▼                       │
┌──────────┐  ┌──────────────┐              │
│ Detector │  │   Storage    │              │
│  Redis   │  │  TimescaleDB │              │
│ Z-score  │  │  (windows)   │              │
│ baseline │  └──────────────┘              │
└────┬─────┘                                │
     │  Z > 2.5 → anomaly event             │
     ▼                                      │
╔═══════════════════╗                       │
║  crypto.anomalies ║  Kafka (30d)          │
╚══════╤════════════╝                       │
       │                                    │
  ┌────┴──────────┐                         │
  │               │                         │
  ▼               ▼                         │
┌──────────┐  ┌──────────────┐              │
│ AI Svc   │  │   Storage    │              │
│ 30s batch│  │  TimescaleDB │◄─────────────┘
│ GPT-4o   │  │  (anomalies) │  (also writes raw trades)
│  -mini   │  └──────────────┘
└────┬─────┘
     │
     ▼
╔══════════════════╗
║  crypto.insights ║  Kafka (30d)
╚══════╤═══════════╝
       │
  ┌────┴──────────┐
  │               │
  ▼               ▼
┌──────────┐  ┌──────────────┐
│  API     │  │   Storage    │
│ Fastify  │  │  TimescaleDB │
│ REST +   │  │  (insights)  │
│   SSE    │  └──────────────┘
└──────────┘
     │
     ├── GET /api/v1/anomalies
     ├── GET /api/v1/insights
     ├── GET /api/v1/windows
     ├── GET /api/v1/events  (SSE)
     ├── GET /health
     └── GET /metrics
```

## Module Responsibilities

| Module | Reads from | Writes to | State |
|---|---|---|---|
| Collector | Binance WebSocket | `crypto.raw-trades` | None |
| Aggregator | `crypto.raw-trades` | `crypto.market-windows` | Redis (HASH per symbol+window) |
| Detector | `crypto.market-windows` | `crypto.anomalies` | Redis (60-window baseline) |
| AI Service | `crypto.anomalies` | `crypto.insights` | In-memory 30s batch buffer |
| Storage | All 4 topics | TimescaleDB | None |
| API | TimescaleDB + event bus | REST/SSE clients | None |

## Kafka Topics

| Topic | Producer | Consumers | Partitions | Retention |
|---|---|---|---|---|
| `crypto.raw-trades` | Collector | Aggregator, Storage | 6 | 7 days |
| `crypto.market-windows` | Aggregator | Detector, Storage | 6 | 1 day |
| `crypto.anomalies` | Detector | AI Service, Storage | 6 | 30 days |
| `crypto.insights` | AI Service | Storage | 6 | 30 days |
| `crypto.dlq` | Any (on failure) | Manual inspection | 1 | 7 days |

---

# Technology Stack

| Concern | Technology | Rationale |
|---|---|---|
| Runtime | Node.js (TypeScript) | Event-driven I/O, Streams API, learning goal |
| Message broker | Apache Kafka (KRaft) | Durable event log, consumer groups, replay |
| Database | PostgreSQL + TimescaleDB | SQL + native time-series, hypertables |
| Aggregator + Detector state | Redis | Atomic HINCRBYFLOAT for window accumulators; Z-score baseline; SET NX finalization lock |
| AI | OpenAI GPT-4o-mini | Low cost (~$0.0001/insight), sufficient quality |
| HTTP/WS server | Fastify | Low overhead, built-in schema validation |
| Logging | Pino | Structured JSON, lowest Node.js overhead |
| Metrics | Prometheus (`prom-client`) | Standard scrape endpoint |
| Dev environment | Docker Compose | Kafka, TimescaleDB, Redis, Kafka UI |

---

# Project Structure

```
bitcoin/
├── src/
│   ├── collector/           — trade ingestion, Kafka producer
│   │   ├── providers/       — provider registry + implementations
│   │   │   ├── binance/     — live Binance WebSocket client
│   │   │   ├── mock/        — synthetic trades (COLLECTOR_MODE=mock)
│   │   │   └── shared/      — reconnect utilities (backoff)
│   │   ├── streams/         — Kafka producer stream
│   │   └── index.ts
│   ├── aggregator/          — tumbling window state, window events
│   │   └── index.ts
│   ├── detector/            — Z-score anomaly detection, Redis baseline
│   │   └── index.ts
│   ├── ai/                  — 30s batch buffer, OpenAI calls, insight events
│   │   └── index.ts
│   ├── storage/             — Kafka consumer for all topics, DB writes
│   │   └── index.ts
│   ├── api/                 — Fastify REST + SSE server
│   │   └── index.ts
│   ├── kafka/               — producer/consumer setup, topic definitions
│   │   └── index.ts
│   ├── schemas/             — TypeScript interfaces for all message types
│   │   └── index.ts
│   ├── logger/              — Pino configuration
│   │   └── index.ts
│   └── main.ts              — entry point, wires all modules
├── infra/
│   └── docker-compose.yml
├── tests/
│   ├── unit/
│   └── integration/
├── .env.example
├── package.json
├── tsconfig.json
├── ARCHITECTURE.md
└── README.md
```

---

# Local Development

## Prerequisites

* Node.js 20+
* Docker and Docker Compose

## Setup

```bash
# 1. Clone and install dependencies
git clone https://github.com/your-user/bitcoin.git
cd bitcoin
npm install

# 2. Start infrastructure
docker compose -f infra/docker-compose.yml up -d

# 3. Configure environment
cp .env.example .env
# Edit .env and set OPENAI_API_KEY

# 4. Run in mock mode (no live Binance connection required)
COLLECTOR_MODE=mock npm run dev

# 5. Run with live Binance feed
COLLECTOR_MODE=live npm run dev
```

## Infrastructure Services

| Service | URL | Purpose |
|---|---|---|
| Kafka UI | http://localhost:8080 | Inspect topics, consumer lag, messages |
| TimescaleDB | localhost:5432 | PostgreSQL + time-series extension |
| Redis | localhost:6379 | Aggregator window state + Detector baseline |
| API | http://localhost:3000 | REST endpoints + SSE (`/api/v1/events`) |
| Metrics | http://localhost:3000/metrics | Prometheus scrape |
| Health | http://localhost:3000/health | System health check |

## Environment Variables

```bash
KAFKA_BROKERS=localhost:9092
POSTGRES_URL=postgresql://bitcoin:bitcoin@localhost:5432/bitcoin
REDIS_URL=redis://localhost:6379
BINANCE_WS_URL=wss://stream.binance.com:9443              # read by providers/binance when live
SYMBOLS=BTCUSDT                      # comma-separated list of symbols to track (e.g. BTCUSDT,ETHUSDT)
COLLECTOR_MODE=live                  # live | mock
TRADE_PROVIDER=binance               # live mode only: binance (default)
MOCK_TRADE_INTERVAL_MS=1000          # read by providers/mock when COLLECTOR_MODE=mock
OPENAI_API_KEY=sk-...
DETECTOR_MIN_SAMPLES=30              # windows before Z-score activates
DETECTOR_ZSCORE_THRESHOLD=2.5        # standard deviations for anomaly
AI_BATCH_WINDOW_MS=30000             # ms to buffer anomalies before AI call
PORT=3000
```

---

# Technical Objectives

This project demonstrates hands-on knowledge of:

* **Node.js Streams** — backpressure, `Readable`/`Transform`/`Writable` pipeline
* **Event-Driven Architecture** — decoupled modules communicating via events
* **Apache Kafka** — topics, consumer groups, round-robin partitioning, offset management, DLQ
* **Streaming Data Processing** — tumbling windows, stateful aggregation
* **Time-Series Data** — TimescaleDB hypertables, `time_bucket()` queries
* **Backpressure** — `pause()`/`resume()` per-message consumer control
* **Statistical Anomaly Detection** — Z-score with rolling Redis baseline
* **Distributed Systems** — at-least-once delivery, idempotent writes
* **Observability** — structured logging, Prometheus metrics, health checks
* **AI Integration** — prompt engineering, batching, cost control, circuit breaker
* **Real-Time APIs** — Fastify REST + SSE (Server-Sent Events) with `Last-Event-ID` resume

---

# Future Enhancements

## Multi-Exchange Support

Add exchange adapters for Coinbase, Kraken, and Bybit. Each adapter connects to the exchange WebSocket and produces to the same `crypto.raw-trades` topic with the `exchange` field set in the payload. No changes are required to the Aggregator, Detector, Storage, or AI modules — they already operate per-symbol using the `symbol` field from each message.

## Observability Dashboard

Add Grafana to Docker Compose with pre-built dashboards for trades/sec, Kafka consumer lag, anomaly rate, and AI call metrics.

## Sentiment Analysis

Correlate market anomalies with news and social media events to enrich AI-generated insights with macroeconomic context.

## Advanced AI Analysis

Use RAG with historical anomaly patterns to generate richer, context-aware explanations, referencing similar past market events.

## ML Anomaly Detection

The `anomalies` table stores full window features (Z-scores, raw values, baseline stats) at detection time. This labeled dataset enables training a supervised ML model to replace or complement the rule-based Z-score detector.
