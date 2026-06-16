# Bitcoin Market Intelligence Platform

## Overview

Bitcoin Market Intelligence Platform is a real-time event processing system designed to ingest live Bitcoin trading activity, detect market anomalies, identify trend changes, and generate AI-powered market insights.

The platform continuously processes public Bitcoin market events and transforms raw trade data into actionable information for traders, analysts, and researchers.

Rather than attempting to predict the future price of Bitcoin directly, the platform focuses on identifying relevant market signals and providing contextual explanations that support trading decisions.

---

# Business Goal

Build a real-time Bitcoin Market Intelligence Platform capable of:

* Ingesting live Bitcoin trading events.
* Detecting abnormal market behavior.
* Identifying trend changes.
* Generating AI-powered market insights.
* Providing historical analysis of detected events.

The platform should operate continuously, processing events as they arrive and transforming raw market data into business intelligence.

---

# Functional Requirements

## Trade Ingestion

The system must consume real-time Bitcoin trade events from a public market data provider.

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
                       (btcusdt@trade stream)
                               │
                               ▼
                    ┌─────────────────────┐
                    │      Collector       │  Node.js Readable Stream
                    │  Exponential Backoff │  Dual heartbeat (30s)
                    │  Mock mode for tests │
                    └──────────┬──────────┘
                               │  partition key = symbol
                               ▼
                    ╔══════════════════════╗
                    ║   btc.raw-trades     ║  Kafka (3 partitions, 7d retention)
                    ╚═══════╤══════════════╝
                            │
            ┌───────────────┼───────────────┐
            │               │               │
            ▼               ▼               │
  ┌──────────────┐  ┌──────────────┐        │
  │  Aggregator  │  │   Storage    │        │
  │  In-memory   │  │  TimescaleDB │        │
  │  accumulators│  │  (trades)    │        │
  └──────┬───────┘  └──────────────┘        │
         │                                  │
         │  tumbling windows (1m, 5m, 15m)  │
         ▼                                  │
╔═══════════════════════╗                   │
║   btc.market-windows  ║  Kafka (1d)       │
╚══════════╤════════════╝                   │
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
╔══════════════╗                            │
║ btc.anomalies║  Kafka (30d)               │
╚══════╤═══════╝                            │
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
╔══════════════╗
║ btc.insights ║  Kafka (30d)
╚══════╤═══════╝
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
| Collector | Binance WebSocket | `btc.raw-trades` | None |
| Aggregator | `btc.raw-trades` | `btc.market-windows` | In-memory accumulators |
| Detector | `btc.market-windows` | `btc.anomalies` | Redis (60-window baseline) |
| AI Service | `btc.anomalies` | `btc.insights` | In-memory 30s batch buffer |
| Storage | All 4 topics | TimescaleDB | None |
| API | TimescaleDB + event bus | REST/SSE clients | None |

## Kafka Topics

| Topic | Producer | Consumers | Retention |
|---|---|---|---|
| `btc.raw-trades` | Collector | Aggregator, Storage | 7 days |
| `btc.market-windows` | Aggregator | Detector, Storage | 1 day |
| `btc.anomalies` | Detector | AI Service, Storage | 30 days |
| `btc.insights` | AI Service | Storage | 30 days |
| `btc.dlq` | Any (on failure) | Manual inspection | 7 days |

---

# Technology Stack

| Concern | Technology | Rationale |
|---|---|---|
| Runtime | Node.js (TypeScript) | Event-driven I/O, Streams API, learning goal |
| Message broker | Apache Kafka (KRaft) | Durable event log, consumer groups, replay |
| Database | PostgreSQL + TimescaleDB | SQL + native time-series, hypertables |
| Detector state | Redis | Atomic operations for rolling Z-score baseline |
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
│   ├── collector/           — WebSocket ingestion, Kafka producer
│   │   ├── binance/         — live Binance WebSocket client
│   │   ├── mock/            — mock/replay mode (COLLECTOR_MODE=mock)
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
| Redis | localhost:6379 | Detector baseline state |
| API | http://localhost:3000 | REST endpoints + SSE (`/api/v1/events`) |
| Metrics | http://localhost:3000/metrics | Prometheus scrape |
| Health | http://localhost:3000/health | System health check |

## Environment Variables

```bash
KAFKA_BROKERS=localhost:9092
POSTGRES_URL=postgresql://bitcoin:bitcoin@localhost:5432/bitcoin
REDIS_URL=redis://localhost:6379
BINANCE_WS_URL=wss://stream.binance.com:9443/ws/btcusdt@trade
COLLECTOR_MODE=live                  # live | mock
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
* **Apache Kafka** — topics, consumer groups, partition keys, offset management, DLQ
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

## Aggregator Horizontal Scaling

Replace in-memory window accumulators with Redis shared state (`HINCRBYFLOAT`). Multiple Aggregator instances can then process different partitions safely, enabling horizontal scaling without changing the detection or storage modules.

## Multi-Exchange Support

Add exchange adapters for Coinbase, Kraken, and Bybit. Each exchange maps to a partition key — the Kafka topology already supports this with 3 partitions in `btc.raw-trades`.

## Observability Dashboard

Add Grafana to Docker Compose with pre-built dashboards for trades/sec, Kafka consumer lag, anomaly rate, and AI call metrics.

## Sentiment Analysis

Correlate market anomalies with news and social media events to enrich AI-generated insights with macroeconomic context.

## Advanced AI Analysis

Use RAG with historical anomaly patterns to generate richer, context-aware explanations, referencing similar past market events.

## ML Anomaly Detection

The `anomalies` table stores full window features (Z-scores, raw values, baseline stats) at detection time. This labeled dataset enables training a supervised ML model to replace or complement the rule-based Z-score detector.

---

# Learning Goals

The primary goal of this project is to gain hands-on experience building a production-style streaming platform: real-time event ingestion with backpressure, stateful windowed aggregation, statistical anomaly detection, and AI-powered market analysis — all connected through an event-driven Kafka backbone.
