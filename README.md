# 📈 Crypto Watch Backend

[![TypeScript](https://img.shields.io/badge/TypeScript-007ACC?style=flat-square&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Node.js](https://img.shields.io/badge/Node.js-18+-43853D?style=flat-square&logo=node.js&logoColor=white)](https://nodejs.org/)
[![Express.js](https://img.shields.io/badge/Express.js-5.x-000000?style=flat-square&logo=express&logoColor=white)](https://expressjs.com/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-4.x-FF6600?style=flat-square&logo=rabbitmq&logoColor=white)](https://www.rabbitmq.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14+-316192?style=flat-square&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Drizzle ORM](https://img.shields.io/badge/Drizzle_ORM-0.45+-C5F74F?style=flat-square&logo=drizzle&logoColor=black)](https://orm.drizzle.team/)
[![Swagger](https://img.shields.io/badge/Swagger-OpenAPI_3.0-85EA2D?style=flat-square&logo=swagger&logoColor=black)](https://swagger.io/)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue.svg?style=flat-square)](https://opensource.org/licenses/ISC)

> High-throughput, event-driven cryptocurrency market data ingestion, real-time streaming, and time-series aggregation engine powered by Node.js, TypeScript, RabbitMQ, PostgreSQL, and Server-Sent Events (SSE).

---

## 🔗 Live Deployments & Demo

| Service | URL | Description |
| :--- | :--- | :--- |
| 🌐 **Frontend Application** | [Crypto Watch UI](https://url-shortner-frontend-nine-sigma.vercel.app/) | Interactive client dashboard consuming live trades and charting OHLC historical candles |
| 📖 **API Documentation** | [Swagger UI on Render](https://crypto-watch-backend.onrender.com/docs/) | Interactive OpenAPI 3.0 documentation and live endpoint testing playground |

> ⚠️ **Render Cold Start Notice**: The backend is hosted on Render's free tier. If the instance has spun down due to inactivity, the initial request may take **40–60 seconds** to wake up. Subsequent requests will respond with standard latency.

---

## 💡 Project Overview

**Crypto Watch Backend** is a production-ready, distributed streaming service engineered to process, store, and stream real-time cryptocurrency trade execution data from the **Binance WebSocket API**. 

Handling volatile financial market streams requires resilient backpressure management, zero-data-loss guarantees, and efficient time-series aggregation. This system achieves this through:
1. **Decoupled Architecture**: Ingestion is decoupled from persistence using **RabbitMQ**, insulating database writes from high-frequency Binance trade bursts.
2. **Resilient Message Processing**: Features in-memory buffer batching, RabbitMQ publisher confirmations, and a multi-stage **Dead-Letter Queue (DLQ)** retry topology with exponential backoff.
3. **Database-Driven Concurrency Guarantees**: PostgreSQL partial unique constraints ensure that strictly one ingestion session (manual or scheduled) can be active cluster-wide at any given time.
4. **On-the-Fly OHLC Aggregation**: Leverages PostgreSQL's `date_bin` and array aggregation to dynamically bucket raw tick data into 1-minute, 5-minute, 15-minute, or 1-hour candlesticks without expensive pre-aggregation background cron overhead.
5. **Real-Time Client Streaming**: Delivers live price updates and collection status via **Server-Sent Events (SSE)** with automated heartbeats.

---

## 🏗️ Architecture & Data Pipeline

```mermaid
flowchart TD
    subgraph External["External Data Sources"]
        BinanceWS["Binance WebSocket API<br/>(e.g., btcusdt@aggTrade, ethusdt@aggTrade)"]
    end

    subgraph Ingestion["Ingestion Layer"]
        BinanceSvc["BinanceService<br/>(Auto-reconnect, Ingestion Buffer)"]
        RabbitMQSvc["RabbitMQService<br/>(Batching Buffer, ConfirmChannel)"]
    end

    subgraph Messaging["Message Broker (RabbitMQ)"]
        Exchange["Exchange: crypto.events (Topic)"]
        TradeQueue["Queue: crypto.market.trade"]
        RetryQueues["Retry Queues: 5s / 30s / 120s<br/>(Dead-Letter Exchanges)"]
        DLQ["Dead-Letter Queue (DLQ):<br/>crypto.market.trade.dlq"]
    end

    subgraph WorkerLayer["Processing & Storage Layer"]
        Worker["MarketTradeWorker<br/>(Prefetch: 10, Batch ACK)"]
        DB[("PostgreSQL Database<br/>(Drizzle ORM)")]
        TradesTable["market_trades<br/>(Idempotent Upsert / Unique Event ID)"]
        SessionsTable["market_sessions<br/>(Partial Unique Index for Status)"]
    end

    subgraph Streaming["Real-Time & Delivery Layer"]
        SSESvc["SseService<br/>(1s Broadcast Throttle, Heartbeat)"]
        Scheduler["MarketSchedulerService<br/>(Operating Windows & Auto-Pause)"]
        MarketController["Express REST API & Swagger<br/>(/api/market/*)"]
    end

    subgraph Clients["Frontend Clients"]
        VercelUI["Frontend Web UI (Vercel)<br/>Real-Time Dashboard & Charts"]
        SwaggerClient["Swagger UI / API Consumers"]
    end

    %% Pipeline connections
    BinanceWS -->|"Aggregated Trades"| BinanceSvc
    BinanceSvc -->|"Buffered Trade Events"| RabbitMQSvc
    RabbitMQSvc -->|"Publish Batches (market.trade)"| Exchange
    Exchange -->|"Route"| TradeQueue
    TradeQueue -->|"Consume"| Worker
    Worker -->|"Failed attempts"| RetryQueues
    RetryQueues -->|"Exceeded max retries"| DLQ
    Worker -->|"Batch Insert (onConflictDoNothing)"| TradesTable
    Worker -->|"Broadcast Trade Events"| SSESvc

    Scheduler -->|"Schedule Session"| SessionsTable
    MarketController -->|"Session Start / Stop"| SessionsTable
    MarketController -->|"OHLC Query (date_bin)"| TradesTable

    SSESvc -->|"SSE Events (/market/stream)"| VercelUI
    MarketController -->|"Historical Candlesticks (/market/history)"| VercelUI
    MarketController -->|"REST / Docs"| SwaggerClient

    DB --- TradesTable
    DB --- SessionsTable
```

---

## ⚡ Key Architectural Features

### 1. Ingestion Buffer & RabbitMQ Batching
* Ingests high-frequency tick data from Binance without overwhelming the broker.
* Trades are collected into an in-memory batch buffer (`tradeBatchSize = 100`) and flushed immediately when full or every `tradeBatchFlushIntervalMs = 2000ms`.
* Uses RabbitMQ **Confirm Channels** (`waitForConfirms()`), ensuring every batch is safely acknowledged by the broker before proceeding.

### 2. Multi-Stage Retry & Dead-Letter Queue (DLQ)
* If database writes fail or worker exceptions occur, messages are not immediately discarded.
* Utilizes a multi-tier delayed retry queue mechanism:
  * **Retry 1**: 5-second delay (`crypto.market.trade.retry.1`)
  * **Retry 2**: 30-second delay (`crypto.market.trade.retry.2`)
  * **Retry 3**: 120-second delay (`crypto.market.trade.retry.3`)
* If retries are exhausted ($> 3$), messages are routed to the **Dead-Letter Queue** (`crypto.market.trade.dlq`) with full contextual error headers for observability and replayability.

### 3. Concurrency Protection & Session Management
* Supports dual operation modes:
  * **Manual Mode**: User-triggered on demand via `POST /api/market/start` with a predefined countdown duration (e.g., 60 seconds).
  * **Scheduled Mode**: Automatic background collection windows scheduled between active trading hours with configurable cycle intervals (`automaticCollectionMinutes` and `automaticPauseMinutes`).
* **PostgreSQL Partial Unique Index**:
  ```sql
  CREATE UNIQUE INDEX market_sessions_one_running_idx 
  ON market_sessions (status) 
  WHERE status = 'RUNNING';
  ```
  This guarantees database-level isolation: no two processes or instances can run a simultaneous collection session.
* **Crash Recovery**: On application boot, expired sessions are automatically closed (`APPLICATION_RESTART`), while ongoing valid sessions are seamlessly resumed.

### 4. Dynamic Candlestick (OHLC) Aggregation via PostgreSQL `date_bin`
* Instead of running memory-heavy Node.js loops or periodic aggregation jobs, candlestick bucketing is calculated directly inside PostgreSQL using `date_bin`:
  ```sql
  SELECT
      bucket AS time,
      (ARRAY_AGG(price ORDER BY event_time ASC, trade_id ASC))[1] AS open,
      MAX(price) AS high,
      MIN(price) AS low,
      (ARRAY_AGG(price ORDER BY event_time DESC, trade_id DESC))[1] AS close
  FROM (
      SELECT
          date_bin(${bucketInterval}::interval, event_time, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS bucket,
          price, event_time, trade_id
      FROM market_trades
      WHERE symbol = ${symbol}
  ) AS trades
  GROUP BY bucket
  ORDER BY bucket DESC
  LIMIT ${limit};
  ```
* Supports standard trading intervals: `1m`, `5m`, `15m`, and `1h`.

### 5. Server-Sent Events (SSE) Live Feed
* Real-time unidirectional feed to frontend web clients over `GET /api/market/stream`.
* Events dispatched:
  * `connected`: Connection handshake confirmation.
  * `prices`: Batched latest price tick data across all tracked symbols.
  * `collector-status`: Ingestion session status (`RUNNING` / `IDLE`), current mode, and countdown remaining seconds.
  * `heartbeat`: Sent every 25 seconds to keep proxies and cloud load balancers from terminating idle HTTP connections.

---

## 🛠️ Tech Stack

| Domain | Technologies |
| :--- | :--- |
| **Runtime & Language** | [Node.js (18+)](https://nodejs.org/), [TypeScript](https://www.typescriptlang.org/), [TSX](https://github.com/privatenumber/tsx) |
| **Framework & Web** | [Express 5](https://expressjs.com/), [CORS](https://github.com/expressjs/cors) |
| **Message Broker** | [RabbitMQ 4 (Management)](https://www.rabbitmq.com/), [amqplib](https://github.com/amqp-node/amqplib) |
| **Database & ORM** | [PostgreSQL](https://www.postgresql.org/), [Drizzle ORM](https://orm.drizzle.team/), `pg` (node-postgres) |
| **WebSockets** | [ws (Node.js WebSocket library)](https://github.com/websockets/ws) |
| **Time & Scheduling** | [Luxon](https://moment.github.io/luxon/) |
| **API Docs** | [Swagger UI Express](https://github.com/scottie1984/swagger-ui-express), [swagger-jsdoc](https://github.com/Surnet/swagger-jsdoc) |
| **Containerization** | [Docker](https://www.docker.com/), [Docker Compose](https://docs.docker.com/compose/) |

---

## 📡 API Reference

Interactive OpenAPI documentation is available locally at `http://localhost:4000/docs` or on Render at `https://crypto-watch-backend.onrender.com/docs/`.

### 1. Historical Candlestick Data
Fetch aggregated OHLC candlesticks for charting.

* **Endpoint**: `GET /api/market/history`
* **Query Parameters**:
  * `symbol` *(required, string)*: Trading pair symbol (e.g. `BTCUSDT`, `ETHUSDT`).
  * `interval` *(optional, string)*: Bucket duration. Supported: `1m`, `5m`, `15m`, `1h` (default: `1m`).
  * `limit` *(optional, number)*: Number of candles between 1 and 1000 (default: `500`).

```bash
curl -X GET "https://crypto-watch-backend.onrender.com/api/market/history?symbol=BTCUSDT&interval=1m&limit=100"
```

**Response (200 OK)**:
```json
{
  "symbol": "BTCUSDT",
  "interval": "1m",
  "candles": [
    {
      "time": 1700000000,
      "open": 91230.50,
      "high": 91350.00,
      "low": 91200.10,
      "close": 91310.25
    }
  ]
}
```

---

### 2. Real-Time Market Stream (SSE)
Establish a persistent Server-Sent Events stream for live price updates and collector state.

* **Endpoint**: `GET /api/market/stream`
* **Headers**: `Accept: text/event-stream`

```bash
curl -N -H "Accept: text/event-stream" "https://crypto-watch-backend.onrender.com/api/market/stream"
```

**SSE Event Streams**:
```text
event: connected
data: {"message":"Connected to live market stream","connectedAt":"2026-11-17T13:00:00.000Z"}

event: collector-status
data: {"collectorStatus":"RUNNING","sessionMode":"MANUAL","sessionId":"f47ac10b-...","remainingSeconds":45}

event: prices
data: {"prices":[{"symbol":"BTCUSDT","price":"91310.25","quantity":"0.042","eventTime":"...","tradeId":123456789}],"timestamp":"..."}

event: heartbeat
data: {"timestamp":"2026-11-17T13:00:25.000Z"}
```

---

### 3. Start Collection Session
Manually trigger Binance data collection. Automatically stops when duration expires.

* **Endpoint**: `POST /api/market/start`
* **Response (201 Created)**:
```json
{
  "message": "Market collection started",
  "session": {
    "id": "e674a7d6-3e4b-4f4d-801a-8c59f2130325",
    "mode": "MANUAL",
    "status": "RUNNING",
    "startedAt": "2026-11-17T13:00:00.000Z",
    "scheduledStopAt": "2026-11-17T13:01:00.000Z"
  }
}
```
* **Conflict (409 Conflict)**:
```json
{
  "message": "A collection session is already running (MANUAL)"
}
```

---

### 4. Stop Collection Session
Manually stop an active collection session before its expiration.

* **Endpoint**: `POST /api/market/stop`
* **Response (200 OK)**:
```json
{
  "message": "Market collection stopped",
  "session": {
    "id": "e674a7d6-3e4b-4f4d-801a-8c59f2130325",
    "mode": "MANUAL",
    "status": "STOPPED",
    "stopReason": "MANUAL_STOP"
  }
}
```

---

## 🗄️ Database Schema

The database is managed with **Drizzle ORM** with migrations located in `src/db/migrations/`.

### `market_trades`
Stores granular trade tick data captured from Binance:
* `id` (`bigserial`, Primary Key)
* `event_id` (`text`, Unique) - Unique composite identifier (`binance-{symbol}-{tradeId}`) preventing duplicate inserts.
* `symbol` (`text`) - Crypto asset pair (e.g. `btcusdt`).
* `price` (`numeric(30, 10)`) - Trade execution price with financial precision.
* `quantity` (`numeric(30, 10)`) - Executed trade quantity.
* `trade_id` (`bigint`) - Binance internal aggregate trade ID.
* `event_time` (`timestamptz`) - Execution timestamp on Binance exchange.
* `created_at` (`timestamptz`) - Ingestion timestamp.

### `market_sessions`
Tracks manual and scheduled collection cycles:
* `id` (`uuid`, Primary Key)
* `mode` (`varchar(20)`) - `MANUAL` or `SCHEDULED`.
* `status` (`statusEnum`) - `RUNNING`, `STOPPED`, `SKIPPED`, `FAILED`.
* `started_at` (`timestamptz`)
* `scheduled_stop_at` (`timestamptz`)
* `scheduled_for` (`timestamptz`) - Target schedule window for cron slots.
* `stopped_at` (`timestamptz`)
* `stop_reason` (`varchar(100)`) - `DURATION_COMPLETED`, `MANUAL_STOP`, `MANUAL_CONFLICT`, `APPLICATION_RESTART`, etc.
* **Constraints**: Partial unique index on `status` (`WHERE status = 'RUNNING'`) guarantees singleton active sessions.

---

## 🚀 Getting Started

### Prerequisites
* **Node.js**: v18.0.0 or higher
* **npm**: v9.0.0 or higher
* **Docker & Docker Compose**: For local RabbitMQ instance
* **PostgreSQL**: v14 or higher (local or managed, e.g. Supabase, Neon, AWS RDS)

---

### Installation & Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/rrajkumar18/crypto-watch-backend.git
   cd crypto-watch-backend
   ```

2. **Install dependencies**:
   ```bash
   npm install
   ```

3. **Start RabbitMQ via Docker Compose**:
   ```bash
   docker compose up -d
   ```
   * RabbitMQ AMQP port: `localhost:5672`
   * RabbitMQ Management UI: [http://localhost:15672](http://localhost:15672) (User: `guest`, Password: `guest`)

4. **Configure Environment Variables**:
   Create a `.env` file in the root directory:
   ```env
   PORT=4000
   NODE_ENV=development

   # Binance WebSocket Endpoint & Symbols
   BINANCE_WS_URL=wss://stream.binance.com:9443
   BINANCE_SYMBOLS=btcusdt,ethusdt,solusdt

   # Message Broker (RabbitMQ)
   RABBITMQ_URL=amqp://guest:guest@localhost:5672

   # PostgreSQL Connection String
   PG_URL=postgresql://user:password@localhost:5432/crypto_watch?sslmode=disable
   ```

5. **Run Database Migrations**:
   ```bash
   npm run db:generate
   npm run db:migrate
   ```

6. **Start the Development Server**:
   ```bash
   npm run dev
   ```
   The server will start on `http://localhost:4000`. You can explore the API via Swagger at `http://localhost:4000/docs`.

---

## ⚙️ Configuration Reference

Key timing, scheduling, and batch parameters can be tuned in `src/config/env.ts`:

| Variable / Setting | Default | Description |
| :--- | :--- | :--- |
| `BINANCE_WS_URL` | `wss://stream.binance.com:9443` | Binance WebSocket streaming gateway |
| `BINANCE_SYMBOLS` | `btcusdt,ethusdt` | Comma-delimited list of cryptocurrency pairs to stream |
| `RABBITMQ_URL` | — | AMQP connection string |
| `PG_URL` | — | PostgreSQL connection string |
| `tradeBatchSize` | `100` | Max trades buffered in memory before triggering a batch publish |
| `tradeBatchFlushIntervalMs` | `2000` | Max milliseconds to wait before flushing incomplete trade batches |
| `manualCollectionSeconds` | `60` | Runtime duration (in seconds) for manually triggered collection |
| `automaticCollectionMinutes` | `2` | Duration of each scheduled collection window |
| `automaticPauseMinutes` | `45` | Pause interval between consecutive scheduled collection windows |
| `jobStartHour` / `jobEndHour` | `9` / `23` | Operating hours window (in configured timezone) |
| `timezone` | `Asia/Kolkata` | Timezone context for market collection schedule |

---

## 💻 Available Scripts

* `npm run dev`: Starts the application with `tsx watch` for hot-reload development.
* `npm run build`: Compiles TypeScript files into `dist/`.
* `npm run start`: Executes the compiled code from `dist/index.js`.
* `npm run db:generate`: Generates SQL migration files using Drizzle Kit.
* `npm run db:migrate`: Executes pending migrations on the target PostgreSQL database.

---

## 🛡️ Graceful Shutdown & Reliability

The application handles `SIGINT` and `SIGTERM` signals cleanly to ensure data consistency during deployments or restarts:
1. Pauses the market scheduler.
2. Closes Binance WebSocket connection.
3. Flushes remaining in-memory trade buffer to RabbitMQ.
4. Closes the Express HTTP server and active SSE client connections.
5. Allows the worker to acknowledge and commit active processing batches.
6. Closes RabbitMQ channels and database connection pools.

---

## 👤 Author

**Raj kumar Patel**  
* GitHub: [@rrajkumar18](https://github.com/rrajkumar18)

---

## 📄 License

This project is licensed under the [ISC License](LICENSE).
