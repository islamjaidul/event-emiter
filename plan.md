# Webhook Delivery Library — Implementation Plan

## 1. Project Overview

Build a reusable TypeScript library (`webhooks-ts`) that any Node.js backend application can import to reliably deliver webhook messages to registered subscriber URLs. Reliability is provided by **RabbitMQ** as the durable broker between `emit()` and HTTP delivery. The entire system — producer app, RabbitMQ, and two demo consumer apps — runs in **Docker** via `docker-compose`.

The library handles the producer side (register, emit, persistent publish, retry-aware delivery worker). Consumers are arbitrary HTTP services receiving JSON payloads. This plan ships with two demo consumer apps to demonstrate fan-out.

---

## 2. Requirements Analysis

### Functional Requirements

| Requirement | Detail |
|---|---|
| Simple API | `createWebhooks(config)`, `register(event, url)`, `emit(event, payload)`, `close()` |
| Reliable delivery across restarts | RabbitMQ durable queues + persistent messages + publisher confirms |
| Non-blocking `emit` | Caller waits only for the AMQP publish-confirm; HTTP delivery is async |
| Failing/slow subscribers don't block caller | Worker consumes from RabbitMQ independently of `emit` call path |
| Two working consumer apps | Independent Express services on different ports, registered for different events |
| Working producer demo | Host app that registers subscribers and emits events on an interval |
| Everything in Docker | `docker-compose up` brings up rabbitmq + producer + consumer-1 + consumer-2 |
| README | How to run, delivery guarantees, one tradeoff |

### Non-Functional Requirements (NFRs)

| # | NFR | Constraint | Design Response |
|---|---|---|---|
| 1 | **Reusable library** | Installable via `npm install` | Single npm package, clean public API in `index.ts`, no app-specific assumptions |
| 2 | **Producer/Consumer model via RabbitMQ** | Library is producer; consumers are external HTTP services; RabbitMQ decouples them | Library publishes to a topic exchange; in-library worker consumes and POSTs to consumer URLs |
| 3 | **Non-blocking library load** | `import` and `createWebhooks(config)` must not block the event loop | RabbitMQ connection deferred via `setImmediate`; ops before READY are buffered |
| 4 | **Singleton-friendly** | One instance per service; `emit` callable from anywhere | Standard Node module-cache singleton pattern; one shared RabbitMQ connection per instance |
| 5 | **Multiple events per registration** | One URL can subscribe to N events | Each `(event, url)` pair = one row in subscription store; fan-out at `emit` time |
| 6 | **Graceful shutdown** | `close()` on SIGTERM/SIGINT must complete cleanly | Stop worker → wait for in-flight HTTP → close channel → close connection → close DB. Idempotent. |
| 7 | **No memory leaks** | Long-running process must not grow unbounded | Bounded startup buffer; `AbortController` on every HTTP call; explicit channel/connection cleanup; reconnect logic uses single timer with `unref()` |
| 8 | **Retry on downstream failure** | If consumer is down, jobs retried with backoff — never silently dropped | Failed deliveries re-published to delayed exchange with `x-delay` header (exponential backoff); after `maxRetries`, routed to dead-letter queue |
| 9 | **No data loss (HARD RULE)** | Zero tolerance — even on broker restart or producer `kill -9` | Publisher confirms (`waitForConfirms`) before `emit` returns; durable queues; persistent messages (`deliveryMode: 2`); manual consumer ack only after HTTP 2xx |
| 10 | **TypeScript convention compliance** | Idiomatic strictly-typed code | `strict: true`, no `any`, `interface` for contracts, named exports only. See §13 |
| 11 | **Containerized deployment** | All processes in Docker | `Dockerfile` for producer + consumers, `docker-compose.yml` orchestrates rabbitmq + 3 services + named volumes |

---

## 3. Architecture

### System view (everything in Docker)

```
┌──────────────────────── docker-compose network ────────────────────────┐
│                                                                        │
│   ┌──────────────────────────┐         ┌────────────────────────────┐  │
│   │   producer (container)   │         │     rabbitmq (container)   │  │
│   │   host app + webhooks-ts │         │     w/ delayed-msg plugin  │  │
│   │                          │         │                            │  │
│   │   ┌────────────────────┐ │ publish │  exchange: webhooks.x      │  │
│   │   │ Public API         │ ├─────────►   (topic, durable)         │  │
│   │   │ createWebhooks()   │ │ confirms│                            │  │
│   │   │ register / emit    │ │◄────────┤  exchange: webhooks.retry  │  │
│   │   │ close              │ │         │   (x-delayed-message)      │  │
│   │   └────────────────────┘ │         │                            │  │
│   │                          │         │  queue:    webhooks.q      │  │
│   │   ┌────────────────────┐ │ consume │   (durable, manual ack)    │  │
│   │   │ In-library Worker  │ ◄─────────┤                            │  │
│   │   │ AMQP consumer      │ │         │  queue:    webhooks.dlq    │  │
│   │   │  → HTTP POST       │ │         │   (dead-letter, retained)  │  │
│   │   └────────┬───────────┘ │         │                            │  │
│   │            │             │ ack/nack│  ports: 5672 (amqp)        │  │
│   │            │             ├─────────►        15672 (mgmt UI)     │  │
│   │   ┌────────▼───────────┐ │         │                            │  │
│   │   │ subscription store │ │         │  volume: rabbitmq-data     │  │
│   │   │ SQLite (volume)    │ │         └────────────────────────────┘  │
│   │   └────────────────────┘ │                                         │
│   │                          │                                         │
│   │   volume: producer-data  │                                         │
│   └────────────┬─────────────┘                                         │
│                │                                                       │
│                │ HTTPS POST to subscriber URLs                         │
│                │                                                       │
│       ┌────────┴────────────────┐                                      │
│       │                         │                                      │
│   ┌───▼───────────────┐    ┌────▼──────────────┐                       │
│   │ consumer-1        │    │ consumer-2        │                       │
│   │ (container)       │    │ (container)       │                       │
│   │ Express on :3001  │    │ Express on :3002  │                       │
│   │ POST /hook → 200  │    │ POST /hook → 200  │                       │
│   └───────────────────┘    └───────────────────┘                       │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

### Data flow

```
1. host app calls webhooks.emit('order.created', { orderId: 123 })
2. library reads subscriptions for 'order.created' from SQLite
3. for each (event, url) pair, library publishes a persistent message to `webhooks.x`
   with routing key 'order.created' and headers { url, attempts: 0, maxRetries }
4. library awaits publisher confirm — only then does emit() resolve to caller
5. RabbitMQ routes the message to `webhooks.q`
6. in-library Worker consumes the message (manual ack mode)
7. Worker HTTP POSTs the payload to the URL from the message headers
8a. on 2xx → ack
8b. on failure & attempts < maxRetries → republish to `webhooks.retry` (delayed
    exchange) with x-delay = backoff(attempts), attempts++; then ack original
8c. on failure & attempts >= maxRetries → publish to `webhooks.dlq`; ack original
```

### Why this split

- **RabbitMQ owns durability** for in-flight delivery: persistent messages, durable queues, publisher confirms, manual ack — all out-of-the-box.
- **SQLite owns subscription state** because RabbitMQ topology (bindings) is awkward for retrieving URL metadata at emit time. Subscriptions change rarely; the SQLite write is fast and durable (WAL + `synchronous=FULL`).
- **Worker stays in-library** for the 2h scope. Documented as a tradeoff — a separate worker container would give independent scaling.

### Non-blocking init design

```
createWebhooks(config) call
  │
  ├─→ return instance reference immediately (no I/O)
  │
  └─→ setImmediate(() => {
          open SQLite (async)
          run schema migrations
          connect to RabbitMQ (async, with retry on ECONNREFUSED)
          assert exchanges (webhooks.x, webhooks.retry, webhooks.dlx)
          assert queues   (webhooks.q, webhooks.dlq)
          bind queues to exchanges
          start consumer (manual ack)
          flush startup buffer
          set state = READY
      })

register/emit calls before READY → push to bounded buffer (max 1000)
```

---

## 4. Data Model & Broker Topology

### SQLite — subscription store only

Table: `subscriptions`
| Column | Type | Notes |
|---|---|---|
| id | TEXT PK | UUID v4 |
| event | TEXT NOT NULL | e.g. `order.created` |
| url | TEXT NOT NULL | subscriber endpoint |
| created_at | INTEGER | Unix ms |

Index: `(event)` for fast emit-time lookup. Unique on `(event, url)` to prevent duplicate registrations.

PRAGMAs: `journal_mode=WAL`, `synchronous=FULL`, `busy_timeout=5000`.

### RabbitMQ topology

| Object | Type | Settings | Purpose |
|---|---|---|---|
| `webhooks.x` | exchange (topic, durable) | — | Primary publish target from `emit()` |
| `webhooks.retry` | exchange (`x-delayed-message`, type=topic, durable) | — | Holds messages awaiting backoff delay |
| `webhooks.dlx` | exchange (topic, durable) | — | Dead-letter routing |
| `webhooks.q` | queue (durable) | `x-dead-letter-exchange=webhooks.dlx`, manual ack | Worker consumes from here |
| `webhooks.dlq` | queue (durable) | retained indefinitely | Failed jobs after `maxRetries` |

Bindings:
- `webhooks.x` → `webhooks.q` (routing key `#` — all events)
- `webhooks.retry` → `webhooks.q` (routing key `#` — re-enters main queue after delay)
- `webhooks.dlx` → `webhooks.dlq` (routing key `#`)

Message properties:
- `persistent: true` (deliveryMode=2)
- `headers: { url, attempts, maxRetries, originalEvent }`
- `messageId`: UUID for idempotency tracking
- `contentType: 'application/json'`

---

## 5. Delivery Guarantee

**At-least-once delivery, no data loss.**

```
emit(event, payload):
  1. Look up subscribers from SQLite                           [sync read]
  2. For each subscriber, channel.publish(webhooks.x,
       routingKey=event, JSON.stringify(payload),
       { persistent: true, headers: { url, attempts: 0 } })   [async write]
  3. await channel.waitForConfirms()                           [broker ack]
  4. Return to caller                                          [emit resolves]

Worker (consumer of webhooks.q, manual ack):
  1. Receive message
  2. POST headers.url with payload, AbortController(timeoutMs)
  3a. 2xx response → channel.ack(msg)
  3b. error / non-2xx:
        if attempts + 1 >= maxRetries:
          channel.publish(webhooks.dlx, key, body, { headers })
          channel.ack(msg)
        else:
          channel.publish(webhooks.retry, key, body, {
            persistent: true,
            headers: { ...headers, attempts: attempts+1, 'x-delay': backoff(attempts+1) }
          })
          channel.ack(msg)
```

**Retry backoff** (delayed-message-exchange plugin reads `x-delay` header):
| Attempt | Delay before retry |
|---|---|
| 1st failure | 10 s |
| 2nd failure | 1 min |
| 3rd failure | 5 min |
| 4th failure | 30 min |
| 5th failure | 2 h |
| 6th+ | capped at 6 h, then dead-lettered |

**Crash recovery:** RabbitMQ holds the message until acked. If the worker crashes mid-delivery (after POST, before ack), the broker re-delivers on reconnect. This produces at-least-once semantics — consumers must be idempotent.

**Connection loss:** AMQP client has reconnect-with-backoff logic. While disconnected, `emit()` writes are buffered (up to `startupBufferLimit`) and flushed on reconnect.

---

## 6. Singleton Pattern & Multi-Event Usage

```typescript
// service initialization (e.g., src/main.ts)
import { createWebhooks } from 'webhooks-ts';

export const webhooks = createWebhooks({
  rabbitmqUrl: process.env.RABBITMQ_URL!,
  dbPath: '/data/webhooks.db',
});

process.on('SIGTERM', async () => {
  await webhooks.close();
  process.exit(0);
});

// -------------------------------------------
// order.service.ts — emit from anywhere
import { webhooks } from './main';

await webhooks.register('order.submitted', 'http://consumer-1:3001/hook');
await webhooks.register('order.delivered', 'http://consumer-1:3001/hook');
await webhooks.register('order.submitted', 'http://consumer-2:3002/hook');

await webhooks.emit('order.submitted', { orderId: 123 });
await webhooks.emit('order.delivered', { orderId: 123, deliveredAt: Date.now() });
```

One AMQP connection per `createWebhooks()` instance, shared across all `emit()` calls. Channel(s) opened on top of the connection — separate channels for publish vs consume to avoid blocking.

---

## 7. Public API Design

```typescript
interface WebhooksConfig {
  readonly rabbitmqUrl: string;          // e.g. amqp://guest:guest@rabbitmq:5672
  readonly dbPath?: string;              // SQLite file (default: ./webhooks.db)
  readonly maxRetries?: number;          // before dead-letter (default: 5)
  readonly timeoutMs?: number;           // per-request HTTP timeout (default: 10000)
  readonly concurrency?: number;         // prefetch count for AMQP consumer (default: 10)
  readonly startupBufferLimit?: number;  // ops buffered before READY (default: 1000)
  readonly reconnectBackoffMs?: number;  // initial AMQP reconnect delay (default: 1000)
}

interface WebhooksInstance {
  register(event: string, url: string): Promise<void>;
  emit(event: string, payload: unknown): Promise<void>;
  close(): Promise<void>;
}

declare function createWebhooks(config: WebhooksConfig): WebhooksInstance;
```

`pollIntervalMs` from the previous plan is **gone** — no polling; we use AMQP push-based consume.

---

## 8. Memory Leak Prevention

| Risk | Mitigation |
|---|---|
| AMQP connection / channel leaked | `close()` calls `channel.close()` then `connection.close()` |
| Reconnect timer leaked | Single `setTimeout` (not `setInterval`) tracked in a ref; cleared in `close()`; `unref()` so it never blocks exit |
| Startup buffer growing unbounded | Cap at `startupBufferLimit`; throw if exceeded |
| HTTP connections not released | `AbortController` with `timeoutMs` on every request |
| Consumer prefetch unbounded | `channel.prefetch(concurrency)` limits in-flight |
| Unacked messages on shutdown | Worker stops consuming → waits for in-flight tasks → closes channel (RabbitMQ requeues unacked) |
| SQLite handle left open | `db.close()` in `close()` after worker stops |
| Uncaught rejections in consumer callback | Wrap callback in try/catch; on error, nack with requeue=false (DLX route) |

---

## 9. File Structure

```
event-emiter/
├── src/
│   ├── index.ts                          # Public API — createWebhooks(), re-exports types
│   ├── types.ts                          # WebhooksConfig, WebhooksInstance, JobHeaders, error classes
│   ├── db.ts                             # async SQLite open + PRAGMA + migrations
│   ├── repositories/
│   │   ├── subscription.repository.ts   # ISubscriptionRepository + SubscriptionRepository
│   │   └── index.ts                     # re-exports interfaces only
│   ├── services/
│   │   ├── webhook.service.ts           # IWebhookService + WebhookService
│   │   └── index.ts
│   ├── broker.ts                         # AMQP connect, assert topology, reconnect logic
│   ├── publisher.ts                      # publish-with-confirm helper + IPublisher interface
│   ├── worker.ts                         # AMQP consumer → HTTP → ack/retry/dead-letter
│   ├── delivery.ts                       # HTTP POST with AbortController timeout
│   └── backoff.ts                        # exponential backoff calculation (pure function)
├── examples/
│   ├── producer/
│   │   ├── index.ts                      # Singleton setup, registers events, emits on interval
│   │   └── Dockerfile
│   ├── consumer-1/
│   │   ├── controllers/
│   │   │   └── webhook.controller.ts    # WebhookController — parse req, call service, send res
│   │   ├── services/
│   │   │   └── inbound-webhook.service.ts  # InboundWebhookService — log + process
│   │   ├── app.ts                        # wires controller, starts Express on :3001
│   │   └── Dockerfile
│   └── consumer-2/
│       ├── controllers/
│       │   └── webhook.controller.ts
│       ├── services/
│       │   └── inbound-webhook.service.ts
│       ├── app.ts                        # Express on :3002
│       └── Dockerfile
├── tests/
│   ├── unit/
│   │   ├── repositories/
│   │   │   └── subscription.repository.test.ts   # real in-memory SQLite (:memory:)
│   │   ├── services/
│   │   │   └── webhook.service.test.ts            # mocked repo + publisher
│   │   ├── controllers/
│   │   │   └── webhook.controller.test.ts         # supertest + mocked service
│   │   ├── backoff.test.ts                        # pure function, no mocks
│   │   ├── delivery.test.ts                       # mocked fetch
│   │   └── worker.test.ts                         # mocked amqplib channel + delivery
│   └── e2e/
│       ├── helpers/
│       │   ├── test-broker.ts                     # Testcontainers RabbitMQ setup/teardown
│       │   └── test-consumer.ts                   # minimal http.Server for assertions
│       ├── webhook-delivery.e2e.test.ts            # register + emit → both consumers receive
│       ├── retry-behavior.e2e.test.ts              # 500 × N → retry → eventual 200
│       ├── dead-letter.e2e.test.ts                 # exhaust maxRetries → message in DLQ
│       └── graceful-shutdown.e2e.test.ts           # emit → close() → delivery still completes
├── jest.config.ts                        # two projects: unit + e2e
├── docker-compose.yml
├── README.md
├── plan.md
├── requirement.md
├── package.json
├── tsconfig.json
└── .gitignore
```

---

## 10. Implementation Phases (~2h total)

### Phase 1 — Scaffold (10 min)
- [ ] `npm init -y`, `tsconfig.json` with strict mode (see §13)
- [ ] Runtime deps: `amqplib`, `sqlite3`, `uuid`, `express`
- [ ] Dev deps: `typescript`, `tsx`, `@types/*`, `jest`, `ts-jest`, `@types/jest`, `supertest`, `@types/supertest`, `@testcontainers/rabbitmq`, `better-sqlite3`, `@types/better-sqlite3`
- [ ] `package.json` scripts: `build`, `start:producer`, `start:consumer-1`, `start:consumer-2`, `test`, `test:unit`, `test:e2e`, `test:watch`, `test:coverage`
- [ ] `jest.config.ts`: two projects — `unit` (`tests/unit/**/*.test.ts`) and `e2e` (`tests/e2e/**/*.e2e.test.ts`)

### Phase 2 — Subscription Store (10 min)
- [ ] `src/db.ts`: async openDb + WAL/FULL pragmas
- [ ] `src/registry.ts`: addSubscription (UNIQUE on event+url, INSERT OR IGNORE), getSubscribers

### Phase 3 — Broker Module (15 min)
- [ ] `src/broker.ts`: connect with retry-backoff loop, assert topology (3 exchanges + 2 queues + bindings)
- [ ] Two channels: `publishChannel` (with confirms enabled) + `consumeChannel` (manual ack, prefetch)
- [ ] Reconnect on connection error (single tracked timeout, unref'd)

### Phase 4 — Publisher (10 min)
- [ ] `src/publisher.ts`: `publishJob(channel, event, payload, headers)` — `publish` + `waitForConfirms`
- [ ] On confirm-nack → throw, propagated to `emit()` caller

### Phase 5 — Worker (15 min)
- [ ] `src/worker.ts`: `consumeChannel.consume('webhooks.q', handler, { noAck: false })`
- [ ] Handler: parse headers → `deliverHttp` → ack/retry/dead-letter logic per §5
- [ ] `stop()`: cancel consumer tag, wait for in-flight handlers

### Phase 6 — Delivery + Backoff (10 min)
- [ ] `src/delivery.ts`: fetch POST + AbortController + return {success, statusCode}
- [ ] `src/backoff.ts`: `nextDelayMs(attempts)` matching schedule in §5

### Phase 7 — Public API (10 min)
- [ ] `src/index.ts`: `createWebhooks(config)`
- [ ] Init Manager: setImmediate-deferred init; bounded startup buffer
- [ ] `register`/`emit` gated on READY; `close` orderly shutdown

### Phase 8 — Two Consumer Apps (10 min)
- [ ] `examples/consumer-1/index.ts`: Express on :3001, POST `/hook` logs `[consumer-1]` + body
- [ ] `examples/consumer-2/index.ts`: Express on :3002, POST `/hook` logs `[consumer-2]` + body
- [ ] Both return 200 on success; flag for simulated 500 via `?fail=true` for testing retries

### Phase 9 — Producer Demo App (10 min)
- [ ] `examples/producer/index.ts`:
  - createWebhooks pointed at `amqp://rabbitmq:5672`
  - register `order.created` → both consumers
  - register `order.shipped` → consumer-1 only
  - emit both events every 5s with rotating payload
  - SIGTERM handler → close()

### Phase 10 — Docker (15 min)
- [ ] `examples/producer/Dockerfile`, `examples/consumer-*/Dockerfile` (multi-stage: tsc build → node:20-alpine runtime)
- [ ] `docker-compose.yml`:
  - `rabbitmq`: `rabbitmq:3-management` (or custom image with delayed-message plugin); ports 5672 + 15672; healthcheck; volume
  - `producer`: depends_on rabbitmq (with healthcheck condition); env `RABBITMQ_URL`; volume for SQLite
  - `consumer-1`, `consumer-2`: independent, expose 3001 / 3002

### Phase 11 — Tests (25 min)

**Unit tests** (`tests/unit/`) — written alongside each source phase, run immediately:
- [ ] `backoff.test.ts` — assert delay schedule per attempt + 6h cap (write in Phase 6)
- [ ] `delivery.test.ts` — mock `fetch`; assert 2xx→success, non-2xx→failure, timeout→failure (Phase 6)
- [ ] `subscription.repository.test.ts` — real `:memory:` SQLite; add, idempotent add, findByEvent, empty result (Phase 2)
- [ ] `webhook.service.test.ts` — mock repo + publisher; register validation, emit fan-out, empty skip (Phase 7)
- [ ] `webhook.controller.test.ts` — supertest + mock service; 200, 400, 422, 500 paths (Phase 8)
- [ ] `worker.test.ts` — mock amqplib channel + delivery; ack, retry publish, dead-letter, uncaught→nack (Phase 5)

**E2e tests** (`tests/e2e/`) — written after Docker phase, run with `npm run test:e2e`:
- [ ] `webhook-delivery.e2e.test.ts` — full fan-out + selective routing (Phase 11)
- [ ] `retry-behavior.e2e.test.ts` — consumer returns 500 twice then 200; verify 3 total calls (Phase 11)
- [ ] `dead-letter.e2e.test.ts` — consumer always 500; verify message in `webhooks.dlq` after maxRetries (Phase 11)
- [ ] `graceful-shutdown.e2e.test.ts` — emit then `close()`; verify consumer still receives (Phase 11)

Run full suite after each phase: `npm test`. Fix any failure before moving to the next phase.

### Phase 12 — README + Cleanup (10 min)
- [ ] `README.md`: prereqs, `docker-compose up`, what each container does, RabbitMQ UI link, how to inspect dead-letter queue, delivery guarantees, one tradeoff (in-library worker → separate worker container)
- [ ] `.gitignore`: node_modules, dist, *.db, *.db-wal, *.db-shm, .env

---

## 11. Dependencies

### Runtime
| Package | Purpose |
|---|---|
| `amqplib` | RabbitMQ client (AMQP 0-9-1) |
| `sqlite3` | Async SQLite for subscription store (production) |
| `uuid` | Subscription IDs + AMQP messageId |
| `express` | Demo consumer servers only |

### Dev
| Package | Purpose |
|---|---|
| `typescript`, `tsx` | Compiler + dev runner |
| `@types/node`, `@types/amqplib`, `@types/sqlite3`, `@types/uuid`, `@types/express` | Type definitions |
| `jest`, `ts-jest` | Test runner + TypeScript transform |
| `@types/jest` | Jest type definitions |
| `supertest`, `@types/supertest` | HTTP assertions for controller unit tests |
| `better-sqlite3`, `@types/better-sqlite3` | Synchronous SQLite for `:memory:` unit tests (simpler than async sqlite3 in test context) |
| `@testcontainers/rabbitmq` | Spin up real RabbitMQ in Docker for e2e tests |

### Infrastructure (Docker)
| Image | Purpose |
|---|---|
| `rabbitmq:3-management` | Broker + management UI on :15672. **Note:** the `x-delayed-message` exchange type requires the `rabbitmq_delayed_message_exchange` plugin. Either use a custom image (`Dockerfile.rabbitmq` with `rabbitmq-plugins enable rabbitmq_delayed_message_exchange`) or fall back to TTL-queue chain (see Tradeoffs). |
| `node:20-alpine` | Base image for producer + consumers |

---

## 12. Key Tradeoffs

**Primary (document in README):**
> **In-library worker vs separate worker container.** The delivery worker currently runs in the same process as the producer host app. With more time, it would be split into its own container so it can scale independently of the producer (e.g., 5 workers behind one busy producer) and so a slow webhook batch never competes for CPU with the producer's request handling. The library API would not change — only the deployment topology and the entry point.

**Other internal design notes:**
| Tradeoff | Choice | Alternative |
|---|---|---|
| Subscription store | SQLite file | Could be RabbitMQ topology (queue-per-subscription) but URL retrieval is awkward; or Postgres for shared multi-instance state |
| Retry mechanism | `rabbitmq-delayed-message-exchange` plugin | TTL-queue chain works without plugin but adds N queues for N backoff steps |
| At-least-once vs exactly-once | At-least-once (manual ack after HTTP success; redelivery possible on worker crash post-POST/pre-ack) | Exactly-once needs idempotency keys at consumer side |
| Single AMQP connection per instance | Yes (channel-multiplexed: 1 publish + 1 consume) | Connection pool (overkill for typical load) |
| Subscriptions live in producer container | Yes | A shared Postgres would let multiple producer instances see the same subscriptions |

---

## 13. TypeScript Convention Compliance

### `tsconfig.json`
```json
{
  "compilerOptions": {
    "target": "ES2020",
    "module": "commonjs",
    "moduleResolution": "node",
    "strict": true,
    "noImplicitAny": true,
    "strictNullChecks": true,
    "noUncheckedIndexedAccess": true,
    "noImplicitReturns": true,
    "noFallthroughCasesInSwitch": true,
    "esModuleInterop": true,
    "forceConsistentCasingInFileNames": true,
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true,
    "outDir": "dist",
    "rootDir": "src"
  },
  "include": ["src/**/*"],
  "exclude": ["node_modules", "dist", "examples"]
}
```

### Code conventions

| Rule | Reason |
|---|---|
| No `any` — `unknown` for arbitrary payloads | Forces narrowing at use sites |
| `interface` for public object shapes (`WebhooksConfig`, `WebhooksInstance`) | Extensible, declaration-merge friendly |
| `type` for unions and primitives (`type JobStatus = 'pending' \| 'failed'`) | Conventional split |
| Explicit return types on public functions | Stable API contract |
| `readonly` on every `WebhooksConfig` field | Prevents post-init mutation |
| Named exports only | Better refactor + tree-shaking |
| Path-relative imports inside `src/` | Avoid path-alias coupling |
| Custom typed errors (`class WebhookError extends Error`) | `instanceof` checks for callers |
| **No top-level side effects in any `src/*.ts` module** | Required for non-blocking import (NFR #3) |
| `.d.ts` declaration output | IntelliSense for consumers |

---

## 14. Out of Scope (given 2-hour limit)

- HMAC signature on webhook payloads
- Wildcard / glob event patterns (`order.*`)
- Per-subscription retry configuration override
- Admin API for inspecting / replaying dead-letter queue
- Separate worker container (called out as the primary tradeoff)
- Horizontal multiple-producer coordination (subscription store is per-producer)
- Exactly-once delivery semantics
- Auth on RabbitMQ beyond default `guest`/`guest` (only safe inside compose network)
- Unit / integration test suite
