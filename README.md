# webhooks-ts

A reusable TypeScript library for reliable webhook delivery. Drop it into any Node.js backend to emit events that are durably delivered to registered HTTP subscribers via **RabbitMQ**.

```ts
const webhooks = createWebhooks({ rabbitmqUrl: 'amqp://localhost:5672' });

await webhooks.register('order.created', 'https://consumer.example.com/hook');
await webhooks.emit('order.created', { orderId: 123 });
```

---

## How to run (Docker)

**Prerequisites:** Docker + Docker Compose installed.

```bash
git clone <repo-url>
cd event-emiter

docker-compose up --build
```

This starts four containers:

| Container | Purpose | Port |
|---|---|---|
| `rabbitmq` | Message broker + management UI | 5672 (AMQP), 15672 (UI) |
| `producer` | Demo app — registers subscribers and emits events every 5s | — |
| `consumer-1` | Webhook receiver — logs `order.created` and `order.shipped` | 3001 |
| `consumer-2` | Webhook receiver — logs `order.created` only | 3002 |

**RabbitMQ management UI:** http://localhost:15672 — login `guest` / `guest`

**Tear down:**
```bash
docker-compose down -v
```

---

## Local development (without Docker)

```bash
npm install
npm run build

# In separate terminals:
npm run start:consumer-1   # starts Express on :3001
npm run start:consumer-2   # starts Express on :3002
npm run start:producer     # requires RABBITMQ_URL env var
```

---

## Tests

```bash
npm test                  # unit + e2e
npm run test:unit         # fast, no Docker required
npm run test:e2e          # spins up RabbitMQ via Testcontainers
npm run test:coverage     # coverage report

# Single file
npm run test:unit -- --testPathPattern="subscription.repository"
npm run test:e2e  -- --testPathPattern="retry-behavior"
```

**Unit tests** cover: repository SQL correctness, service business logic, controller HTTP response mapping, backoff schedule, HTTP delivery, worker ack/retry/dead-letter logic.

**E2e tests** cover: fan-out delivery to both consumers, retry on 500, dead-letter after max retries, message survival across graceful shutdown.

---

## Architecture

```
Producer App (your service + this library)
  │
  ├─ register(event, url) → SQLite subscription store
  │
  └─ emit(event, payload)
       │
       ├─ 1. Look up subscribers from SQLite
       ├─ 2. Publish persistent message per subscriber to RabbitMQ
       ├─ 3. Await publisher confirm → return to caller (non-blocking)
       │
       └─ [background worker]
            ├─ Consume from RabbitMQ (manual ack)
            ├─ HTTP POST to subscriber URL
            ├─ 2xx → ack
            ├─ failure + retries left → re-publish with backoff delay
            └─ failure + no retries left → dead-letter queue
```

### RabbitMQ topology

| Object | Type | Purpose |
|---|---|---|
| `webhooks.x` | topic exchange | Primary publish target |
| `webhooks.retry` | delayed-message exchange | Holds messages during backoff |
| `webhooks.dlx` | topic exchange | Dead-letter routing |
| `webhooks.q` | durable queue | Worker consumes from here |
| `webhooks.dlq` | durable queue | Failed messages — retained for audit |

### Layered architecture

```
Controller  →  parse req / send res
Service     →  business logic, fan-out orchestration
Repository  →  all SQLite reads and writes
```

---

## API

```ts
import { createWebhooks } from 'webhooks-ts';

const webhooks = createWebhooks({
  rabbitmqUrl: string;          // required — e.g. amqp://guest:guest@localhost:5672
  dbPath?: string;              // SQLite file path (default: ./webhooks.db)
  maxRetries?: number;          // before dead-lettering (default: 5)
  timeoutMs?: number;           // per-request HTTP timeout ms (default: 10000)
  concurrency?: number;         // max parallel deliveries (default: 10)
  startupBufferLimit?: number;  // max ops buffered before READY (default: 1000)
  reconnectBackoffMs?: number;  // initial AMQP reconnect delay ms (default: 1000)
});

await webhooks.register(event: string, url: string): Promise<void>;
await webhooks.emit(event: string, payload: unknown): Promise<void>;
await webhooks.close(): Promise<void>;
```

**Singleton pattern** — create once at startup, import the instance anywhere:

```ts
// main.ts
export const webhooks = createWebhooks({ rabbitmqUrl: process.env.RABBITMQ_URL! });

process.on('SIGTERM', async () => {
  await webhooks.close();
  process.exit(0);
});

// order.service.ts
import { webhooks } from './main';
await webhooks.emit('order.shipped', { orderId: 42 });
```

---

## Delivery guarantees

**At-least-once delivery.** A message is never lost once `emit()` resolves.

- `emit()` publishes a **persistent** message and waits for a **publisher confirm** from RabbitMQ before returning — the broker has durably written it to disk.
- Queues are **durable** — they survive broker restarts.
- The worker uses **manual ack** — a message is only removed from the queue after a successful HTTP 2xx response.
- If the worker crashes after POSTing but before acking, RabbitMQ **re-delivers** on reconnect.
- Failed deliveries are retried with **exponential backoff**:

| Attempt | Delay |
|---|---|
| 1st failure | 10 s |
| 2nd failure | 1 min |
| 3rd failure | 5 min |
| 4th failure | 30 min |
| 5th failure | 2 h |
| 6th+ | capped at 6 h |

- After `maxRetries`, the message is moved to `webhooks.dlq` and **retained** — never deleted.

**Inspect dead-letter queue:**
```bash
# via RabbitMQ management UI
open http://localhost:15672/#/queues/%2F/webhooks.dlq

# via CLI inside the rabbitmq container
docker exec -it <rabbitmq-container> rabbitmqctl list_queues name messages
```

---

## Tradeoff — in-library worker vs separate worker container

The delivery worker currently runs **inside the producer process**. This simplifies deployment (no extra container) but means slow webhook batches share CPU with the producer's own request handling, and the worker cannot scale independently.

With more time this would be extracted into its own container — consuming from the same `webhooks.q` queue — so it could scale to N workers behind a single busy producer. The public API (`register`, `emit`, `close`) would not change; only the deployment topology would.

---

## Project skills (Claude Code)

| Command | Purpose |
|---|---|
| `/conventions` | Enforce TypeScript, AMQP, SQLite, and Docker conventions |
| `/architecture` | Enforce Controller → Service → Repository layer boundaries |
| `/test` | Run all tests, diagnose failures, fix source, re-run until green |
