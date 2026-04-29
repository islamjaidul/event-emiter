# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project state

Greenfield repository. Only `requirement.md` (the brief), `plan.md` (the full architectural plan), and this file exist — no source code yet. Treat `plan.md` as the source of truth for every architectural decision. Do not deviate from it without explicit user direction.

The 2-hour scope is non-negotiable. Stay inside §14 "Out of Scope" in `plan.md` unless asked.

## What this codebase is

A reusable TypeScript library (`webhooks-ts`) that sits inside a host Node.js backend (the **producer**). When the host calls `emit(event, payload)`, the library publishes a durable message to **RabbitMQ**. An in-library background worker consumes from RabbitMQ and HTTP-POSTs the payload to registered subscriber URLs (**consumers**). The full system runs in Docker:

```
producer (container)  ──publish──►  rabbitmq (container)  ──consume──►  worker (in producer)
                                                                              │
                                                                    HTTP POST ▼
                                              consumer-1 (container :3001)
                                              consumer-2 (container :3002)
```

Public API surface:

```ts
const webhooks = createWebhooks({ rabbitmqUrl: '...', dbPath: '...' });
await webhooks.register('order.created', 'http://consumer-1:3001/hook');
await webhooks.emit('order.created', { orderId: 123 });
await webhooks.close();
```

## Hard rules (from `requirement.md`)

Any change that breaks these is a regression.

1. **No data loss.** `emit()` calls `channel.waitForConfirms()` before returning — the broker has durably accepted the message. Messages are persistent (`deliveryMode: 2`), queues are durable. The caller is blocked only until broker-confirm, not until HTTP delivery.
2. **`emit()` does not block on HTTP.** Slow or failing consumers never propagate latency to the caller. HTTP delivery is fully async inside the worker.
3. **Library load is non-blocking.** `createWebhooks()` returns immediately. All I/O (SQLite open, RabbitMQ connect, exchange/queue assertions, consumer start) is deferred via `setImmediate`. Calls before READY are buffered in a bounded queue.
4. **No memory leaks.** Reconnect timers must be `unref()`'d. Every HTTP request uses `AbortController`. The startup buffer is bounded by `startupBufferLimit`. `close()` must release every handle: consumer tag, AMQP channel, AMQP connection, SQLite connection.
5. **Graceful shutdown.** `close()` is idempotent: cancel consumer → wait for in-flight HTTP → close channel → close connection → close SQLite.
6. **Strict TypeScript.** `strict: true`, `noUncheckedIndexedAccess: true`, no `any` anywhere. See §13 of `plan.md` for the full convention list.

## Architecture decisions to understand before editing

**Two storage responsibilities, two backends:**
- **SQLite** (`subscriptions` table, WAL + `synchronous=FULL`) — stores `(event, url)` pairs written by `register()`. Queried at `emit()` time to fan-out messages. This is the only place SQLite is used; there is no `webhook_jobs` table.
- **RabbitMQ** — owns all in-flight delivery state. Do not add a SQLite jobs table.

**RabbitMQ topology (asserted on startup):**

| Object | Type | Purpose |
|---|---|---|
| `webhooks.x` | topic exchange (durable) | Primary publish target from `emit()` |
| `webhooks.retry` | `x-delayed-message` exchange | Holds retry messages during backoff delay |
| `webhooks.dlx` | topic exchange (durable) | Dead-letter routing after `maxRetries` |
| `webhooks.q` | durable queue | Worker consumes from here, manual ack |
| `webhooks.dlq` | durable queue | Retained dead-letter messages |

Bindings: `webhooks.x → webhooks.q` (`#`), `webhooks.retry → webhooks.q` (`#`), `webhooks.dlx → webhooks.dlq` (`#`).

**Delivery and retry flow:**
1. `emit()` looks up subscribers from SQLite, publishes one persistent message per `(event, url)` to `webhooks.x` with `headers: { url, attempts: 0, maxRetries }`, then awaits publisher confirm.
2. Worker consumes from `webhooks.q` with `noAck: false` and `prefetch(concurrency)`.
3. On HTTP 2xx → `channel.ack(msg)`.
4. On failure with `attempts < maxRetries` → re-publish to `webhooks.retry` with `headers['x-delay'] = backoff(attempts)` and `attempts++`, then `channel.ack(msg)` the original.
5. On failure with `attempts >= maxRetries` → publish to `webhooks.dlx`, then `channel.ack(msg)`.

**Worker is in-process.** It shares the AMQP connection with the publisher (separate channels). This is the documented primary tradeoff — a separate worker container would be the next step with more time.

**Two channels on one connection:** `publishChannel` (publisher confirms enabled) and `consumeChannel` (manual ack, prefetch set). Never mix them.

**AMQP reconnect:** On connection error, a single tracked `setTimeout` (with `unref()`) retries with exponential backoff. No `setInterval`.

## File layout

```
src/
  index.ts      # createWebhooks() factory — init manager + startup buffer + public API
  types.ts      # WebhooksConfig, WebhooksInstance, JobHeaders (all interfaces/types)
  db.ts         # async SQLite open, WAL pragmas, schema migration
  registry.ts   # addSubscription (UNIQUE on event+url), getSubscribers
  broker.ts     # AMQP connect, assert topology, reconnect loop, channel setup
  publisher.ts  # publish-with-confirm helper used by emit()
  worker.ts     # AMQP consumer handler — HTTP POST → ack/retry/dead-letter
  delivery.ts   # fetch POST with AbortController timeout
  backoff.ts    # nextDelayMs(attempts) → exponential schedule
examples/
  producer/
    index.ts    # singleton setup, registers both consumers, emits on interval, SIGTERM handler
    Dockerfile  # multi-stage: tsc build → node:20-alpine
  consumer-1/
    index.ts    # Express :3001, POST /hook → log + 200
    Dockerfile
  consumer-2/
    index.ts    # Express :3002, POST /hook → log + 200
    Dockerfile
docker-compose.yml
```

`index.ts` is the only file that ties everything together — all other `src/` files are pure modules with no cross-imports except through their direct dependency chain. Internal helpers are not exported; only `index.ts` re-exports public types.

## Commands

```bash
# Bring up the full system (rabbitmq + producer + consumer-1 + consumer-2)
docker-compose up --build

# Tear down and remove volumes
docker-compose down -v

# Build TypeScript only (no Docker)
npm run build

# Run individual example processes locally (RabbitMQ must be running separately)
npm run start:producer
npm run start:consumer-1
npm run start:consumer-2

# RabbitMQ management UI (when running via compose)
open http://localhost:15672   # guest / guest
```

### Test commands

```bash
npm test                                                # full suite: unit + e2e
npm run test:unit                                       # unit tests only (fast, no Docker)
npm run test:e2e                                        # e2e tests (spins up RabbitMQ via Testcontainers)
npm run test:watch                                      # unit watch mode
npm run test:coverage                                   # coverage report

# Run a single test file
npm run test:unit -- --testPathPattern="subscription.repository"
npm run test:e2e  -- --testPathPattern="retry-behavior"
```

**Run the full suite after every change.** If a test fails, fix the source before moving on — never skip or weaken assertions. Use `/test` to have Codex run, diagnose, and fix failing tests automatically.

## TypeScript conventions (enforced)

- `interface` for public object shapes; `type` for unions and primitives.
- Named exports only — no default exports anywhere in `src/`.
- `readonly` on all `WebhooksConfig` fields.
- Explicit return types on public functions; inferred types are fine internally.
- `unknown` for the `payload` parameter — never `any`.
- `class WebhookError extends Error` for typed errors callers can `instanceof`-check.
- **No top-level side effects in any `src/*.ts` module** — required for non-blocking import. All side-effecting init happens inside `createWebhooks()`, behind `setImmediate`.
- The `x-delayed-message` exchange type requires the `rabbitmq_delayed_message_exchange` plugin. If the plugin is unavailable, fall back to a TTL-queue chain (documented in `plan.md` §12 tradeoffs).

## Project skills (slash commands)

| Command | When to use |
|---|---|
| `/conventions` | After writing any `src/` file — checks TypeScript strictness, AMQP rules, SQLite rules, memory leak checklist, Docker conventions. Fixes violations in-place. |
| `/architecture` | After adding a new class or module — enforces Controller → Service → Repository layer boundaries, DI wiring, error taxonomy, naming conventions. |
| `/test` | After any code change — runs `npm run test:unit` then `npm run test:e2e`, diagnoses failures, fixes root cause in source, re-runs until green. |

Run all three skills in sequence when finishing a feature: `/conventions` → `/architecture` → `/test`.

## When in doubt

- Architecture or scope → `plan.md` §1–§14.
- Functional intent → `requirement.md` (36 lines, authoritative).
- Accepted tradeoffs → `plan.md` §12 (in-library worker, SQLite for subscriptions only, at-least-once over exactly-once, delayed-exchange over TTL chain).
- Test patterns and Jest config → `.Codex/commands/test.md` (has concrete examples for every test file).
