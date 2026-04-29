Review the code I just wrote (or the file at $ARGUMENTS) against the project's conventions and fix any violations in-place.

Work through each rule below in order. For each violation found, fix it directly in the file — do not just list violations.

---

## 1. TypeScript strictness

- `tsconfig.json` must have `strict: true`, `noUncheckedIndexedAccess: true`, `noImplicitReturns: true`.
- **No `any`** anywhere. Use `unknown` for payloads crossing the public API boundary. Narrow with type guards or `as` only after a runtime check.
- Every array index access (`arr[i]`) must be null-checked because `noUncheckedIndexedAccess` makes it `T | undefined`.
- Every `Promise`-returning function must be `await`-ed or explicitly `.catch()`-ed. Floating promises are bugs.

## 2. Public API surface (`src/index.ts`)

- `createWebhooks(config: WebhooksConfig): WebhooksInstance` is the only exported function.
- All public types (`WebhooksConfig`, `WebhooksInstance`, `WebhookError`) must be re-exported from `src/index.ts` so consumers import from one place.
- **No default exports** anywhere in `src/`. Named exports only.
- All `WebhooksConfig` fields must be `readonly`.
- Every public function must have an explicit return type annotation. Internal functions may use inferred types.

## 3. Types — interface vs type

- `interface` for object shapes that are part of the public contract (`WebhooksConfig`, `WebhooksInstance`, `JobHeaders`).
- `type` for unions, primitives, and mapped types (`type JobStatus = 'pending' | 'processing' | 'delivered' | 'failed'`).
- Do not use `interface` for internal-only shapes that are never exported — `type` is fine there.

## 4. No top-level side effects in `src/` modules

The most important convention for non-blocking library load. Any file under `src/` must not execute I/O, timers, or open connections at module evaluation time.

Bad:
```ts
// src/broker.ts
const connection = await amqplib.connect(url); // ← runs on import
```

Good:
```ts
// src/broker.ts
export async function connectBroker(url: string): Promise<Connection> { ... }
// called only from inside createWebhooks(), behind setImmediate
```

If you find a top-level `await`, a `new X()` that opens sockets, or a `setInterval`/`setTimeout` at module scope — move it inside the deferred init block in `src/index.ts`.

## 5. Error handling

- All thrown errors must extend `WebhookError` (which extends `Error`). Never `throw new Error(...)` directly in library code — callers can't `instanceof`-check a plain `Error`.
- Worker callback must wrap the entire delivery+ack cycle in `try/catch`. On an uncaught error, nack with `requeue: false` so the message routes to DLX instead of looping.
- AMQP `channel.publish()` return value (boolean) must be checked — `false` means the write buffer is full; wait for the `drain` event before publishing more.

```ts
// correct pattern
if (!channel.publish(exchange, key, content, options)) {
  await once(channel, 'drain');
}
```

## 6. RabbitMQ conventions

- **Two channels, one connection.** `publishChannel` has publisher confirms enabled (`channel.confirmSelect()`). `consumeChannel` has `channel.prefetch(concurrency)` and `noAck: false`. Never publish on the consume channel or vice versa.
- **Always `await channel.waitForConfirms()`** after bulk publishes in `emit()`. Do not return from `emit()` before confirms arrive.
- **Manual ack discipline.** Every message consumed must be either `ack`-ed or `nack`-ed exactly once. Check every code path (success, retry, dead-letter, exception) — a missed ack causes the queue to grow until the connection drops.
- Message headers must carry `{ url: string, attempts: number, maxRetries: number, originalEvent: string }`. Read these in the worker; do not re-query SQLite per delivery.
- Exchange and queue names are constants — define them once in `src/broker.ts` and import everywhere. Never hard-code the strings `'webhooks.x'` etc. in two places.

## 7. SQLite conventions (subscription store only)

- SQLite is used **only** for the `subscriptions` table. There is no `webhook_jobs` table. Do not add one.
- On open, always set:
  ```sql
  PRAGMA journal_mode = WAL;
  PRAGMA synchronous = FULL;
  PRAGMA busy_timeout = 5000;
  ```
- `addSubscription` must use `INSERT OR IGNORE` with a UNIQUE constraint on `(event, url)` — calling `register` twice for the same pair is idempotent.
- Wrap multi-statement SQLite operations in `BEGIN`/`COMMIT` explicitly. Do not rely on auto-commit for anything that must be atomic.

## 8. Memory leak checklist

Before closing a PR, verify:

- [ ] Every `setTimeout` used for reconnect is stored in a ref and `unref()`'d. No bare `setTimeout` calls that are never cleared.
- [ ] `AbortController` is created per HTTP request and garbage-collectable after the request resolves.
- [ ] `channel.prefetch(concurrency)` is set — unbounded prefetch will load all queued messages into memory.
- [ ] `close()` cancels the consumer tag (`channel.cancel(consumerTag)`) before closing the channel.
- [ ] `close()` closes `publishChannel`, then `consumeChannel`, then `connection`, then `db` — in that order.
- [ ] `close()` is idempotent: a second call is a no-op (guard with an `isClosed` flag).
- [ ] The startup buffer array is spliced/cleared after flushing — not retained.

## 9. Docker / example conventions

- `Dockerfile`s use multi-stage builds: `FROM node:20-alpine AS builder` → `tsc`, then `FROM node:20-alpine` for runtime. Only `dist/` and `node_modules` (production only) go into the final image.
- Environment variables consumed by the producer: `RABBITMQ_URL`, `DB_PATH`. Both must be read at runtime (`process.env`), not hard-coded.
- Consumer apps (`consumer-1`, `consumer-2`) must respond to `POST /hook` with HTTP 200 within the producer's `timeoutMs`. A slow response that times out triggers a retry — make sure the demo handler is synchronous.
- Graceful shutdown in example apps: listen for `SIGTERM` → call `webhooks.close()` → `process.exit(0)`.

## 10. File responsibility boundaries

The codebase uses a strict **Controller → Service → Repository** layered architecture (enforced by `/architecture`). Each layer and file owns exactly one concern — cross-layer imports in the wrong direction are a bug, not a style issue.

| File / folder | Layer | Owns | Must NOT import |
|---|---|---|---|
| `repositories/subscription.repository.ts` | Repository | SQLite CRUD for subscriptions | `services/`, `broker.ts`, `worker.ts`, `delivery.ts` |
| `services/webhook.service.ts` | Service | Business logic, fan-out, broker orchestration | `repositories/*` raw driver, HTTP `req`/`res`, AMQP channel primitives |
| `controllers/webhook.controller.ts` | Controller | Parse req, validate input, call one service, shape response | SQL, AMQP, business conditionals |
| `broker.ts` | Infrastructure | AMQP connect, topology assert, channels | `repositories/`, `services/`, `delivery.ts` |
| `publisher.ts` | Infrastructure | publish-with-confirm | `repositories/`, `worker.ts` |
| `worker.ts` | Infrastructure | Consume loop, ack/retry/dead-letter | `repositories/`, `publisher.ts` |
| `delivery.ts` | Infrastructure | HTTP POST + AbortController | anything except `types.ts` |
| `backoff.ts` | Utility | Pure delay calculation | anything |
| `db.ts` | Infrastructure | Open SQLite, run pragmas + migrations | anything except `types.ts` |
| `index.ts` | Composition root | Wire repo → service → public API | — (may import all) |

Dependency direction is always **top-down within layers** and **inward** toward `types.ts`. If a lower layer needs something from a higher one, the dependency belongs in `index.ts` via injection.

Run `/architecture` to enforce Controller → Service → Repository rules in detail.
