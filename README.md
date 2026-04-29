# webhooks-ts

`webhooks-ts` is a reusable TypeScript library for reliable webhook delivery from a Node.js backend.

It gives you a simple producer API:

```ts
const webhooks = createWebhooks({ rabbitmqUrl: 'amqp://localhost:5672' });

await webhooks.register('order.created', 'https://example.com/hook');
await webhooks.emit('order.created', { orderId: 123 });
await webhooks.close();
```

Under the hood it uses:
- SQLite for subscription storage (`event` -> `url`)
- RabbitMQ for durable message buffering and worker consumption
- An in-process worker that POSTs payloads to subscriber URLs

## End-to-End Flow (Install -> Package -> Import -> Run)

### 1. Install dependencies

```bash
npm install
```

### 2. Build the library

```bash
npm run build
```

This compiles TypeScript into `dist/`.

### 3. Package the library (industry-style tarball)

```bash
rm -f webhooks-ts-*.tgz
npm pack
```

This generates `webhooks-ts-<version>.tgz`.

Optional verification:

```bash
tar -tzf webhooks-ts-1.0.0.tgz | head
```

You should see `package/dist/...` entries.

Package settings are in `package.json`:
- `exports` and `types` for clean imports
- `files` whitelist so only publishable artifacts are included
- `sideEffects: false`
- `engines.node >= 20`

### 4. Import from another producer app

In another app:

```bash
npm install /absolute/path/to/webhooks-ts-1.0.0.tgz
```

Then use:

```ts
import { createWebhooks } from 'webhooks-ts';

const webhooks = createWebhooks({
  rabbitmqUrl: 'amqp://guest:guest@localhost:5672',
  dbPath: './webhooks.db',
});

await webhooks.register('order.created', 'http://127.0.0.1:3001/hook');
await webhooks.register('order.shipped', 'http://127.0.0.1:3002/hook');

await webhooks.emit('order.created', { orderId: 123 });
await webhooks.emit('order.shipped', { orderId: 123, shipped: true });

await webhooks.close();
```

Important URL note:
- Use `127.0.0.1`/`localhost` when producer runs on your host machine.
- Use `consumer-1` / `consumer-2` hostnames only when producer runs inside the same Docker Compose network.

## Docker Usage (Producer + RabbitMQ + 2 Consumers)

### Start full stack

```bash
docker compose up --build -d
```

Services:
- `rabbitmq` on `5672` (AMQP), management UI on `15672`
- `producer` demo app
- `consumer-1` Express app on `:3001` (published to host)
- `consumer-2` Express app on `:3002` (published to host)

RabbitMQ UI:
- [http://localhost:15672](http://localhost:15672)
- user/pass: `guest` / `guest`

Follow logs when needed:

```bash
docker compose logs -f producer consumer-1 consumer-2
```

### Stop and clean

```bash
docker compose down -v
```

If you want to test an external packaged producer (from section 4) against these same consumers,
pause the demo producer first to avoid mixed logs:

```bash
docker compose stop producer
```

## How Routing Works (No Accidental Cross-Delivery)

Producer decides delivery by registration.

- If you register only:
  - `order.created -> consumer-1`
  - `order.shipped -> consumer-2`
- Then `order.created` goes only to consumer-1, and `order.shipped` goes only to consumer-2.

If you register two URLs for the same event, both receive it (intentional fan-out for that event).

## How Consumers Receive Messages

Consumer apps expose:
- `POST /hook`

Delivery path:
1. Producer calls `emit(event, payload)`.
2. Library loads all subscribed URLs for that exact event from SQLite.
3. Library publishes one durable RabbitMQ message per target URL.
4. In-process worker consumes from RabbitMQ queue.
5. Worker sends `HTTP POST` to each URL with the payload JSON body.
6. On HTTP `2xx`, message is acknowledged.
7. On failure, message is retried; after `maxRetries`, it is routed to dead-letter exchange/queue.

## SQLite: Access and Persisted Data

SQLite is used only for subscriptions.

Default DB path:
- `./webhooks.db` (or `dbPath` you provide)

In Docker demo producer:
- `/data/webhooks.db` (backed by `producer-data` volume)

Persisted table:
- `subscriptions`
  - `id` (TEXT, PK)
  - `event` (TEXT)
  - `url` (TEXT)
  - `created_at` (INTEGER ms)
  - unique constraint on `(event, url)`

Important:
- Payloads are not stored in SQLite.
- In-flight delivery state is owned by RabbitMQ.

Optional inspect (if `sqlite3` CLI is installed):

```bash
sqlite3 ./webhooks.db "SELECT id,event,url,created_at FROM subscriptions;"
```

Inspect subscriptions inside Docker producer container:

```bash
docker compose exec producer node -e "\
const sqlite3=require('sqlite3');\
const db=new sqlite3.Database('/data/webhooks.db');\
db.all('SELECT id,event,url,created_at FROM subscriptions',[],(e,rows)=>{\
if(e) throw e; console.log(rows); db.close();\
});"
```

## RabbitMQ Topology

- Exchange: `webhooks.x` (topic)
- Exchange: `webhooks.retry` (topic)
- Exchange: `webhooks.dlx` (topic)
- Queue: `webhooks.q` (durable)
- Queue: `webhooks.dlq` (durable)

## Reliability Guarantees

- At-least-once delivery
- Durable queue + persistent publish
- `emit()` waits for publisher confirm before returning
- Worker uses manual ack after successful HTTP response
- Graceful shutdown via `close()`

## Retry Behavior Note

Current code republishes failed messages to `webhooks.retry` and includes `x-delay` header.

Because `webhooks.retry` is currently declared as a standard `topic` exchange, retries happen without broker-enforced delay unless you switch to RabbitMQ delayed-message exchange plugin or implement a TTL retry chain.

## Testing

Run all tests:

```bash
npm test
```

Run unit only:

```bash
npm run test:unit
```

Run e2e only:

```bash
npm run test:e2e
```

E2E tests validate:
- event routing behavior
- retry behavior
- dead-letter behavior
- graceful shutdown

## Public API

```ts
interface WebhooksConfig {
  rabbitmqUrl: string;
  dbPath?: string;
  maxRetries?: number;
  timeoutMs?: number;
  concurrency?: number;
  startupBufferLimit?: number;
  reconnectBackoffMs?: number;
}

interface WebhooksInstance {
  register(event: string, url: string): Promise<void>;
  emit(event: string, payload: unknown): Promise<void>;
  close(): Promise<void>;
}
```
