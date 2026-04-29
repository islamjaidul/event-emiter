Run every test suite, identify failures, fix the root cause in source code, and re-run until the full suite is green. Never skip, comment out, or weaken a test assertion to force a pass.

---

## Step-by-step execution

### 1. Run unit tests first

```bash
npm run test:unit
```

Capture all failures. For each failing test:

1. Read the test file to understand what behaviour it is asserting.
2. Read the source file under test to find the actual bug.
3. Fix the **source file** (not the test). Only modify a test if it contains a genuine mistake in the assertion itself (wrong expected value, wrong mock setup) — and explain why before changing it.
4. Re-run the single failing file to confirm the fix:
   ```bash
   npm run test:unit -- --testPathPattern="<filename>"
   ```
5. Continue until `npm run test:unit` exits 0.

### 2. Run e2e tests

```bash
npm run test:e2e
```

E2e tests spin up a real RabbitMQ container via Testcontainers and a real SQLite file. They take longer (~30–60 s). For each failure:

1. Read the e2e test to understand the scenario (delivery, retry, dead-letter, shutdown).
2. Check whether the failure is in source code or test setup (Docker/network/timing).
3. Fix source code first. If it is a timing issue, increase the `waitFor` timeout — do not shorten it.
4. Re-run the single e2e file:
   ```bash
   npm run test:e2e -- --testPathPattern="<filename>"
   ```
5. Continue until `npm run test:e2e` exits 0.

### 3. Run full suite to confirm nothing regressed

```bash
npm test
```

If any test regressed (was passing before your fix and is now failing), fix that too before finishing.

---

## Test commands reference

```bash
npm test                                          # unit + e2e
npm run test:unit                                 # Jest on tests/unit/**
npm run test:e2e                                  # Jest on tests/e2e/**
npm run test:unit -- --testPathPattern="backoff"  # single unit file
npm run test:e2e  -- --testPathPattern="retry"    # single e2e file
npm run test:unit -- --watch                      # watch mode (unit only)
npm run test:coverage                             # coverage report
```

---

## What each test layer covers

### Unit tests — `tests/unit/`

Fast, no I/O, all dependencies mocked. Each file tests exactly one class or module.

| Test file | What it tests | Key mocks |
|---|---|---|
| `repositories/subscription.repository.test.ts` | SQL correctness, idempotent insert, event lookup | In-memory SQLite (`:memory:`) — no mock, real driver |
| `services/webhook.service.test.ts` | register validation, emit fan-out, empty-subscriber skip | `ISubscriptionRepository`, `IPublisher` — Jest mocks |
| `controllers/webhook.controller.test.ts` | request parsing, validation errors → 400, service errors → 422/500, success → 200 | `IWebhookService` — Jest mock; `supertest` for HTTP |
| `backoff.test.ts` | delay schedule per attempt, cap at 6h | none — pure function |
| `delivery.test.ts` | 2xx → success, non-2xx → failure, timeout → failure | `global.fetch` — Jest `spyOn` |
| `worker.test.ts` | ack on success, retry publish on failure, dead-letter after maxRetries, nack on uncaught error | `amqplib.Channel` — Jest mock; `deliverHttp` — Jest mock |

#### Repository unit test pattern

```typescript
// tests/unit/repositories/subscription.repository.test.ts
import Database from 'better-sqlite3';
import { SubscriptionRepository } from '../../../src/repositories/subscription.repository';

describe('SubscriptionRepository', () => {
  let db: Database.Database;
  let repo: SubscriptionRepository;

  beforeEach(() => {
    db = new Database(':memory:');
    db.pragma('journal_mode = WAL');
    db.exec(`
      CREATE TABLE subscriptions (
        id TEXT PRIMARY KEY,
        event TEXT NOT NULL,
        url TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        UNIQUE(event, url)
      )
    `);
    repo = new SubscriptionRepository(db);
  });

  afterEach(() => db.close());

  it('adds a subscription', async () => {
    await repo.add('order.created', 'http://localhost:3001/hook');
    const subs = await repo.findByEvent('order.created');
    expect(subs).toHaveLength(1);
    expect(subs[0]?.url).toBe('http://localhost:3001/hook');
  });

  it('is idempotent — duplicate register is a no-op', async () => {
    await repo.add('order.created', 'http://localhost:3001/hook');
    await repo.add('order.created', 'http://localhost:3001/hook'); // should not throw
    const subs = await repo.findByEvent('order.created');
    expect(subs).toHaveLength(1);
  });

  it('returns empty array for unknown event', async () => {
    const subs = await repo.findByEvent('unknown.event');
    expect(subs).toHaveLength(0);
  });

  it('returns only subscribers for the queried event', async () => {
    await repo.add('order.created', 'http://a.com/hook');
    await repo.add('order.shipped', 'http://b.com/hook');
    const subs = await repo.findByEvent('order.created');
    expect(subs).toHaveLength(1);
    expect(subs[0]?.event).toBe('order.created');
  });
});
```

#### Service unit test pattern

```typescript
// tests/unit/services/webhook.service.test.ts
import { WebhookService } from '../../../src/services/webhook.service';
import type { ISubscriptionRepository } from '../../../src/repositories/subscription.repository';
import type { IPublisher } from '../../../src/publisher';
import { ValidationError } from '../../../src/types';

const makeRepo = (): jest.Mocked<ISubscriptionRepository> => ({
  add: jest.fn().mockResolvedValue(undefined),
  findByEvent: jest.fn().mockResolvedValue([]),
  remove: jest.fn().mockResolvedValue(undefined),
});

const makePublisher = (): jest.Mocked<IPublisher> => ({
  publish: jest.fn().mockResolvedValue(undefined),
});

describe('WebhookService', () => {
  let repo: jest.Mocked<ISubscriptionRepository>;
  let publisher: jest.Mocked<IPublisher>;
  let service: WebhookService;

  beforeEach(() => {
    repo = makeRepo();
    publisher = makePublisher();
    service = new WebhookService(repo, publisher);
  });

  describe('register', () => {
    it('delegates to repository', async () => {
      await service.register('order.created', 'http://example.com/hook');
      expect(repo.add).toHaveBeenCalledWith('order.created', 'http://example.com/hook');
    });

    it('throws ValidationError for empty event', async () => {
      await expect(service.register('', 'http://example.com/hook'))
        .rejects.toBeInstanceOf(ValidationError);
    });

    it('throws ValidationError for non-http url', async () => {
      await expect(service.register('order.created', 'ftp://bad.url'))
        .rejects.toBeInstanceOf(ValidationError);
    });
  });

  describe('emit', () => {
    it('publishes one message per subscriber', async () => {
      repo.findByEvent.mockResolvedValue([
        { id: '1', event: 'order.created', url: 'http://c1.com/hook', createdAt: Date.now() },
        { id: '2', event: 'order.created', url: 'http://c2.com/hook', createdAt: Date.now() },
      ]);

      await service.emit('order.created', { orderId: 42 });

      expect(publisher.publish).toHaveBeenCalledTimes(2);
      expect(publisher.publish).toHaveBeenCalledWith('order.created', 'http://c1.com/hook', { orderId: 42 });
      expect(publisher.publish).toHaveBeenCalledWith('order.created', 'http://c2.com/hook', { orderId: 42 });
    });

    it('skips publish silently when no subscribers', async () => {
      repo.findByEvent.mockResolvedValue([]);
      await service.emit('order.created', { orderId: 42 });
      expect(publisher.publish).not.toHaveBeenCalled();
    });
  });
});
```

#### Controller unit test pattern

```typescript
// tests/unit/controllers/webhook.controller.test.ts
import express from 'express';
import request from 'supertest';
import { WebhookController } from '../../../examples/consumer-1/controllers/webhook.controller';
import { ServiceError, ValidationError } from '../../../src/types';
import type { IInboundWebhookService } from '../../../examples/consumer-1/services/inbound-webhook.service';

const makeService = (): jest.Mocked<IInboundWebhookService> => ({
  process: jest.fn().mockResolvedValue(undefined),
});

function buildApp(controller: WebhookController) {
  const app = express();
  app.use(express.json());
  app.post('/hook', (req, res) => controller.handleIncoming(req, res));
  return app;
}

describe('WebhookController', () => {
  let service: jest.Mocked<IInboundWebhookService>;
  let app: express.Express;

  beforeEach(() => {
    service = makeService();
    app = buildApp(new WebhookController(service));
  });

  it('returns 200 and calls service on valid payload', async () => {
    const res = await request(app)
      .post('/hook')
      .send({ event: 'order.created', data: { orderId: 1 } });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ received: true });
    expect(service.process).toHaveBeenCalledTimes(1);
  });

  it('returns 400 for missing event field', async () => {
    const res = await request(app).post('/hook').send({ data: {} });
    expect(res.status).toBe(400);
    expect(service.process).not.toHaveBeenCalled();
  });

  it('returns 422 when service throws ServiceError', async () => {
    service.process.mockRejectedValue(new ServiceError('duplicate'));
    const res = await request(app)
      .post('/hook')
      .send({ event: 'order.created', data: {} });
    expect(res.status).toBe(422);
  });

  it('returns 500 on unexpected error', async () => {
    service.process.mockRejectedValue(new Error('db exploded'));
    const res = await request(app)
      .post('/hook')
      .send({ event: 'order.created', data: {} });
    expect(res.status).toBe(500);
  });
});
```

---

### E2e tests — `tests/e2e/`

Real RabbitMQ (Testcontainers), real SQLite, real HTTP servers. Tests assert observable outcomes, not internals.

| Test file | Scenario | Pass condition |
|---|---|---|
| `webhook-delivery.e2e.test.ts` | register both consumers, emit `order.created`, emit `order.shipped` (consumer-1 only) | consumer-1 receives both; consumer-2 receives only `order.created` |
| `retry-behavior.e2e.test.ts` | consumer-1 returns 500 twice, then 200 | consumer-1 receives the message 3 times total; message not in DLQ |
| `dead-letter.e2e.test.ts` | consumer always returns 500, exhaust maxRetries | message appears in `webhooks.dlq`; no further HTTP calls |
| `graceful-shutdown.e2e.test.ts` | emit then immediately close() | consumer still receives the message (broker held it during shutdown) |

#### E2e helper pattern

```typescript
// tests/e2e/helpers/test-broker.ts
import { RabbitMQContainer, StartedRabbitMQContainer } from '@testcontainers/rabbitmq';

export async function startBroker(): Promise<{
  container: StartedRabbitMQContainer;
  url: string;
}> {
  const container = await new RabbitMQContainer('rabbitmq:3-management')
    .withExposedPorts(5672)
    .start();
  const url = `amqp://${container.getHost()}:${container.getMappedPort(5672)}`;
  return { container, url };
}

// tests/e2e/helpers/test-consumer.ts
import http from 'node:http';

export function startTestConsumer(
  port: number,
  handler: (body: unknown) => number, // returns HTTP status code
): { server: http.Server; calls: unknown[] } {
  const calls: unknown[] = [];
  const server = http.createServer((req, res) => {
    let raw = '';
    req.on('data', (chunk: Buffer) => { raw += chunk.toString(); });
    req.on('end', () => {
      const body: unknown = JSON.parse(raw);
      calls.push(body);
      const status = handler(body);
      res.writeHead(status).end();
    });
  });
  server.listen(port);
  return { server, calls };
}
```

#### E2e delivery test pattern

```typescript
// tests/e2e/webhook-delivery.e2e.test.ts
import { startBroker } from './helpers/test-broker';
import { startTestConsumer } from './helpers/test-consumer';
import { createWebhooks } from '../../src/index';

describe('webhook delivery e2e', () => {
  it('delivers order.created to both consumers, order.shipped to consumer-1 only', async () => {
    const { container, url } = await startBroker();
    const c1 = startTestConsumer(14001, () => 200);
    const c2 = startTestConsumer(14002, () => 200);

    const webhooks = createWebhooks({
      rabbitmqUrl: url,
      dbPath: ':memory:',
      maxRetries: 3,
      timeoutMs: 3000,
    });

    await webhooks.register('order.created', 'http://localhost:14001/hook');
    await webhooks.register('order.created', 'http://localhost:14002/hook');
    await webhooks.register('order.shipped', 'http://localhost:14001/hook');

    await webhooks.emit('order.created', { orderId: 1 });
    await webhooks.emit('order.shipped', { orderId: 1 });

    // wait for async delivery
    await waitFor(() => c1.calls.length >= 2 && c2.calls.length >= 1, 10_000);

    expect(c1.calls).toHaveLength(2);
    expect(c2.calls).toHaveLength(1);

    await webhooks.close();
    c1.server.close();
    c2.server.close();
    await container.stop();
  }, 60_000);
});

async function waitFor(condition: () => boolean, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, 200));
  }
}
```

---

## Jest configuration

### `jest.config.ts` (root)

```typescript
import type { Config } from 'jest';

const config: Config = {
  projects: [
    {
      displayName: 'unit',
      testMatch: ['<rootDir>/tests/unit/**/*.test.ts'],
      preset: 'ts-jest',
      testEnvironment: 'node',
      clearMocks: true,
    },
    {
      displayName: 'e2e',
      testMatch: ['<rootDir>/tests/e2e/**/*.e2e.test.ts'],
      preset: 'ts-jest',
      testEnvironment: 'node',
      testTimeout: 60_000,
      clearMocks: true,
    },
  ],
  collectCoverageFrom: [
    'src/**/*.ts',
    '!src/index.ts',   // composition root — covered by e2e
    '!src/types.ts',   // pure types
  ],
  coverageThresholds: {
    global: { lines: 80, functions: 80, branches: 70 },
  },
};

export default config;
```

### `package.json` scripts

```json
{
  "scripts": {
    "test":          "jest",
    "test:unit":     "jest --selectProjects unit",
    "test:e2e":      "jest --selectProjects e2e",
    "test:watch":    "jest --selectProjects unit --watch",
    "test:coverage": "jest --coverage"
  }
}
```

---

## Hard rules for tests

- **Never use `any` in test code.** Use the same type discipline as production code.
- **Never mock the repository with SQLite.** The repository test uses a real in-memory SQLite DB — that is the point.
- **Never mock RabbitMQ in e2e tests.** If you need RabbitMQ mocked, write a unit test instead.
- **`waitFor` timeouts must be generous** (≥5s for unit, ≥30s for e2e). Do not tighten timeouts to make tests faster — flaky tests are worse than slow tests.
- **Each test is independent.** `beforeEach` rebuilds all state. No shared mutable state between tests.
- **`afterEach` / `afterAll` must always clean up** — close servers, containers, DB handles. A leaked handle will hang the Jest process.
- **Test file names must match:** `*.test.ts` for unit, `*.e2e.test.ts` for e2e. The Jest projects config splits them on this pattern.
