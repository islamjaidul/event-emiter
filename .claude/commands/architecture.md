Enforce the Controller → Service → Repository layered architecture across the codebase. When generating or reviewing code, apply every rule below. Fix violations in-place rather than listing them.

---

## Layer overview

```
HTTP Request
     │
     ▼
┌─────────────────────────────────────────────────────┐
│  Controller  (src/controllers/ | examples/*/routes) │
│  Owns: parse req, validate input, call service,     │
│        shape and send HTTP response                 │
│  Knows nothing about: DB, RabbitMQ, business rules  │
└─────────────────────┬───────────────────────────────┘
                      │ calls
                      ▼
┌─────────────────────────────────────────────────────┐
│  Service  (src/services/)                           │
│  Owns: business logic, orchestration, broker calls  │
│  Knows nothing about: HTTP req/res, raw SQL         │
└─────────────────────┬───────────────────────────────┘
                      │ calls
                      ▼
┌─────────────────────────────────────────────────────┐
│  Repository  (src/repositories/)                    │
│  Owns: all DB reads/writes (SQLite)                 │
│  Knows nothing about: HTTP, broker, business rules  │
└─────────────────────────────────────────────────────┘
```

Dependency direction is **strictly top-down**. A lower layer never imports from a higher one.

---

## 1. Repository layer — `src/repositories/`

Responsible for **all SQLite interactions**. No raw SQL outside this layer.

### Rules

- One repository class per aggregate root. This project has one: `SubscriptionRepository`.
- Constructor accepts a `Database` instance (injected — never opened inside the repository).
- Methods are `async`, return typed domain objects (not raw `sqlite3` row objects).
- No business logic — no conditionals that encode rules, no calls to broker or HTTP.
- Errors from the DB driver are caught here, wrapped in `RepositoryError extends WebhookError`, and re-thrown. Never let `sqlite3` error objects leak to the service layer.

### Interface contract

```typescript
// src/repositories/subscription.repository.ts

export interface ISubscriptionRepository {
  add(event: string, url: string): Promise<void>;
  findByEvent(event: string): Promise<Subscription[]>;
  remove(event: string, url: string): Promise<void>;
}

export class SubscriptionRepository implements ISubscriptionRepository {
  constructor(private readonly db: Database) {}

  async add(event: string, url: string): Promise<void> {
    // INSERT OR IGNORE — idempotent; UNIQUE(event, url)
  }

  async findByEvent(event: string): Promise<Subscription[]> {
    // SELECT id, event, url, created_at WHERE event = ?
    // Return Subscription[], never raw row objects
  }

  async remove(event: string, url: string): Promise<void> {
    // DELETE WHERE event = ? AND url = ?
  }
}
```

### Domain type (not a DB row)

```typescript
// src/types.ts
export interface Subscription {
  readonly id: string;
  readonly event: string;
  readonly url: string;
  readonly createdAt: number;
}
```

Map raw DB columns (`created_at`) to camelCase domain fields (`createdAt`) **inside** the repository. The service never sees snake_case.

---

## 2. Service layer — `src/services/`

Responsible for **business logic and orchestration**. Calls repositories for data and broker modules for messaging. Never touches `req`, `res`, raw SQL, or AMQP channel primitives directly.

### Rules

- One service class per domain capability. This project has one: `WebhookService`.
- Constructor accepts repository and broker dependencies via injection (interfaces, not concrete classes).
- Business rules live here: fan-out logic, validation of event names, guard against empty subscriber lists, deciding when to publish vs skip.
- Raise `ServiceError extends WebhookError` for domain-rule violations (distinct from `RepositoryError`).
- A service method maps to one user-facing operation (`register`, `emit`). Do not create helper methods that are only meaningful in DB or HTTP terms.

### Interface contract

```typescript
// src/services/webhook.service.ts

export interface IWebhookService {
  register(event: string, url: string): Promise<void>;
  emit(event: string, payload: unknown): Promise<void>;
}

export class WebhookService implements IWebhookService {
  constructor(
    private readonly subscriptions: ISubscriptionRepository,
    private readonly publisher: IPublisher,
  ) {}

  async register(event: string, url: string): Promise<void> {
    // validate event name format (non-empty, no spaces)
    // validate url (must be http/https)
    // delegate persistence to repository
    await this.subscriptions.add(event, url);
  }

  async emit(event: string, payload: unknown): Promise<void> {
    // look up subscribers — business decision: skip silently if none
    const subscribers = await this.subscriptions.findByEvent(event);
    if (subscribers.length === 0) return;

    // fan-out: one publish per subscriber
    await Promise.all(
      subscribers.map((sub) =>
        this.publisher.publish(event, sub.url, payload),
      ),
    );
  }
}
```

### What services must NOT do

```typescript
// ❌ touching req/res
async register(req: Request, res: Response) { ... }

// ❌ raw SQL
await db.run('INSERT INTO subscriptions ...');

// ❌ AMQP primitives
channel.publish('webhooks.x', ...);

// ❌ HTTP response shaping
return { status: 201, body: { ok: true } };
```

---

## 3. Controller layer — `src/controllers/` and `examples/*/routes/`

Responsible for **HTTP request/response only**. Parses input, delegates to service, formats response. Zero business logic.

### Rules

- One controller class per resource. Consumer apps have one: `WebhookController` (handles `POST /hook`).
- Controllers call exactly one service method per route handler. If you need to call two services, that orchestration belongs in the service layer.
- Validate request shape (required fields present, correct types) **in the controller** before calling the service. Use a typed guard or schema parser (`zod` / manual). Do not let `unknown` bleed into service methods.
- Map service/repository errors to HTTP status codes here — not in the service. The service raises typed errors; the controller catches and decides the status code.
- Never embed SQL, broker calls, or business conditionals in a controller.

### Interface contract (consumer apps)

```typescript
// examples/consumer-1/controllers/webhook.controller.ts

export class WebhookController {
  constructor(private readonly webhookService: IInboundWebhookService) {}

  async handleIncoming(req: Request, res: Response): Promise<void> {
    // 1. Parse + validate shape
    const body = req.body as unknown;
    if (!isWebhookPayload(body)) {
      res.status(400).json({ error: 'Invalid payload shape' });
      return;
    }

    // 2. Delegate to service
    try {
      await this.webhookService.process(body);
      res.status(200).json({ received: true });
    } catch (err) {
      if (err instanceof ServiceError) {
        res.status(422).json({ error: err.message });
      } else {
        res.status(500).json({ error: 'Internal error' });
      }
    }
  }
}

// Route wiring (separate from controller class)
export function registerWebhookRoutes(
  router: Router,
  controller: WebhookController,
): void {
  router.post('/hook', (req, res) => controller.handleIncoming(req, res));
}
```

### What controllers must NOT do

```typescript
// ❌ business logic
if (payload.amount > 1000) { applyTax(); }

// ❌ direct DB access
const rows = await db.all('SELECT ...');

// ❌ broker calls
channel.publish(...);

// ❌ calling multiple services for orchestration
await orderService.validate(body);
await notificationService.send(body);
// ↑ wrap this in a service method instead
```

---

## 4. Dependency injection — wiring in `src/index.ts`

All three layers are wired together in `src/index.ts` (or an `examples/*/app.ts` for the demo apps). No layer instantiates its own dependencies.

```typescript
// src/index.ts  (inside createWebhooks, after DB + broker are ready)

const subscriptionRepo = new SubscriptionRepository(db);
const publisher        = new Publisher(publishChannel);
const webhookService   = new WebhookService(subscriptionRepo, publisher);

// The public instance delegates to the service — no logic in index.ts
return {
  register: (event, url)     => webhookService.register(event, url),
  emit:     (event, payload) => webhookService.emit(event, payload),
  close:    ()               => shutdown(worker, publishChannel, consumeChannel, connection, db),
};
```

```typescript
// examples/consumer-1/app.ts

const controller = new WebhookController(new InboundWebhookService());
const router     = Router();
registerWebhookRoutes(router, controller);
app.use(router);
```

---

## 5. File structure after applying the pattern

```
src/
  repositories/
    subscription.repository.ts    # ISubscriptionRepository + SubscriptionRepository
    index.ts                      # re-exports interfaces only
  services/
    webhook.service.ts            # IWebhookService + WebhookService
    index.ts                      # re-exports interfaces only
  controllers/                    # (library has no HTTP server — leave empty or omit)
  index.ts                        # wires repo → service → public API
  types.ts                        # Subscription, JobHeaders, WebhooksConfig, error classes
  db.ts
  broker.ts
  publisher.ts
  worker.ts
  delivery.ts
  backoff.ts

examples/
  consumer-1/
    controllers/
      webhook.controller.ts       # WebhookController
      index.ts
    services/
      inbound-webhook.service.ts  # InboundWebhookService (logs, acks, stores if needed)
      index.ts
    app.ts                        # wires controller, starts Express
    Dockerfile
  consumer-2/
    controllers/
      webhook.controller.ts
    services/
      inbound-webhook.service.ts
    app.ts
    Dockerfile
  producer/
    index.ts
    Dockerfile
```

---

## 6. Naming conventions per layer

| Layer | Class suffix | File suffix | Interface prefix |
|---|---|---|---|
| Repository | `Repository` | `.repository.ts` | `I` → `ISubscriptionRepository` |
| Service | `Service` | `.service.ts` | `I` → `IWebhookService` |
| Controller | `Controller` | `.controller.ts` | `I` → `IWebhookController` |
| Route wiring | — | `.routes.ts` | — |

Constructor parameter names match the interface: `private readonly subscriptions: ISubscriptionRepository` (not `repo`, not `db`).

---

## 7. Cross-layer error taxonomy

```typescript
// src/types.ts

export class WebhookError extends Error {
  constructor(message: string, public readonly code: string) {
    super(message);
    this.name = 'WebhookError';
  }
}

export class RepositoryError extends WebhookError {
  constructor(message: string) { super(message, 'REPOSITORY_ERROR'); }
}

export class ServiceError extends WebhookError {
  constructor(message: string) { super(message, 'SERVICE_ERROR'); }
}

export class ValidationError extends ServiceError {
  constructor(field: string, reason: string) {
    super(`Validation failed on '${field}': ${reason}`);
    this.name   = 'ValidationError';
  }
}
```

- `RepositoryError` — thrown only inside `src/repositories/`. Wraps driver errors.
- `ServiceError` — thrown only inside `src/services/`. Signals a violated business rule.
- `ValidationError` — thrown in service or controller input-check, caught in controller for 400/422 responses.
- Controllers catch all three and map to HTTP status codes. Nothing above the controller catches them.

---

## 8. Checklist before marking a layer complete

**Repository:**
- [ ] No business logic (no `if` branches that encode rules)
- [ ] Returns domain types (camelCase), not raw driver rows
- [ ] All errors are `RepositoryError`
- [ ] Constructor takes `Database`, not a path or config

**Service:**
- [ ] No `req`, `res`, `Request`, `Response` imports
- [ ] No raw SQL strings
- [ ] No AMQP channel or `amqplib` imports
- [ ] All errors are `ServiceError` (or subtypes)
- [ ] Dependencies are interface types, not concrete classes

**Controller:**
- [ ] Calls exactly one service method per handler
- [ ] Validates request shape before calling service
- [ ] Maps typed errors to HTTP status codes
- [ ] No SQL, no broker, no business conditionals
