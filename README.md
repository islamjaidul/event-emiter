# webhooks-ts

Reliable webhook delivery library for Node.js using RabbitMQ.

## Run with Docker

```bash
docker-compose up --build
```

RabbitMQ UI: [http://localhost:15672](http://localhost:15672) (`guest` / `guest`)

## Guarantees

- `emit()` returns only after RabbitMQ publisher confirms.
- Persistent messages + durable queues.
- Worker uses manual ack after HTTP 2xx.
- Retry with backoff; exhausted retries go to `webhooks.dlq`.

## Tradeoff

The delivery worker runs in-process with the producer library. A separate worker container would scale independently in a larger deployment.
