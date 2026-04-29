jest.setTimeout(180_000);

import { createWebhooks, type WebhooksInstance } from '../../src';
import { startTestBroker, type TestBroker } from './helpers/test-broker';
import { startTestConsumer, type TestConsumer } from './helpers/test-consumer';
import { waitFor } from './helpers/wait-for';

describe('graceful shutdown e2e', () => {
  let broker: TestBroker;
  let webhooks: WebhooksInstance;
  let consumer: TestConsumer;

  beforeAll(async () => {
    broker = await startTestBroker();
    consumer = await startTestConsumer();
    webhooks = createWebhooks({
      rabbitmqUrl: broker.amqpUrl,
      dbPath: './.tmp-e2e-close.db',
      reconnectBackoffMs: 100,
    });
    await webhooks.register('close.event', consumer.url);
  });

  afterAll(async () => {
    if (typeof webhooks !== 'undefined') await webhooks.close();
    if (typeof consumer !== 'undefined') await consumer.stop();
    if (typeof broker !== 'undefined') await broker.stop();
  });

  it('closes cleanly and idempotently', async () => {
    await webhooks.emit('close.event', { event: 'close.event', data: { x: 1 } });
    await waitFor(() => consumer.received.length >= 1);

    await webhooks.close();
    await webhooks.close();
  });
});
