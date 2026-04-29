jest.setTimeout(180_000);

import { createWebhooks, type WebhooksInstance } from '../../src';
import { startTestBroker, type TestBroker } from './helpers/test-broker';
import { startTestConsumer, type TestConsumer } from './helpers/test-consumer';
import { waitFor } from './helpers/wait-for';

describe('retry behavior e2e', () => {
  let broker: TestBroker;
  let webhooks: WebhooksInstance;
  let consumer: TestConsumer;

  beforeAll(async () => {
    broker = await startTestBroker();
    consumer = await startTestConsumer({ failForFirstAttempts: 1 });

    webhooks = createWebhooks({
      rabbitmqUrl: broker.amqpUrl,
      dbPath: './.tmp-e2e-retry.db',
      maxRetries: 2,
      reconnectBackoffMs: 100,
    });

    await webhooks.register('retry.event', consumer.url);
  });

  afterAll(async () => {
    if (typeof webhooks !== 'undefined') await webhooks.close();
    if (typeof consumer !== 'undefined') await consumer.stop();
    if (typeof broker !== 'undefined') await broker.stop();
  });

  it('retries failed deliveries and eventually succeeds', async () => {
    await webhooks.emit('retry.event', { event: 'retry.event', data: { orderId: 9 } });
    await waitFor(() => consumer.attempts.count >= 2, 40_000);

    expect(consumer.attempts.count).toBe(2);
    expect(consumer.received[0]?.event).toBe('retry.event');
  });
});
