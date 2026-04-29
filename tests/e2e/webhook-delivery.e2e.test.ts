jest.setTimeout(180_000);

import { createWebhooks, type WebhooksInstance } from '../../src';
import { startTestBroker, type TestBroker } from './helpers/test-broker';
import { startTestConsumer, type TestConsumer } from './helpers/test-consumer';
import { waitFor } from './helpers/wait-for';

describe('webhook delivery e2e', () => {
  let broker: TestBroker;
  let webhooks: WebhooksInstance;
  let consumer1: TestConsumer;
  let consumer2: TestConsumer;

  beforeAll(async () => {
    broker = await startTestBroker();
    consumer1 = await startTestConsumer();
    consumer2 = await startTestConsumer();

    webhooks = createWebhooks({
      rabbitmqUrl: broker.amqpUrl,
      dbPath: './.tmp-e2e-delivery.db',
      startupBufferLimit: 100,
      reconnectBackoffMs: 100,
    });

    await webhooks.register('order.created', consumer1.url);
    await webhooks.register('order.shipped', consumer2.url);
  });

  afterAll(async () => {
    if (typeof webhooks !== 'undefined') await webhooks.close();
    if (typeof consumer1 !== 'undefined') await consumer1.stop();
    if (typeof consumer2 !== 'undefined') await consumer2.stop();
    if (typeof broker !== 'undefined') await broker.stop();
  });

  it('routes each event to the intended consumer only', async () => {
    await webhooks.emit('order.created', { event: 'order.created', data: { orderId: 1 } });
    await waitFor(() => consumer1.received.length >= 1);

    await webhooks.emit('order.shipped', { event: 'order.shipped', data: { orderId: 1 } });
    await waitFor(() => consumer2.received.length >= 1);

    expect(consumer1.received.some((item) => item.event === 'order.created')).toBe(true);
    expect(consumer1.received.some((item) => item.event === 'order.shipped')).toBe(false);
    expect(consumer2.received.some((item) => item.event === 'order.created')).toBe(false);
    expect(consumer2.received.some((item) => item.event === 'order.shipped')).toBe(true);
  });
});
