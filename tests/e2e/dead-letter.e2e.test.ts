jest.setTimeout(180_000);

import amqp from 'amqplib';
import { createWebhooks, type WebhooksInstance } from '../../src';
import { QUEUE_DLQ } from '../../src/infrastructure/amqp/broker';
import { startTestBroker, type TestBroker } from './helpers/test-broker';
import { startTestConsumer, type TestConsumer } from './helpers/test-consumer';
import { waitFor } from './helpers/wait-for';

describe('dead-letter e2e', () => {
  let broker: TestBroker;
  let webhooks: WebhooksInstance;
  let consumer: TestConsumer;

  beforeAll(async () => {
    broker = await startTestBroker();
    consumer = await startTestConsumer({ failForFirstAttempts: 100 });
    webhooks = createWebhooks({
      rabbitmqUrl: broker.amqpUrl,
      dbPath: './.tmp-e2e-dlq.db',
      maxRetries: 0,
      reconnectBackoffMs: 100,
    });
    await webhooks.register('dead.event', consumer.url);
  });

  afterAll(async () => {
    if (typeof webhooks !== 'undefined') await webhooks.close();
    if (typeof consumer !== 'undefined') await consumer.stop();
    if (typeof broker !== 'undefined') await broker.stop();
  });

  it('routes permanently failed messages to dlq', async () => {
    await webhooks.emit('dead.event', { event: 'dead.event', data: { y: 1 } });

    await waitFor(async () => {
      const connection = await amqp.connect(broker.amqpUrl);
      const channel = await connection.createChannel();
      const state = await channel.checkQueue(QUEUE_DLQ);
      await channel.close();
      await connection.close();
      return state.messageCount >= 1;
    }, 20_000);
  });
});
