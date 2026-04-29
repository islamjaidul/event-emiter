import { createWebhooks } from '../../src';

const webhooks = createWebhooks({
  rabbitmqUrl: process.env.RABBITMQ_URL ?? 'amqp://guest:guest@rabbitmq:5672',
  dbPath: process.env.DB_PATH ?? '/data/webhooks.db',
});

let counter = 1;

async function bootstrap(): Promise<void> {
  await webhooks.register('order.created', 'http://consumer-1:3001/hook');
  await webhooks.register('order.created', 'http://consumer-2:3002/hook');
  await webhooks.register('order.shipped', 'http://consumer-1:3001/hook');

  setInterval(() => {
    const payload = { orderId: counter++, createdAt: Date.now() };
    void webhooks.emit('order.created', payload);
    void webhooks.emit('order.shipped', { ...payload, shippedAt: Date.now() });
  }, 5000).unref();
}

void bootstrap();

process.on('SIGTERM', async () => {
  await webhooks.close();
  process.exit(0);
});
