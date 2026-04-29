import type { Channel, ConfirmChannel, ConsumeMessage } from 'amqplib';
import { Worker } from '../../src/worker';

jest.mock('../../src/delivery', () => ({
  deliverHttp: jest.fn(async () => ({ success: true, statusCode: 200 })),
}));

const message: ConsumeMessage = {
  content: Buffer.from(JSON.stringify({ id: 1 })),
  fields: {} as never,
  properties: {
    headers: { url: 'http://x/hook', attempts: 0, maxRetries: 3, originalEvent: 'order.created' },
    contentType: 'application/json',
    messageId: 'msg1',
  } as never,
};

describe('Worker', () => {
  it('acks success path', async () => {
    let handler: ((msg: ConsumeMessage | null) => void) | null = null;
    const consumeChannel = {
      consume: jest.fn(async (_q, cb: (msg: ConsumeMessage | null) => void) => {
        handler = cb;
        return { consumerTag: 'ctag' };
      }),
      ack: jest.fn(),
      nack: jest.fn(),
      cancel: jest.fn(),
    } as unknown as Channel;

    const publishChannel = {
      publish: jest.fn(() => true),
      waitForConfirms: jest.fn(async () => undefined),
    } as unknown as ConfirmChannel;

    const worker = new Worker(consumeChannel, publishChannel, { timeoutMs: 1000 });
    await worker.start();
    if (handler !== null) {
      (handler as (msg: ConsumeMessage | null) => void)(message);
    }
    await new Promise((r) => setTimeout(r, 10));
    expect((consumeChannel.ack as jest.Mock).mock.calls.length).toBe(1);
  });
});
