import { once } from 'node:events';
import amqp, { type Channel, type ChannelModel, type ConfirmChannel, type ConsumeMessage } from 'amqplib';
import { BrokerError } from './types';

export const EXCHANGE_WEBHOOKS = 'webhooks.x';
export const EXCHANGE_RETRY = 'webhooks.retry';
export const EXCHANGE_DLX = 'webhooks.dlx';
export const QUEUE_WEBHOOKS = 'webhooks.q';
export const QUEUE_DLQ = 'webhooks.dlq';

export interface BrokerChannels {
  readonly connection: ChannelModel;
  readonly publishChannel: ConfirmChannel;
  readonly consumeChannel: Channel;
}

export async function waitForDrain(channel: Channel | ConfirmChannel): Promise<void> {
  await once(channel, 'drain');
}

export async function connectBroker(rabbitmqUrl: string, concurrency: number): Promise<BrokerChannels> {
  try {
    const connection = await amqp.connect(rabbitmqUrl);
    const publishChannel = await connection.createConfirmChannel();
    const consumeChannel = await connection.createChannel();

    await assertTopology(publishChannel);
    await consumeChannel.prefetch(concurrency);

    return { connection, publishChannel, consumeChannel };
  } catch (error) {
    throw new BrokerError('Failed to connect to RabbitMQ', error);
  }
}

async function assertTopology(channel: ConfirmChannel): Promise<void> {
  await channel.assertExchange(EXCHANGE_WEBHOOKS, 'topic', { durable: true });
  await channel.assertExchange(EXCHANGE_RETRY, 'topic', { durable: true });
  await channel.assertExchange(EXCHANGE_DLX, 'topic', { durable: true });

  await channel.assertQueue(QUEUE_WEBHOOKS, { durable: true });
  await channel.assertQueue(QUEUE_DLQ, { durable: true });

  await channel.bindQueue(QUEUE_WEBHOOKS, EXCHANGE_WEBHOOKS, '#');
  await channel.bindQueue(QUEUE_WEBHOOKS, EXCHANGE_RETRY, '#');
  await channel.bindQueue(QUEUE_DLQ, EXCHANGE_DLX, '#');
}

export function ensureMessage(msg: ConsumeMessage | null): ConsumeMessage {
  if (msg === null) {
    throw new BrokerError('Received null AMQP message');
  }
  return msg;
}
