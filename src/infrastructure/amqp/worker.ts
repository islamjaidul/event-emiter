import { once } from 'node:events';
import type { Channel, ConfirmChannel, ConsumeMessage, Options } from 'amqplib';
import type { JobHeaders } from '../../core/types';
import { BrokerError } from '../../core/errors';
import { nextDelayMs } from '../../core/backoff';
import { deliverHttp } from '../http/delivery';
import {
  EXCHANGE_DLX,
  EXCHANGE_RETRY,
  QUEUE_WEBHOOKS,
  ensureMessage,
  waitForDrain,
} from './broker';

interface WorkerConfig {
  readonly timeoutMs: number;
}

export class Worker {
  private consumerTag: string | null = null;
  private inFlight = new Set<Promise<void>>();

  public constructor(
    private readonly consumeChannel: Channel,
    private readonly publishChannel: ConfirmChannel,
    private readonly config: WorkerConfig,
  ) {}

  public async start(): Promise<void> {
    const result = await this.consumeChannel.consume(
      QUEUE_WEBHOOKS,
      (msg) => {
        const task = this.handleMessage(msg)
          .catch(() => undefined)
          .finally(() => this.inFlight.delete(task));
        this.inFlight.add(task);
      },
      { noAck: false },
    );

    this.consumerTag = result.consumerTag;
  }

  public async stop(): Promise<void> {
    if (this.consumerTag !== null) {
      await this.consumeChannel.cancel(this.consumerTag);
      this.consumerTag = null;
    }

    if (this.inFlight.size > 0) {
      await Promise.all(Array.from(this.inFlight));
    }
  }

  private async handleMessage(raw: ConsumeMessage | null): Promise<void> {
    const msg = ensureMessage(raw);

    try {
      const headers = this.parseHeaders(msg.properties.headers);
      const payload = JSON.parse(msg.content.toString('utf8')) as unknown;
      const result = await deliverHttp(headers.url, payload, this.config.timeoutMs);

      if (result.success) {
        this.consumeChannel.ack(msg);
        return;
      }

      if (headers.attempts < headers.maxRetries) {
        await this.republishRetry(msg, headers);
        this.consumeChannel.ack(msg);
        return;
      }

      await this.republishDlx(msg, headers);
      this.consumeChannel.ack(msg);
    } catch (error) {
      this.consumeChannel.nack(msg, false, false);
      throw new BrokerError('Unhandled worker message failure', error);
    }
  }

  private parseHeaders(headers: unknown): JobHeaders {
    const parsed = headers as Partial<Record<string, unknown>>;
    const url = parsed.url;
    const attempts = parsed.attempts;
    const maxRetries = parsed.maxRetries;
    const originalEvent = parsed.originalEvent;

    if (
      typeof url !== 'string' ||
      typeof attempts !== 'number' ||
      typeof maxRetries !== 'number' ||
      typeof originalEvent !== 'string'
    ) {
      throw new BrokerError('Invalid message headers');
    }

    return { url, attempts, maxRetries, originalEvent };
  }

  private async republishRetry(msg: ConsumeMessage, headers: JobHeaders): Promise<void> {
    const nextAttempt = headers.attempts + 1;
    const exchangeHeaders: Options.Publish['headers'] = {
      ...msg.properties.headers,
      attempts: nextAttempt,
      'x-delay': nextDelayMs(nextAttempt),
    };

    const ok = this.publishChannel.publish(EXCHANGE_RETRY, headers.originalEvent, msg.content, {
      persistent: true,
      contentType: msg.properties.contentType,
      messageId: msg.properties.messageId,
      headers: exchangeHeaders,
    });

    if (!ok) {
      await waitForDrain(this.publishChannel);
    }

    await this.publishChannel.waitForConfirms();
  }

  private async republishDlx(msg: ConsumeMessage, headers: JobHeaders): Promise<void> {
    const ok = this.publishChannel.publish(EXCHANGE_DLX, headers.originalEvent, msg.content, {
      persistent: true,
      contentType: msg.properties.contentType,
      messageId: msg.properties.messageId,
      headers: msg.properties.headers,
    });

    if (!ok) {
      await once(this.publishChannel, 'drain');
    }

    await this.publishChannel.waitForConfirms();
  }
}
