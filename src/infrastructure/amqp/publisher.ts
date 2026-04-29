import { randomUUID } from 'node:crypto';
import type { ConfirmChannel, Options } from 'amqplib';
import type { IPublisher } from '../../contracts/publisher';
import type { JobHeaders } from '../../core/types';
import { BrokerError } from '../../core/errors';
import { EXCHANGE_WEBHOOKS, waitForDrain } from './broker';

export class Publisher implements IPublisher {
  public constructor(
    private readonly channel: ConfirmChannel,
    private readonly maxRetries: number,
  ) {}

  public async publish(event: string, url: string, payload: unknown): Promise<void> {
    const headers: JobHeaders = {
      url,
      attempts: 0,
      maxRetries: this.maxRetries,
      originalEvent: event,
    };

    const options: Options.Publish = {
      persistent: true,
      contentType: 'application/json',
      messageId: randomUUID(),
      headers,
    };

    const ok = this.channel.publish(EXCHANGE_WEBHOOKS, event, Buffer.from(JSON.stringify(payload)), options);
    if (!ok) {
      await waitForDrain(this.channel);
    }

    try {
      await this.channel.waitForConfirms();
    } catch (error) {
      throw new BrokerError('Failed waiting for publisher confirms', error);
    }
  }
}
