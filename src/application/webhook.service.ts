import type { IPublisher } from '../contracts/publisher';
import type { ISubscriptionRepository } from '../contracts/subscription-repository';
import type { IWebhookService } from '../contracts/webhook-service';
import { ServiceError, ValidationError } from '../core/errors';

export class WebhookService implements IWebhookService {
  public constructor(
    private readonly subscriptions: ISubscriptionRepository,
    private readonly publisher: IPublisher,
  ) {}

  public async register(event: string, url: string): Promise<void> {
    if (event.trim().length === 0 || event.includes(' ')) {
      throw new ValidationError('Event must be non-empty and cannot contain spaces');
    }

    let parsed: URL;
    try {
      parsed = new URL(url);
    } catch {
      throw new ValidationError('Invalid URL format');
    }

    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
      throw new ValidationError('URL must use http or https protocol');
    }

    try {
      await this.subscriptions.add(event, url);
    } catch (error) {
      throw new ServiceError('Failed to register subscription', error);
    }
  }

  public async emit(event: string, payload: unknown): Promise<void> {
    if (event.trim().length === 0) {
      throw new ValidationError('Event must be non-empty');
    }

    let subscribers;
    try {
      subscribers = await this.subscriptions.findByEvent(event);
    } catch (error) {
      throw new ServiceError('Failed to fetch subscriptions', error);
    }

    if (subscribers.length === 0) {
      return;
    }

    await Promise.all(subscribers.map(async (sub) => this.publisher.publish(event, sub.url, payload)));
  }
}
