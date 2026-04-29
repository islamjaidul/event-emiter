import { ServiceError } from '../../../src/types';

export interface InboundPayload {
  readonly event: string;
  readonly data: unknown;
}

export interface IInboundWebhookService {
  process(payload: InboundPayload): Promise<void>;
}

export class InboundWebhookService implements IInboundWebhookService {
  public async process(payload: InboundPayload): Promise<void> {
    if (payload.event.trim().length === 0) {
      throw new ServiceError('event is required');
    }

    console.log('[consumer-1] received:', payload);
  }
}
