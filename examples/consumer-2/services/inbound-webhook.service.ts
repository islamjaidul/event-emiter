export type InboundPayload = unknown;

export interface IInboundWebhookService {
  process(payload: InboundPayload): Promise<void>;
}

export class InboundWebhookService implements IInboundWebhookService {
  public async process(payload: InboundPayload): Promise<void> {
    console.log('[consumer-2] received:', payload);
  }
}
