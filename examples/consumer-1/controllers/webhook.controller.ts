import type { Request, Response } from 'express';
import type { IInboundWebhookService, InboundPayload } from '../services/inbound-webhook.service';
import { ServiceError } from '../../../src/types';

function isInboundPayload(value: unknown): value is InboundPayload {
  if (typeof value !== 'object' || value === null) {
    return false;
  }

  const asRecord = value as Record<string, unknown>;
  return typeof asRecord.event === 'string' && 'data' in asRecord;
}

export class WebhookController {
  public constructor(private readonly webhookService: IInboundWebhookService) {}

  public async handleIncoming(req: Request, res: Response): Promise<void> {
    if (!isInboundPayload(req.body)) {
      res.status(400).json({ error: 'Invalid payload shape' });
      return;
    }

    try {
      await this.webhookService.process(req.body);
      res.status(200).json({ received: true });
    } catch (error) {
      if (error instanceof ServiceError) {
        res.status(422).json({ error: error.message });
        return;
      }
      res.status(500).json({ error: 'Internal error' });
    }
  }
}
