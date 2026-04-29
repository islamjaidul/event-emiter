import type { Request, Response } from 'express';
import type { IInboundWebhookService } from '../services/inbound-webhook.service';
import { ServiceError } from '../../../src/core/errors';

export class WebhookController {
  public constructor(private readonly webhookService: IInboundWebhookService) {}

  public async handleIncoming(req: Request, res: Response): Promise<void> {
    try {
      await this.webhookService.process(req.body as unknown);
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
