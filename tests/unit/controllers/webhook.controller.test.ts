import express from 'express';
import request from 'supertest';
import { WebhookController } from '../../../examples/consumer-1/controllers/webhook.controller';
import { ServiceError } from '../../../src/types';
import type { IInboundWebhookService } from '../../../examples/consumer-1/services/inbound-webhook.service';

const makeService = (): jest.Mocked<IInboundWebhookService> => ({
  process: jest.fn().mockResolvedValue(undefined),
});

describe('WebhookController', () => {
  it('handles valid payload', async () => {
    const service = makeService();
    const controller = new WebhookController(service);
    const app = express();
    app.use(express.json());
    app.post('/hook', (req, res) => {
      void controller.handleIncoming(req, res);
    });

    const response = await request(app).post('/hook').send({ event: 'order.created', data: { id: 1 } });
    expect(response.status).toBe(200);
  });

  it('returns 422 on service error', async () => {
    const service = makeService();
    service.process.mockRejectedValue(new ServiceError('bad'));
    const controller = new WebhookController(service);
    const app = express();
    app.use(express.json());
    app.post('/hook', (req, res) => {
      void controller.handleIncoming(req, res);
    });

    const response = await request(app).post('/hook').send({ event: 'order.created', data: { id: 1 } });
    expect(response.status).toBe(422);
  });
});
