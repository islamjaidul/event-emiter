import express from 'express';
import { WebhookController } from './controllers/webhook.controller';
import { InboundWebhookService } from './services/inbound-webhook.service';

const app = express();
app.use(express.json());

const controller = new WebhookController(new InboundWebhookService());
app.post('/hook', (req, res) => {
  void controller.handleIncoming(req, res);
});

const server = app.listen(3002, () => {
  console.log('consumer-2 listening on :3002');
});

process.on('SIGTERM', () => {
  server.close(() => process.exit(0));
});
