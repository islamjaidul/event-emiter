import type { IPublisher } from '../../../src/contracts/publisher';
import type { ISubscriptionRepository } from '../../../src/contracts/subscription-repository';
import { WebhookService } from '../../../src/application/webhook.service';
import { ValidationError } from '../../../src/core/errors';

const makeRepo = (): jest.Mocked<ISubscriptionRepository> => ({
  add: jest.fn().mockResolvedValue(undefined),
  findByEvent: jest.fn().mockResolvedValue([]),
  remove: jest.fn().mockResolvedValue(undefined),
});

const makePublisher = (): jest.Mocked<IPublisher> => ({
  publish: jest.fn().mockResolvedValue(undefined),
});

describe('WebhookService', () => {
  it('register validates', async () => {
    const service = new WebhookService(makeRepo(), makePublisher());
    await expect(service.register('', 'http://x')).rejects.toBeInstanceOf(ValidationError);
  });

  it('emit fanout', async () => {
    const repo = makeRepo();
    const publisher = makePublisher();
    repo.findByEvent.mockResolvedValue([
      { id: '1', event: 'order.created', url: 'http://a/hook', createdAt: Date.now() },
      { id: '2', event: 'order.created', url: 'http://b/hook', createdAt: Date.now() },
    ]);
    const service = new WebhookService(repo, publisher);
    await service.emit('order.created', { orderId: 1 });
    expect(publisher.publish).toHaveBeenCalledTimes(2);
  });
});
