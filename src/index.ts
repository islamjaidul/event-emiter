export { createWebhooks } from './composition/create-webhooks';

export type { WebhooksConfig, WebhooksInstance, JobHeaders, Subscription } from './core/types';
export { WebhookError, ValidationError, RepositoryError, ServiceError, BrokerError } from './core/errors';
