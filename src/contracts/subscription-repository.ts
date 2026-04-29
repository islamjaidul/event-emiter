import type { Subscription } from '../core/types';

export interface ISubscriptionRepository {
  add(event: string, url: string): Promise<void>;
  findByEvent(event: string): Promise<Subscription[]>;
  remove(event: string, url: string): Promise<void>;
}
