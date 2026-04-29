import { randomUUID } from 'node:crypto';
import type { ISubscriptionRepository } from '../../../contracts/subscription-repository';
import type { Subscription } from '../../../core/types';
import { RepositoryError } from '../../../core/errors';
import type { SqliteDatabase } from './db';

interface SubscriptionRow {
  readonly id: string;
  readonly event: string;
  readonly url: string;
  readonly created_at: number;
}

export class SubscriptionRepository implements ISubscriptionRepository {
  public constructor(private readonly db: SqliteDatabase) {}

  public async add(event: string, url: string): Promise<void> {
    try {
      const now = Date.now();
      await this.db.run('BEGIN');
      await this.db.run(
        'INSERT OR IGNORE INTO subscriptions (id, event, url, created_at) VALUES (?, ?, ?, ?)',
        [randomUUID(), event, url, now],
      );
      await this.db.run('COMMIT');
    } catch (error) {
      await this.safeRollback();
      throw new RepositoryError('Failed to add subscription', error);
    }
  }

  public async findByEvent(event: string): Promise<Subscription[]> {
    try {
      const rows = await this.db.all<SubscriptionRow>(
        'SELECT id, event, url, created_at FROM subscriptions WHERE event = ?',
        [event],
      );
      return rows.map((row) => ({
        id: row.id,
        event: row.event,
        url: row.url,
        createdAt: row.created_at,
      }));
    } catch (error) {
      throw new RepositoryError('Failed to find subscriptions by event', error);
    }
  }

  public async remove(event: string, url: string): Promise<void> {
    try {
      await this.db.run('BEGIN');
      await this.db.run('DELETE FROM subscriptions WHERE event = ? AND url = ?', [event, url]);
      await this.db.run('COMMIT');
    } catch (error) {
      await this.safeRollback();
      throw new RepositoryError('Failed to remove subscription', error);
    }
  }

  private async safeRollback(): Promise<void> {
    try {
      await this.db.run('ROLLBACK');
    } catch {
      return;
    }
  }
}
