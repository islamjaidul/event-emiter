import Database from 'better-sqlite3';
import { SubscriptionRepository } from '../../../src/repositories/subscription.repository';

class BetterSqliteAdapter {
  public constructor(private readonly db: Database.Database) {}
  public async run(sql: string, params: ReadonlyArray<unknown> = []): Promise<void> {
    this.db.prepare(sql).run(...params);
  }
  public async all<T>(sql: string, params: ReadonlyArray<unknown> = []): Promise<T[]> {
    return this.db.prepare(sql).all(...params) as T[];
  }
  public async close(): Promise<void> {
    this.db.close();
  }
}

describe('SubscriptionRepository', () => {
  let db: Database.Database;
  let repo: SubscriptionRepository;

  beforeEach(() => {
    db = new Database(':memory:');
    db.exec(`
      CREATE TABLE subscriptions (
        id TEXT PRIMARY KEY,
        event TEXT NOT NULL,
        url TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        UNIQUE(event, url)
      )
    `);
    repo = new SubscriptionRepository(new BetterSqliteAdapter(db));
  });

  afterEach(() => db.close());

  it('adds and queries subscriptions', async () => {
    await repo.add('order.created', 'http://localhost:3001/hook');
    const subs = await repo.findByEvent('order.created');
    expect(subs).toHaveLength(1);
    expect(subs[0]?.url).toBe('http://localhost:3001/hook');
  });

  it('is idempotent', async () => {
    await repo.add('order.created', 'http://localhost:3001/hook');
    await repo.add('order.created', 'http://localhost:3001/hook');
    const subs = await repo.findByEvent('order.created');
    expect(subs).toHaveLength(1);
  });
});
