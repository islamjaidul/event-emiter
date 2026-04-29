import sqlite3 from 'sqlite3';
import { BrokerError } from '../../../core/errors';

export interface SqliteDatabase {
  run(sql: string, params?: ReadonlyArray<unknown>): Promise<void>;
  all<T>(sql: string, params?: ReadonlyArray<unknown>): Promise<T[]>;
  close(): Promise<void>;
}

class NodeSqliteDatabase implements SqliteDatabase {
  public constructor(private readonly db: sqlite3.Database) {}

  public run(sql: string, params: ReadonlyArray<unknown> = []): Promise<void> {
    return new Promise((resolve, reject) => {
      this.db.run(sql, params as never[], (err) => {
        if (err) {
          reject(new BrokerError(`SQLite run failed: ${sql}`, err));
          return;
        }
        resolve();
      });
    });
  }

  public all<T>(sql: string, params: ReadonlyArray<unknown> = []): Promise<T[]> {
    return new Promise((resolve, reject) => {
      this.db.all(sql, params as never[], (err, rows) => {
        if (err) {
          reject(new BrokerError(`SQLite query failed: ${sql}`, err));
          return;
        }
        resolve(rows as T[]);
      });
    });
  }

  public close(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.db.close((err) => {
        if (err) {
          reject(new BrokerError('SQLite close failed', err));
          return;
        }
        resolve();
      });
    });
  }
}

export async function openDb(path: string): Promise<SqliteDatabase> {
  const db = await new Promise<sqlite3.Database>((resolve, reject) => {
    const instance = new sqlite3.Database(path, (err) => {
      if (err) {
        reject(new BrokerError('SQLite open failed', err));
        return;
      }
      resolve(instance);
    });
  });

  const wrapped = new NodeSqliteDatabase(db);
  await wrapped.run('PRAGMA journal_mode = WAL');
  await wrapped.run('PRAGMA synchronous = FULL');
  await wrapped.run('PRAGMA busy_timeout = 5000');

  await wrapped.run(`
    CREATE TABLE IF NOT EXISTS subscriptions (
      id TEXT PRIMARY KEY,
      event TEXT NOT NULL,
      url TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      UNIQUE(event, url)
    )
  `);
  await wrapped.run('CREATE INDEX IF NOT EXISTS idx_subscriptions_event ON subscriptions(event)');

  return wrapped;
}
