import type { Channel, ChannelModel, ConfirmChannel } from 'amqplib';
import type { WebhooksConfig, WebhooksInstance } from '../core/types';
import { ServiceError } from '../core/errors';
import { WebhookService } from '../application/webhook.service';
import { connectBroker } from '../infrastructure/amqp/broker';
import { Publisher } from '../infrastructure/amqp/publisher';
import { Worker } from '../infrastructure/amqp/worker';
import { openDb, type SqliteDatabase } from '../infrastructure/persistence/sqlite/db';
import { SubscriptionRepository } from '../infrastructure/persistence/sqlite/subscription.repository';

interface RuntimeState {
  db: SqliteDatabase | null;
  connection: ChannelModel | null;
  publishChannel: ConfirmChannel | null;
  consumeChannel: Channel | null;
  worker: Worker | null;
}

interface BufferedOperation {
  readonly run: () => Promise<void>;
  readonly resolve: () => void;
  readonly reject: (error: unknown) => void;
}

export function createWebhooks(config: WebhooksConfig): WebhooksInstance {
  const merged = {
    dbPath: config.dbPath ?? './webhooks.db',
    maxRetries: config.maxRetries ?? 5,
    timeoutMs: config.timeoutMs ?? 10_000,
    concurrency: config.concurrency ?? 10,
    startupBufferLimit: config.startupBufferLimit ?? 1000,
    reconnectBackoffMs: config.reconnectBackoffMs ?? 1000,
  };

  const runtime: RuntimeState = {
    db: null,
    connection: null,
    publishChannel: null,
    consumeChannel: null,
    worker: null,
  };
  const startupBuffer: BufferedOperation[] = [];

  let ready = false;
  let closed = false;
  let closingPromise: Promise<void> | null = null;
  let reconnectTimer: NodeJS.Timeout | null = null;
  let service: WebhookService | null = null;

  const ensureService = (): WebhookService => {
    if (service === null) {
      throw new ServiceError('Webhooks is not ready yet');
    }
    return service;
  };

  const enqueueOrRun = async (operation: () => Promise<void>): Promise<void> => {
    if (closed) {
      throw new ServiceError('Webhooks instance is already closed');
    }

    if (ready) {
      await operation();
      return;
    }

    if (startupBuffer.length >= merged.startupBufferLimit) {
      throw new ServiceError('Startup buffer is full');
    }

    await new Promise<void>((resolve, reject) => {
      startupBuffer.push({ run: operation, resolve, reject });
    });
  };

  const scheduleReconnect = (): void => {
    if (closed || reconnectTimer !== null) {
      return;
    }

    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      void init();
    }, merged.reconnectBackoffMs);
    reconnectTimer.unref();
  };

  const cleanupRuntime = async (): Promise<void> => {
    if (runtime.worker !== null) {
      await runtime.worker.stop();
      runtime.worker = null;
    }

    if (runtime.publishChannel !== null) {
      await runtime.publishChannel.close();
      runtime.publishChannel = null;
    }

    if (runtime.consumeChannel !== null) {
      await runtime.consumeChannel.close();
      runtime.consumeChannel = null;
    }

    if (runtime.connection !== null) {
      await runtime.connection.close();
      runtime.connection = null;
    }

    if (runtime.db !== null) {
      await runtime.db.close();
      runtime.db = null;
    }
  };

  const init = async (): Promise<void> => {
    if (closed) {
      return;
    }

    try {
      if (runtime.connection !== null || runtime.db !== null) {
        await cleanupRuntime();
      }

      runtime.db = await openDb(merged.dbPath);
      const broker = await connectBroker(config.rabbitmqUrl, merged.concurrency);
      runtime.connection = broker.connection;
      runtime.publishChannel = broker.publishChannel;
      runtime.consumeChannel = broker.consumeChannel;

      runtime.connection.on('error', () => {
        ready = false;
        scheduleReconnect();
      });
      runtime.connection.on('close', () => {
        ready = false;
        scheduleReconnect();
      });

      const repository = new SubscriptionRepository(runtime.db);
      const publisher = new Publisher(broker.publishChannel, merged.maxRetries);
      service = new WebhookService(repository, publisher);
      runtime.worker = new Worker(broker.consumeChannel, broker.publishChannel, {
        timeoutMs: merged.timeoutMs,
      });

      await runtime.worker.start();
      ready = true;

      while (startupBuffer.length > 0) {
        const op = startupBuffer.shift();
        if (op !== undefined) {
          try {
            await op.run();
            op.resolve();
          } catch (error) {
            op.reject(error);
          }
        }
      }
    } catch {
      ready = false;
      scheduleReconnect();
    }
  };

  setImmediate(() => {
    void init();
  });

  return {
    async register(event: string, url: string): Promise<void> {
      await enqueueOrRun(async () => {
        await ensureService().register(event, url);
      });
    },

    async emit(event: string, payload: unknown): Promise<void> {
      await enqueueOrRun(async () => {
        await ensureService().emit(event, payload);
      });
    },

    async close(): Promise<void> {
      if (closingPromise !== null) {
        await closingPromise;
        return;
      }

      closingPromise = (async () => {
        closed = true;
        ready = false;

        if (reconnectTimer !== null) {
          clearTimeout(reconnectTimer);
          reconnectTimer = null;
        }

        await cleanupRuntime();

        while (startupBuffer.length > 0) {
          const op = startupBuffer.shift();
          if (op !== undefined) {
            op.reject(new ServiceError('Webhooks instance closed before initialization completed'));
          }
        }
      })();

      await closingPromise;
    },
  };
}
