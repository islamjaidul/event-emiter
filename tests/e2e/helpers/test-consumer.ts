import http, { type IncomingMessage, type ServerResponse } from 'node:http';

export interface TestConsumer {
  readonly url: string;
  readonly received: Array<{ event: string; data: unknown }>;
  readonly attempts: { count: number };
  stop(): Promise<void>;
}

interface TestConsumerOptions {
  readonly failForFirstAttempts?: number;
}

export async function startTestConsumer(options: TestConsumerOptions = {}): Promise<TestConsumer> {
  const received: Array<{ event: string; data: unknown }> = [];
  const attempts = { count: 0 };

  const server = http.createServer(async (req: IncomingMessage, res: ServerResponse) => {
    if (req.method !== 'POST' || req.url !== '/hook') {
      res.statusCode = 404;
      res.end('not found');
      return;
    }

    attempts.count += 1;

    const chunks: Buffer[] = [];
    for await (const chunk of req) {
      chunks.push(Buffer.from(chunk));
    }

    const parsed = JSON.parse(Buffer.concat(chunks).toString('utf8')) as { event?: string; data?: unknown };

    if (typeof parsed.event === 'string') {
      received.push({ event: parsed.event, data: parsed.data });
    }

    const threshold = options.failForFirstAttempts ?? 0;
    if (attempts.count <= threshold) {
      res.statusCode = 500;
      res.end('forced failure');
      return;
    }

    res.statusCode = 200;
    res.end('ok');
  });

  await new Promise<void>((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve());
  });

  const address = server.address();
  if (address === null || typeof address === 'string') {
    throw new Error('Failed to resolve test consumer address');
  }

  return {
    url: `http://127.0.0.1:${address.port}/hook`,
    received,
    attempts,
    stop: async () => {
      await new Promise<void>((resolve, reject) => {
        server.close((err) => {
          if (err) {
            reject(err);
            return;
          }
          resolve();
        });
      });
    },
  };
}
