export interface WebhooksConfig {
  readonly rabbitmqUrl: string;
  readonly dbPath?: string;
  readonly maxRetries?: number;
  readonly timeoutMs?: number;
  readonly concurrency?: number;
  readonly startupBufferLimit?: number;
  readonly reconnectBackoffMs?: number;
}

export interface WebhooksInstance {
  register(event: string, url: string): Promise<void>;
  emit(event: string, payload: unknown): Promise<void>;
  close(): Promise<void>;
}

export interface JobHeaders {
  readonly url: string;
  readonly attempts: number;
  readonly maxRetries: number;
  readonly originalEvent: string;
}

export interface Subscription {
  readonly id: string;
  readonly event: string;
  readonly url: string;
  readonly createdAt: number;
}
