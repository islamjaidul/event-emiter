export class WebhookError extends Error {
  public constructor(message: string, public readonly cause?: unknown) {
    super(message);
    this.name = 'WebhookError';
  }
}

export class ValidationError extends WebhookError {
  public constructor(message: string) {
    super(message);
    this.name = 'ValidationError';
  }
}

export class RepositoryError extends WebhookError {
  public constructor(message: string, cause?: unknown) {
    super(message, cause);
    this.name = 'RepositoryError';
  }
}

export class ServiceError extends WebhookError {
  public constructor(message: string, cause?: unknown) {
    super(message, cause);
    this.name = 'ServiceError';
  }
}

export class BrokerError extends WebhookError {
  public constructor(message: string, cause?: unknown) {
    super(message, cause);
    this.name = 'BrokerError';
  }
}
