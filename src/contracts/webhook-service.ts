export interface IWebhookService {
  register(event: string, url: string): Promise<void>;
  emit(event: string, payload: unknown): Promise<void>;
}
