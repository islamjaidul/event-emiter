export interface IPublisher {
  publish(event: string, url: string, payload: unknown): Promise<void>;
}
