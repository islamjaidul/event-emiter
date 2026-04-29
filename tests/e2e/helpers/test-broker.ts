import { RabbitMQContainer } from '@testcontainers/rabbitmq';

export interface TestBroker {
  readonly amqpUrl: string;
  stop(): Promise<void>;
}

export async function startTestBroker(): Promise<TestBroker> {
  const container = await new RabbitMQContainer('rabbitmq:3.13-management').start();
  return {
    amqpUrl: container.getAmqpUrl(),
    stop: async () => {
      await container.stop();
    },
  };
}
