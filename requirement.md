Build a TypeScript library that any Node.js application can use to reliably deliver webhook messages to registered subscriber URLs.


The integration should be as simple as:


const webhooks = createWebhooks({ ... })

await webhooks.register('order.created', 'https://example.com/hook')
await webhooks.emit('order.created', { orderId: 123 })

Delivery must be reliable enough that a server restart doesn't lose pending or in-flight webhooks. Slow or failing subscribers must not affect the caller when emit is called.


Include a working example app and a README.md covering how to run it, the delivery guarantees you provide, and one tradeoff you'd revisit with more time.

Instructions:

1. Share the Github URL of the project when it is done.
2. Time Limit: 2hrs.
3. Usage of a coding agent is allowed. Make sure you document the process and prompts for the further interview.


Non functional requirement
- This library is designed for backend application where Typescript library can be reusable across the multiple application
    - remember, we are developing reusable library that will emit with key and value ("event", "web-link")
    - where this library will be registered, consider this as producer service who produce events
    - there will be a consumer service who will consume those events
- As this is library, while it's loading the main node js application thread should not be blocked. Library can be heavy later on (consider this)
- For a single registration, multiple event .e.g. 'order.submitted', 'order.delivered' can be happened, so design in a way that emit can be done from anywhere
    - Singletone const webhooks = createWebhooks({ ... }) can be registered for service initialization
    - createWebhooks can be close when service down gracefully
    - make sure there is no memory leakage
- design queue system there should be retry mechanism. e.g. if any downstream / consumer service is down then it should consumer later on
- make sure, no data loss is happened (hard rule)
- I want to use rabbitmq to make sure producer and consumer. Everything should be in docker
- Create two consumer app who will receive webhook 
- Follow typescript convention correctly