# goflow-client

Node SDK for the [GoFlow broker](https://github.com/).

RabbitMQ-shaped API — no gRPC types, no lease tokens, no stream
mechanics in your code. Three method calls.

## Install

```bash
# From npm (after publish)
npm install goflow-client

# Local dev (point at this directory)
npm install /path/to/GoFlow/clients/node
```

## Usage

```javascript
const { GoFlow } = require('goflow-client');

const g = new GoFlow('localhost:9090');

// Produce
await g.publish('email',
  { to: 'a@b.com', subject: 'hi', body: 'hello' },
  'idem-key-123'
);

// Consume
const consumer = g.consume('email', async (job) => {
  // job.payload is already parsed (JSON) when the broker had JSON
  await sendEmail(job.payload);
  // return normally = ack; throw = nack with retry
}, { prefetch: 4, leaseSeconds: 60 });

// Later:
consumer.close();
g.close();
```

That's the full API.

## API

### `new GoFlow(target, opts?)`

- `target` — broker host:port (e.g. `localhost:9090`)
- `opts.credentials` — gRPC credentials; defaults to insecure plaintext

### `g.publish(type, payload, idempotencyKey): Promise<Job>`

Enqueue a job.

- `type` — job type string (e.g. `"email"`)
- `payload` — `Buffer`, `string`, `null`, or any JSON-serializable value
- `idempotencyKey` — unique per logical request. Sending the same key
  again returns the original job and does **not** re-enqueue

### `g.consume(type, handler, opts?): Consumer`

Open a long-lived consumer. Non-blocking — handler fires per delivery.

- `type` — subscribe to this job type (empty string = any)
- `handler(job) => Promise<void>` — return normally to ack, throw to nack
- `opts.prefetch` (default 1) — concurrent in-flight deliveries
- `opts.leaseSeconds` (default 60) — delivery lease window
- `opts.reconnectDelayMs` (default 2000) — delay between reconnects on
  transient stream errors

Call `consumer.close()` to stop.

### `g.close()`

Close every consumer and the underlying gRPC connection.

### `Job` shape

```ts
{
  id: string;
  type: string;
  status: string;          // "pending" | "running" | "completed" | "failed"
  attempts: number;        // 1 on first delivery, incremented on each retry
  payload: any;            // JSON-parsed if possible, else Buffer
  raw: Buffer;             // raw bytes exactly as the broker delivered
}
```

## What it hides for you

- Loading `broker.proto` and the gRPC codegen setup
- Opening and maintaining the bidi stream
- Sending the `StartConsume` opening frame
- Encoding/decoding payloads (JSON auto-encode on publish, auto-decode
  on consume)
- Issuing / tracking / echoing lease tokens
- Ack / nack frame construction
- Stream reconnection on transient errors

## Semantics

- **At-least-once delivery** — if the stream dies after the broker has
  pushed a delivery but before an ack lands, the broker's lease
  sweeper reclaims the job after `leaseSeconds` and re-delivers it.
  Your handler must be idempotent on retry.
- **Thrown error ⇒ nack with retry** — the broker will requeue after
  exponential backoff; after `MAX_RETRIES`, the job lands in the DLQ.
- **Return value ignored** — only the resolve/reject matters.
