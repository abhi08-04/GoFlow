// goflow-client — Node SDK for the GoFlow broker.
//
// Usage:
//
//   const { GoFlow } = require('goflow-client');
//   const g = new GoFlow('localhost:9090');
//
//   await g.publish('email', { to: 'a@b.com', subject: 'hi' }, 'idem-k1');
//
//   g.consume('email', async (job) => {
//     // job.payload is already parsed (JSON) when possible
//     await sendEmail(job.payload);
//     // return normally = ack; throw = nack
//   }, { prefetch: 4, leaseSeconds: 60 });
//
//   // on shutdown:
//   g.close();
//
// No gRPC types, no lease tokens, no stream mechanics in user code.

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

const PROTO_PATH = path.join(__dirname, 'proto', 'broker.proto');
const def = protoLoader.loadSync(PROTO_PATH, {
  keepCase: false,
  longs: Number,
  enums: String,
  defaults: true,
  oneofs: true,
});
const BrokerStub = grpc.loadPackageDefinition(def).goflow.v1.Broker;

class GoFlow {
  /**
   * @param {string} target  host:port of the broker (e.g. "localhost:9090")
   * @param {object} [opts]
   * @param {grpc.ChannelCredentials} [opts.credentials]  default: insecure
   */
  constructor(target, opts = {}) {
    const creds = opts.credentials || grpc.credentials.createInsecure();
    this.client = new BrokerStub(target, creds);
    this._consumers = new Set();
  }

  /**
   * Publish a job.
   * @param {string} type              job type
   * @param {any}    payload           Buffer | string | JSON-serializable object
   * @param {string} idempotencyKey    unique per logical request
   * @returns {Promise<Job>}
   */
  publish(type, payload, idempotencyKey) {
    const payloadBytes = toPayloadBytes(payload);
    return new Promise((resolve, reject) => {
      this.client.Publish(
        { type, payload: payloadBytes, idempotencyKey },
        (err, job) => (err ? reject(err) : resolve(normalizeJob(job)))
      );
    });
  }

  /**
   * Open a consumer. Returns a handle you can close() to stop.
   * @param {string}   type                      job type to subscribe to
   * @param {Handler}  handler                   (job) => Promise<void>|void
   * @param {object}   [opts]
   * @param {number}   [opts.prefetch=1]         un-acked in-flight cap
   * @param {number}   [opts.leaseSeconds=60]    per-delivery lease
   * @param {number}   [opts.reconnectDelayMs=2000]
   */
  consume(type, handler, opts = {}) {
    const consumer = new Consumer(this.client, type, handler, {
      prefetch: opts.prefetch || 1,
      leaseSeconds: opts.leaseSeconds || 60,
      reconnectDelayMs: opts.reconnectDelayMs || 2000,
    });
    this._consumers.add(consumer);
    consumer._start();
    return consumer;
  }

  /** Close every consumer and the underlying connection. */
  close() {
    for (const c of this._consumers) c.close();
    this._consumers.clear();
    try { this.client.close(); } catch (_) { /* already closed */ }
  }
}

class Consumer {
  constructor(client, type, handler, opts) {
    this.client = client;
    this.type = type;
    this.handler = handler;
    this.opts = opts;
    this.closed = false;
    this.stream = null;
    this._reconnectTimer = null;
  }

  _start() {
    if (this.closed) return;

    const stream = this.client.Consume();
    this.stream = stream;

    stream.write({
      start: {
        types: this.type ? [this.type] : [],
        leaseSeconds: this.opts.leaseSeconds,
        prefetch: this.opts.prefetch,
      },
    });

    stream.on('data', frame => {
      const d = frame.delivery;
      if (!d) return;
      const job = normalizeJob(d.job);
      Promise.resolve()
        .then(() => this.handler(job))
        .then(
          () => this._safeWrite({ ack: { jobId: d.job.id, leaseToken: d.leaseToken } }),
          err => this._safeWrite({
            nack: {
              jobId: d.job.id,
              leaseToken: d.leaseToken,
              error: (err && err.message) || String(err),
            },
          })
        );
    });

    const onDisconnect = () => {
      if (this.closed) return;
      this._reconnectTimer = setTimeout(() => this._start(), this.opts.reconnectDelayMs);
    };
    stream.on('error', onDisconnect);
    stream.on('end', onDisconnect);
  }

  _safeWrite(frame) {
    try { this.stream && this.stream.write(frame); }
    catch (_) { /* stream already closed */ }
  }

  close() {
    this.closed = true;
    if (this._reconnectTimer) clearTimeout(this._reconnectTimer);
    try { this.stream && this.stream.end(); } catch (_) { /* already closed */ }
  }
}

// ---------- helpers ----------

function toPayloadBytes(payload) {
  if (payload == null) return Buffer.alloc(0);
  if (Buffer.isBuffer(payload)) return payload;
  if (typeof payload === 'string') return Buffer.from(payload, 'utf8');
  if (payload instanceof Uint8Array) return Buffer.from(payload);
  // Objects / arrays / numbers / booleans → JSON.
  return Buffer.from(JSON.stringify(payload), 'utf8');
}

function normalizeJob(j) {
  const raw = Buffer.isBuffer(j.payload) ? j.payload : Buffer.from(j.payload || []);
  let payload = raw;
  if (raw.length > 0) {
    const str = raw.toString('utf8');
    try {
      payload = JSON.parse(str);
    } catch (_) {
      payload = raw;
    }
  }
  return {
    id: j.id,
    type: j.type,
    status: j.status,
    attempts: typeof j.attempts === 'number' ? j.attempts : Number(j.attempts || 0),
    payload,
    raw,
  };
}

module.exports = { GoFlow };
