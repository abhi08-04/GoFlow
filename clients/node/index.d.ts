/* TypeScript declarations for goflow-client. */

export interface Job {
  id: string;
  type: string;
  status: string;
  attempts: number;
  /** Parsed JSON if possible, else the raw Buffer. */
  payload: any;
  /** The raw payload bytes exactly as the broker delivered them. */
  raw: Buffer;
}

export type Handler = (job: Job) => void | Promise<void>;

export interface ConsumeOpts {
  prefetch?: number;
  leaseSeconds?: number;
  reconnectDelayMs?: number;
}

export interface Consumer {
  close(): void;
}

export interface GoFlowOpts {
  credentials?: unknown; // grpc.ChannelCredentials
}

export class GoFlow {
  constructor(target: string, opts?: GoFlowOpts);
  publish(type: string, payload: any, idempotencyKey: string): Promise<Job>;
  consume(type: string, handler: Handler, opts?: ConsumeOpts): Consumer;
  close(): void;
}
