/**
 * SSE push for durable event consumers (`GET /events/consumers/:name/stream`).
 *
 * An opt-in alternative to long-polling `POST /events/consumers/:name/pull`:
 * same journal, same scan, same filters, same order, pushed as Server-Sent
 * Events. Nothing about pull/ack changes, and the stream is READ-ONLY with
 * respect to the consumer: it keeps its position in memory, never writes the
 * stored cursor and never takes or acks a lease. Clients keep committing
 * progress with `POST /events/consumers/:name/ack`.
 *
 * The journal is the single authority. Every page comes from
 * `EventConsumerService.scanForStream` (the pull scan with the readiness gate
 * always on), so any replica can serve a stream and a hollow
 * claimed-but-unfilled row is held, never pushed.
 *
 * Wake-up: one per-process core NATS subscription on the JetStream subjects
 * (a plain subscription on the shared turn-events connection: no JetStream
 * consumer, no server state, no acks, no effect on any existing consumer)
 * fans out to the open streams whose type filter could match the subject.
 * A wake only triggers a re-scan of the journal; it carries no data. The wake
 * can beat the persistence fill, so a woken stream re-scans a few times in
 * quick succession, re-scans fast while a held row is pending, and falls back
 * to a fixed interval otherwise (or always, when NATS is unavailable).
 */

import { NotFoundError, STREAM_CONFIGS, ValidationError } from '@omni/core';
import type { OmniEvent } from '@omni/db';
import type { StreamScanResult } from './event-consumers';
import { getTurnEventsConnection } from './turn-events';

/** Default SSE heartbeat (EVENT_STREAM_HEARTBEAT_MS). Bun.serve idleTimeout is 60 s. */
export const DEFAULT_STREAM_HEARTBEAT_MS = 15_000;
/** Default re-scan interval with no wake (STREAM_POLL_FALLBACK_MS). */
export const DEFAULT_STREAM_POLL_FALLBACK_MS = 1_000;
/** Re-scan delay while a not-ready row is held at the head of the page. */
const STREAM_HELD_RESCAN_MS = 100;
/** Quick re-scans after a wake, for a fill that lands just after the NATS message. */
const STREAM_WAKE_FOLLOW_UP_MS: readonly number[] = [50, 200, 500];

/** A positive interval from the environment, or the default. Read per stream. */
export function resolveStreamIntervalMs(raw: string | undefined, fallback: number): number {
  if (raw === undefined || raw.trim() === '') return fallback;
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

/**
 * Where a stream starts: the `Last-Event-ID` header (an EventSource
 * reconnect), else `?after=`, else the consumer's stored cursor. Delivery is
 * strictly after this position. Inputs arrive Zod-validated by the route.
 */
export function resolveStreamStart(input: { lastEventId?: number; after?: number; cursor: number }): number {
  return input.lastEventId ?? input.after ?? input.cursor;
}

/**
 * Subject prefixes that can carry an event the type filter matches: exact
 * `a.b` → `a.b.` (subjects are `<type>.<channel>.<instance>`), trailing-*
 * glob `a.*` → `a.`. A superset by design: a false wake costs one re-scan.
 */
export function wakePrefixesFor(eventType: string): string[] {
  return [eventType.endsWith('*') ? eventType.slice(0, -1) : `${eventType}.`];
}

interface CoreSubscription {
  unsubscribe(): void;
}

/** The slice of a NATS connection the hub uses (structural, for tests). */
export interface WakeConnection {
  subscribe(subject: string, opts: { callback: (err: unknown, msg: { subject: string }) => void }): CoreSubscription;
}

interface WakeListener {
  prefixes: readonly string[];
  wake: () => void;
}

/**
 * One shared core-NATS listener per process, fanned out to open streams.
 * Subscribes when the first stream registers (retrying on later
 * registrations while NATS is unavailable) and unsubscribes when the last
 * stream leaves, so an idle process holds no subscription at all.
 */
export class ConsumerStreamWakeHub {
  private readonly listeners = new Set<WakeListener>();
  private subscriptions: CoreSubscription[] = [];

  constructor(
    private readonly getConnection: () => WakeConnection | null,
    private readonly subjects: readonly string[],
  ) {}

  /** Wake `wake` on any subject starting with one of `prefixes`. Returns the unregister function. */
  register(prefixes: readonly string[], wake: () => void): () => void {
    const listener: WakeListener = { prefixes, wake };
    this.listeners.add(listener);
    this.attach();
    return () => {
      this.listeners.delete(listener);
      if (this.listeners.size === 0) this.detach();
    };
  }

  /** Open core subscriptions (0 = polling only). Exposed for tests. */
  get subscriptionCount(): number {
    return this.subscriptions.length;
  }

  private attach(): void {
    if (this.subscriptions.length > 0) return;
    const connection = this.getConnection();
    if (!connection) return;
    try {
      this.subscriptions = this.subjects.map((subject) =>
        connection.subscribe(subject, { callback: (err, msg) => (err ? undefined : this.dispatch(msg.subject)) }),
      );
    } catch {
      this.detach(); // connection closing: streams keep their fallback interval
    }
  }

  private detach(): void {
    for (const subscription of this.subscriptions) {
      try {
        subscription.unsubscribe();
      } catch {
        // already closed with its connection
      }
    }
    this.subscriptions = [];
  }

  private dispatch(subject: string): void {
    for (const listener of this.listeners) {
      if (listener.prefixes.some((prefix) => subject.startsWith(prefix))) listener.wake();
    }
  }
}

/** Every subject the JetStream event streams capture. */
const EVENT_SUBJECTS = Object.values(STREAM_CONFIGS).flatMap((config) => config.subjects ?? []);

/** The process-wide hub, on the shared plain NATS connection (null → polling only). */
export const consumerStreamWakeHub = new ConsumerStreamWakeHub(
  () => getTurnEventsConnection() as WakeConnection | null,
  EVENT_SUBJECTS,
);

export interface ConsumerStreamOptions {
  /** Position to deliver strictly after. */
  start: number;
  /** One journal page strictly after a position (EventConsumerService.scanForStream). */
  scan: (after: number) => Promise<StreamScanResult>;
  /** Write one matching row as an SSE event. Awaited: scanning is driven by successful writes. */
  send: (row: OmniEvent) => Promise<void>;
  /** Write an SSE comment line. */
  heartbeat: () => Promise<void>;
  /** End the stream with a final error event (consumer deleted or no longer streamable). */
  close: (error: Error) => Promise<void>;
  /** Register for wake-ups; returns the unregister function. */
  subscribeWake: (wake: () => void) => () => void;
  /** Aborted when the client disconnects. */
  signal: AbortSignal;
  heartbeatMs: number;
  fallbackMs: number;
  heldRescanMs?: number;
  wakeFollowUpMs?: readonly number[];
  /** A transient scan failure; the stream retries on the fallback interval. */
  onError?: (error: unknown) => void;
}

/** Errors that end the stream instead of being retried. */
function isTerminal(error: unknown): error is Error {
  return error instanceof NotFoundError || error instanceof ValidationError;
}

/**
 * Catch up from `start`, then follow live, until the client disconnects.
 * Scans never overlap: a wake during a scan marks the stream dirty and the
 * scan loop goes round once more. Resolves after every timer, the heartbeat
 * and the wake registration are released.
 */
export async function runConsumerStream(options: ConsumerStreamOptions): Promise<void> {
  const { signal, heartbeatMs, fallbackMs } = options;
  const heldRescanMs = options.heldRescanMs ?? STREAM_HELD_RESCAN_MS;
  const wakeFollowUpMs = options.wakeFollowUpMs ?? STREAM_WAKE_FOLLOW_UP_MS;

  let position = options.start;
  let stopped = false;
  let running = false;
  let dirty = false;
  let followUps: number[] = [];
  let timer: ReturnType<typeof setTimeout> | undefined;
  let inflight: Promise<void> = Promise.resolve();
  let finish = () => {};
  const finished = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const stop = () => {
    stopped = true;
    finish();
  };
  if (signal.aborted) stop();
  else signal.addEventListener('abort', stop, { once: true });

  /** Scan and send until the journal is drained or a row is held. */
  const drain = async (): Promise<'held' | 'idle'> => {
    for (;;) {
      const page = await options.scan(position);
      for (const row of page.items) {
        if (stopped) return 'idle';
        await options.send(row);
      }
      if (page.through !== null) position = Math.max(position, page.through);
      if (page.held) return 'held';
      if (!page.full || stopped) return 'idle';
    }
  };

  const cycle = async (): Promise<void> => {
    let next = fallbackMs;
    try {
      let status: 'held' | 'idle';
      do {
        dirty = false;
        status = await drain();
      } while (dirty && !stopped);
      next = status === 'held' ? heldRescanMs : (followUps.shift() ?? fallbackMs);
    } catch (error) {
      if (stopped) return;
      if (isTerminal(error)) {
        await options.close(error).catch(() => undefined);
        stop();
        return;
      }
      options.onError?.(error);
    } finally {
      running = false;
    }
    if (!stopped) timer = setTimeout(request, next);
  };

  function request(): void {
    if (stopped) return;
    if (running) {
      dirty = true;
      return;
    }
    clearTimeout(timer);
    running = true;
    inflight = cycle();
  }

  const unsubscribe = options.subscribeWake(() => {
    followUps = [...wakeFollowUpMs];
    request();
  });
  const heartbeat = setInterval(() => {
    if (!stopped) options.heartbeat().catch(stop);
  }, heartbeatMs);

  try {
    request();
    await finished;
  } finally {
    stopped = true;
    clearInterval(heartbeat);
    clearTimeout(timer);
    unsubscribe();
    await inflight.catch(() => undefined);
  }
}
