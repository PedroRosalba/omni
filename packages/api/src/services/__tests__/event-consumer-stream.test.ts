/**
 * SSE consumer stream — DB-free units.
 *
 * Covers the stream loop (catch-up, held rows, wake, heartbeat, disconnect
 * cleanup), the wake hub, start-position precedence, and the service's
 * read-only stream scan against a fake handle that refuses every write. The
 * real-PostgreSQL case (catch-up + held hollow row) lives in
 * `__tests__/event-consumers-postgres.test.ts`.
 */

import { describe, expect, test } from 'bun:test';
import { NotFoundError } from '@omni/core';
import { type Database, type DurableConsumer, type OmniEvent, durableConsumers } from '@omni/db';
import {
  type ConsumerStreamOptions,
  ConsumerStreamWakeHub,
  type WakeConnection,
  resolveStreamIntervalMs,
  resolveStreamStart,
  runConsumerStream,
  wakePrefixesFor,
} from '../event-consumer-stream';
import { EventConsumerService, type StreamScanResult } from '../event-consumers';

const SETTLE_MS = 5_000;

function row(seq: number, extra: Partial<OmniEvent> = {}): OmniEvent {
  return {
    id: `00000000-0000-4000-8000-${String(seq).padStart(12, '0')}`,
    journalSeq: seq,
    eventType: 'message.received',
    journalReady: true,
    receivedAt: new Date(),
    rawPayload: { n: seq },
    ...extra,
  } as OmniEvent;
}

function consumer(extra: Partial<DurableConsumer> = {}): DurableConsumer {
  return {
    id: '11111111-1111-4111-8111-111111111111',
    name: 'vox',
    eventType: 'message.received',
    excludeTypes: null,
    filters: null,
    cursor: 7,
    shared: false,
    waitForReady: false,
    leaseState: null,
    createdAt: new Date(),
    updatedAt: new Date(),
    ...extra,
  } as DurableConsumer;
}

/** A handle that answers the two read chains and throws on any write. */
function fakePool(registered: DurableConsumer, page: () => OmniEvent[]): Database {
  const refuse = () => {
    throw new Error('the stream must never write');
  };
  return {
    select: () => ({
      from: (table: unknown) => {
        const query = {
          where: () => query,
          orderBy: () => query,
          limit: async (n: number) => (table === durableConsumers ? [registered] : page().slice(0, n)),
        };
        return query;
      },
    }),
    insert: refuse,
    update: refuse,
    delete: refuse,
    transaction: refuse,
  } as unknown as Database;
}

describe('resolveStreamStart', () => {
  test('Last-Event-ID wins over ?after, which wins over the stored cursor', () => {
    expect(resolveStreamStart({ lastEventId: 30, after: 20, cursor: 10 })).toBe(30);
    expect(resolveStreamStart({ after: 20, cursor: 10 })).toBe(20);
    expect(resolveStreamStart({ cursor: 10 })).toBe(10);
    expect(resolveStreamStart({ lastEventId: 0, after: 20, cursor: 10 })).toBe(0);
  });
});

describe('resolveStreamIntervalMs', () => {
  test('defaults when unset, blank, non-numeric, zero or negative', () => {
    for (const raw of [undefined, '', '  ', 'abc', '0', '-5'])
      expect(resolveStreamIntervalMs(raw, 15_000)).toBe(15_000);
  });
  test('honours a positive value', () => {
    expect(resolveStreamIntervalMs('2500', 15_000)).toBe(2500);
  });
});

describe('wakePrefixesFor', () => {
  test('exact type → "<type>." subject prefix; trailing-* glob → its prefix; * → everything', () => {
    expect(wakePrefixesFor('message.received')).toEqual(['message.received.']);
    expect(wakePrefixesFor('custom.github.*')).toEqual(['custom.github.']);
    expect(wakePrefixesFor('*')).toEqual(['']);
  });
});

describe('ConsumerStreamWakeHub', () => {
  function fakeConnection() {
    const subs = new Map<string, (err: unknown, msg: { subject: string }) => void>();
    let unsubscribed = 0;
    const connection: WakeConnection = {
      subscribe(subject, opts) {
        subs.set(subject, opts.callback);
        return {
          unsubscribe() {
            unsubscribed++;
            subs.delete(subject);
          },
        };
      },
    };
    const publish = (subject: string) => {
      for (const callback of subs.values()) callback(null, { subject });
    };
    return { connection, subs, publish, unsubscribed: () => unsubscribed };
  }

  test('one shared subscription set, fanned out by prefix, released with the last stream', () => {
    const nats = fakeConnection();
    const hub = new ConsumerStreamWakeHub(() => nats.connection, ['message.>']);
    const woke: string[] = [];
    const offA = hub.register(['message.received.'], () => woke.push('a'));
    const offB = hub.register(['custom.'], () => woke.push('b'));
    expect(hub.subscriptionCount).toBe(1);

    nats.publish('message.received.whatsapp-baileys.inst-1');
    nats.publish('message.sent.whatsapp-baileys.inst-1');
    expect(woke).toEqual(['a']);

    offA();
    expect(hub.subscriptionCount).toBe(1);
    offB();
    expect(hub.subscriptionCount).toBe(0);
    expect(nats.unsubscribed()).toBe(1);
  });

  test('without NATS it is polling-only, and attaches once a connection appears', () => {
    const nats = fakeConnection();
    let available = false;
    const hub = new ConsumerStreamWakeHub(() => (available ? nats.connection : null), ['message.>', 'custom.>']);
    const offA = hub.register([''], () => undefined);
    expect(hub.subscriptionCount).toBe(0);
    available = true;
    const offB = hub.register([''], () => undefined);
    expect(hub.subscriptionCount).toBe(2);
    offA();
    offB();
    expect(hub.subscriptionCount).toBe(0);
  });
});

/** A fake journal the loop scans; matching is the scan's job, so the fake filters by `match`. */
function harness(options: { journal: OmniEvent[]; match?: (r: OmniEvent) => boolean; pageSize?: number }) {
  const journal = options.journal;
  const match = options.match ?? (() => true);
  const pageSize = options.pageSize ?? 100;
  const scans: number[] = [];
  const sent: OmniEvent[] = [];
  let heartbeats = 0;
  let wake: (() => void) | null = null;
  let unsubscribed = false;
  let held: ((r: OmniEvent) => boolean) | null = null;
  const closed: Error[] = [];
  const errors: unknown[] = [];
  let failNext: Error | null = null;

  const scan = async (after: number): Promise<StreamScanResult> => {
    scans.push(after);
    if (failNext) {
      const error = failNext;
      failNext = null;
      throw error;
    }
    const page = journal.filter((r) => r.journalSeq > after).slice(0, pageSize);
    const cut = held ? page.findIndex(held) : -1;
    const rows = cut === -1 ? page : page.slice(0, cut);
    const last = rows[rows.length - 1];
    return {
      items: rows.filter(match),
      through: last ? last.journalSeq : null,
      held: cut !== -1,
      full: cut === -1 && page.length === pageSize,
    };
  };

  const controller = new AbortController();
  const base: ConsumerStreamOptions = {
    start: 0,
    scan,
    send: async (r) => {
      sent.push(r);
    },
    heartbeat: async () => {
      heartbeats++;
    },
    close: async (error) => {
      closed.push(error);
    },
    subscribeWake: (w) => {
      wake = w;
      return () => {
        unsubscribed = true;
        wake = null;
      };
    },
    signal: controller.signal,
    heartbeatMs: 60_000,
    fallbackMs: 60_000,
    heldRescanMs: 10,
    wakeFollowUpMs: [],
    onError: (error) => errors.push(error),
  };

  return {
    run: (overrides: Partial<ConsumerStreamOptions> = {}) => runConsumerStream({ ...base, ...overrides }),
    abort: () => controller.abort(),
    wake: () => wake?.(),
    hold: (fn: ((r: OmniEvent) => boolean) | null) => {
      held = fn;
    },
    failNextScan: (error: Error) => {
      failNext = error;
    },
    scans,
    sent,
    closed,
    errors,
    heartbeats: () => heartbeats,
    unsubscribed: () => unsubscribed,
    hasWake: () => wake !== null,
  };
}

const settle = (ms = 30) => new Promise((resolve) => setTimeout(resolve, ms));
const seqs = (rows: OmniEvent[]) => rows.map((r) => r.journalSeq);

describe('runConsumerStream', () => {
  test('catch-up sends matching rows in order, pages through full pages, and skips non-matches while advancing', async () => {
    const journal = Array.from({ length: 7 }, (_, i) => row(i + 1, { rawPayload: { even: (i + 1) % 2 === 0 } }));
    const h = harness({ journal, pageSize: 3, match: (r) => (r.rawPayload as { even: boolean }).even });
    const done = h.run({ start: 0 });
    await settle();
    expect(seqs(h.sent)).toEqual([2, 4, 6]);
    // Pages after 0, 3, 6 (full pages chain immediately), then 7 is drained.
    expect(h.scans).toEqual([0, 3, 6]);

    // Next scan starts past the last SCANNED row (7, a non-match), not the last sent (6).
    h.wake();
    await settle();
    expect(h.scans[h.scans.length - 1]).toBe(7);
    h.abort();
    await done;
  });

  test('starts strictly after the given position', async () => {
    const h = harness({ journal: [row(1), row(2), row(3)] });
    const done = h.run({ start: 2 });
    await settle();
    expect(seqs(h.sent)).toEqual([3]);
    h.abort();
    await done;
  });

  test('a held row is not sent and is re-scanned fast until it fills', async () => {
    const journal = [row(1), row(2, { journalReady: false })];
    const h = harness({ journal });
    h.hold((r) => !r.journalReady);
    const done = h.run();
    await settle(15);
    expect(seqs(h.sent)).toEqual([1]);
    const scansWhileHeld = h.scans.length;
    await settle(40);
    expect(h.scans.length).toBeGreaterThan(scansWhileHeld); // held re-scan, not the 60 s fallback
    expect(h.scans.every((after) => after <= 1)).toBe(true);

    journal[1] = row(2, { journalReady: true, rawPayload: { filled: true } });
    await settle(40);
    expect(seqs(h.sent)).toEqual([1, 2]);
    expect(h.sent[1]?.rawPayload).toEqual({ filled: true });
    h.abort();
    await done;
  });

  test('a wake triggers a re-scan before the fallback interval', async () => {
    const journal = [row(1)];
    const h = harness({ journal });
    const done = h.run();
    await settle();
    expect(seqs(h.sent)).toEqual([1]);
    journal.push(row(2));
    await settle();
    expect(seqs(h.sent)).toEqual([1]); // nothing without a wake (fallback is 60 s)
    h.wake();
    await settle();
    expect(seqs(h.sent)).toEqual([1, 2]);
    h.abort();
    await done;
  });

  test('a wake schedules quick follow-up scans for a fill that lands after the NATS message', async () => {
    const journal = [row(1)];
    const h = harness({ journal });
    const done = h.run({ wakeFollowUpMs: [20] });
    await settle();
    h.wake(); // the event row is not in the journal yet
    await settle(5);
    journal.push(row(2));
    await settle(50);
    expect(seqs(h.sent)).toEqual([1, 2]);
    h.abort();
    await done;
  });

  test('the fallback interval re-scans without any wake', async () => {
    const journal = [row(1)];
    const h = harness({ journal });
    const done = h.run({ fallbackMs: 20 });
    await settle();
    journal.push(row(2));
    await settle(60);
    expect(seqs(h.sent)).toEqual([1, 2]);
    h.abort();
    await done;
  });

  test('emits heartbeats on the configured interval', async () => {
    const h = harness({ journal: [] });
    const done = h.run({ heartbeatMs: 10 });
    await settle(55);
    expect(h.heartbeats()).toBeGreaterThanOrEqual(2);
    h.abort();
    await done;
  });

  test('disconnect releases the wake registration and every timer', async () => {
    const h = harness({ journal: [] });
    const done = h.run({ heartbeatMs: 10, fallbackMs: 10 });
    await settle();
    expect(h.hasWake()).toBe(true);
    h.abort();
    await done;
    expect(h.unsubscribed()).toBe(true);
    const scans = h.scans.length;
    const beats = h.heartbeats();
    await settle(50);
    expect(h.scans.length).toBe(scans);
    expect(h.heartbeats()).toBe(beats);
  });

  test('a consumer deleted mid-stream ends the stream with a close event', async () => {
    const h = harness({ journal: [row(1)] });
    const done = h.run({ fallbackMs: 10 });
    await settle();
    h.failNextScan(new NotFoundError('DurableConsumer', 'vox'));
    await done; // resolves on its own: no abort needed
    expect(h.closed).toHaveLength(1);
    expect(h.unsubscribed()).toBe(true);
  });

  test('a transient scan failure is reported and retried on the fallback interval', async () => {
    const h = harness({ journal: [row(1)] });
    h.failNextScan(new Error('connection reset'));
    const done = h.run({ fallbackMs: 15 });
    await settle(50);
    expect(h.errors).toHaveLength(1);
    expect(seqs(h.sent)).toEqual([1]);
    h.abort();
    await done;
  });
});

describe('EventConsumerService.scanForStream (read-only, gate always on)', () => {
  test('a hollow row younger than the settle window is held even when the consumer has no waitForReady', async () => {
    const page = [row(8), row(9, { journalReady: false }), row(10)];
    const service = new EventConsumerService(fakePool(consumer({ waitForReady: false }), () => page));
    const result = await service.scanForStream('vox', 7, null);
    expect(seqs(result.items)).toEqual([8]);
    expect(result).toMatchObject({ through: 8, held: true, full: false });
  });

  test('a dead hollow row older than the settle window does not stall the stream', async () => {
    const page = [row(8, { journalReady: false, receivedAt: new Date(Date.now() - SETTLE_MS - 1_000) }), row(9)];
    const service = new EventConsumerService(fakePool(consumer(), () => page));
    const result = await service.scanForStream('vox', 7, null);
    expect(seqs(result.items)).toEqual([8, 9]);
    expect(result).toMatchObject({ through: 9, held: false });
  });

  test('payload filters drop non-matches but `through` covers every scanned row', async () => {
    const page = [row(8, { rawPayload: { kind: 'a' } }), row(9, { rawPayload: { kind: 'b' } })];
    const filtered = consumer({ filters: [{ field: 'kind', operator: 'eq', value: 'a' }] });
    const service = new EventConsumerService(fakePool(filtered, () => page));
    const result = await service.scanForStream('vox', 7, null);
    expect(seqs(result.items)).toEqual([8]);
    expect(result.through).toBe(9);
  });

  test('a full uncut page reports full; an empty page reports nothing scanned', async () => {
    const rows = [row(8), row(9)];
    const full = await new EventConsumerService(fakePool(consumer(), () => rows)).scanForStream('vox', 7, null, 2);
    expect(full).toMatchObject({ through: 9, held: false, full: true });
    const empty = await new EventConsumerService(fakePool(consumer(), () => [])).scanForStream('vox', 9, null);
    expect(empty).toEqual({ items: [], through: null, held: false, full: false });
  });

  test('a shared consumer is refused (leases are the pull path)', async () => {
    const service = new EventConsumerService(fakePool(consumer({ shared: true }), () => []));
    await expect(service.getStreamable('vox')).rejects.toThrow(/shared/);
    await expect(service.scanForStream('vox', 0, null)).rejects.toThrow(/shared/);
  });

  test('never writes: the fake handle throws on insert/update/delete and the scan still succeeds', async () => {
    const service = new EventConsumerService(fakePool(consumer(), () => [row(8)]));
    await expect(service.scanForStream('vox', 7, null)).resolves.toMatchObject({ through: 8 });
  });
});
