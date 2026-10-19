/**
 * Durable event consumers — pure-logic units (#989).
 *
 * The DB-bound contract (paging, ack monotonicity against real rows, RLS
 * containment) lives in `__tests__/event-consumers-postgres.test.ts`; these
 * cover the pure pieces: the payload matcher (which MUST be the events-wait /
 * automation matcher), initial-cursor resolution, and lag math.
 */

import { afterEach, beforeEach, describe, expect, mock, test } from 'bun:test';
import { ValidationError } from '@omni/core';
import type { EventBus } from '@omni/core';
import type { ConsumerAccessFilter, Database, DurableConsumer, SharedLeaseState } from '@omni/db';
import { durableConsumers, omniEvents } from '@omni/db';
import { type SQL, and, gt } from 'drizzle-orm';
import { PgDialect } from 'drizzle-orm/pg-core';
import {
  EventConsumerService,
  accessFilterClause,
  claimLease,
  computeLag,
  matchesConsumerPayload,
  planSharedPull,
  releaseLease,
  resolveInitialCursor,
} from '../event-consumers';
import { eventTypeFilterClause } from '../events';

describe('matchesConsumerPayload', () => {
  test('no filters = always match (and null filters too)', () => {
    expect(matchesConsumerPayload({ rawPayload: { a: 1 } }, undefined)).toBe(true);
    expect(matchesConsumerPayload({ rawPayload: { a: 1 } }, null)).toBe(true);
    expect(matchesConsumerPayload({ rawPayload: null }, [])).toBe(true);
  });

  test('conditions run through the automation matcher (nested dot paths)', () => {
    const row = { rawPayload: { user: { id: 42 }, status: 'open' } };
    expect(matchesConsumerPayload(row, [{ field: 'user.id', operator: 'eq', value: 42 }])).toBe(true);
    expect(matchesConsumerPayload(row, [{ field: 'user.id', operator: 'eq', value: 43 }])).toBe(false);
    expect(
      matchesConsumerPayload(row, [
        { field: 'status', operator: 'eq', value: 'open' },
        { field: 'user.id', operator: 'gte', value: 40 },
      ]),
    ).toBe(true);
    expect(
      matchesConsumerPayload(row, [
        { field: 'status', operator: 'eq', value: 'closed' },
        { field: 'user.id', operator: 'eq', value: 42 },
      ]),
    ).toBe(false);
  });

  test('a null rawPayload matches only payload-independent conditions', () => {
    expect(matchesConsumerPayload({ rawPayload: null }, [{ field: 'status', operator: 'exists' }])).toBe(false);
    expect(matchesConsumerPayload({ rawPayload: null }, [{ field: 'status', operator: 'not_exists' }])).toBe(true);
  });
});

describe('resolveInitialCursor', () => {
  test("'now' starts at the journal head, 'beginning' at 0", () => {
    expect(resolveInitialCursor('now', 123)).toBe(123);
    expect(resolveInitialCursor('beginning', 123)).toBe(0);
    expect(resolveInitialCursor('now', 0)).toBe(0);
  });
});

describe('computeLag', () => {
  test('lag = head - cursor, floored at 0', () => {
    expect(computeLag(10, 4)).toBe(6);
    expect(computeLag(10, 10)).toBe(0);
    // A cursor past the head (journal pruned/reset) must not report negative lag.
    expect(computeLag(3, 10)).toBe(0);
  });
});

describe('shared consumer leases (#1188)', () => {
  /** Simulated journal seqs 1..n; a pull leases up to `limit` rows after the plan's position. */
  function pull(state: SharedLeaseState, journal: number[], limit: number, now: number, id: string) {
    const plan = planSharedPull(state, now);
    const rows = plan.reclaim
      ? journal.filter((seq) => seq > plan.reclaim.from && seq <= plan.reclaim.to)
      : journal.filter((seq) => seq > plan.after).slice(0, limit);
    if (rows.length === 0) return { state, rows, id: null };
    const from = plan.reclaim ? plan.reclaim.from : plan.after;
    const lease = { id, from, to: rows[rows.length - 1] as number, expiresAt: now + 1000 };
    return { state: claimLease(state, lease, plan.reclaim?.id), rows, id };
  }

  test('two pullers on one shared consumer receive each event exactly once between them', () => {
    const journal = Array.from({ length: 10 }, (_, i) => i + 1);
    let state: SharedLeaseState = { claimed: 0, leases: [] };
    const seen: Record<string, number[]> = { a: [], b: [] };
    let cursor = 0;
    for (let round = 0; round < 10; round++) {
      // Both pull before either acks — the concurrent case.
      const a = pull(state, journal, 2, 0, `a${round}`);
      state = a.state;
      const b = pull(state, journal, 2, 0, `b${round}`);
      state = b.state;
      seen.a?.push(...a.rows);
      seen.b?.push(...b.rows);
      // Ack out of order: b first.
      for (const id of [b.id, a.id]) {
        if (!id) continue;
        const released = releaseLease(state, id);
        if (!released) throw new Error('lease vanished');
        state = released.state;
        cursor = released.cursor;
      }
    }
    expect([...(seen.a ?? []), ...(seen.b ?? [])].sort((x, y) => x - y)).toEqual(journal);
    expect(seen.a?.length).toBeGreaterThan(0);
    expect(seen.b?.length).toBeGreaterThan(0);
    expect(cursor).toBe(10);
  });

  test('cursor waits for the oldest outstanding lease', () => {
    let state: SharedLeaseState = { claimed: 0, leases: [] };
    state = claimLease(state, { id: 'a', from: 0, to: 5, expiresAt: 1 });
    state = claimLease(state, { id: 'b', from: 5, to: 9, expiresAt: 1 });
    const afterB = releaseLease(state, 'b');
    expect(afterB?.cursor).toBe(0);
    expect(afterB && releaseLease(afterB.state, 'a')?.cursor).toBe(9);
  });

  test('an unacked lease is redelivered after expiry and the stale ack is refused', () => {
    const journal = [1, 2, 3];
    let state: SharedLeaseState = { claimed: 0, leases: [] };
    const first = pull(state, journal, 10, 0, 'crashed');
    state = first.state;
    expect(pull(state, journal, 10, 500, 'early').rows).toEqual([]); // still leased
    const retry = pull(state, journal, 10, 1000, 'retry');
    expect(retry.rows).toEqual([1, 2, 3]);
    expect(releaseLease(retry.state, 'crashed')).toBeNull();
    expect(releaseLease(retry.state, 'retry')?.cursor).toBe(3);
  });
});

// ============================================================================
// Opt-in access filter (0080): SQL predicate inside scanPage, gated create
// ============================================================================

const ENV = 'OMNI_JOURNAL_ACCESS_DECISION';

function render(clause: SQL | undefined): { sql: string; params: unknown[] } {
  if (!clause) throw new Error('expected a clause');
  const q = new PgDialect().sqlToQuery(clause);
  return { sql: q.sql.replace(/\s+/g, ' '), params: q.params };
}

function consumerRow(overrides: Partial<DurableConsumer> = {}): DurableConsumer {
  return {
    id: '00000000-0000-4000-8000-000000000001',
    name: 'c1',
    eventType: 'message.received',
    excludeTypes: null,
    filters: null,
    cursor: 0,
    shared: false,
    leaseState: null,
    accessFilter: 'all',
    createdAt: new Date('2026-01-01'),
    updatedAt: new Date('2026-01-01'),
    ...overrides,
  };
}

/**
 * DB-free pool: getByName returns `consumer`, head() returns `head`, the
 * journal scan returns no rows. Every journal-scan WHERE is captured.
 */
function mockPool(consumer: DurableConsumer, head = 0) {
  const scanWheres: SQL[] = [];
  const inserted: unknown[] = [];
  const select = mock((fields?: Record<string, unknown>) => ({
    from: (table: unknown) => {
      const rows = table === durableConsumers ? [consumer] : fields && 'head' in fields ? [{ head }] : [];
      const chain = {
        where: (cond: SQL) => {
          if (table === omniEvents) scanWheres.push(cond);
          return chain;
        },
        orderBy: () => chain,
        limit: () => chain,
        // biome-ignore lint/suspicious/noThenProperty: drizzle query builders are thenables
        then: (resolve: (v: unknown) => unknown, reject?: (e: unknown) => unknown) =>
          Promise.resolve(rows).then(resolve, reject),
      };
      return chain;
    },
  }));
  const insert = mock(() => ({
    values: (v: Record<string, unknown>) => {
      inserted.push(v);
      return {
        onConflictDoNothing: () => ({
          returning: () =>
            Promise.resolve([consumerRow({ ...v, cursor: Number(v.cursor) } as Partial<DurableConsumer>)]),
        }),
      };
    },
  }));
  return { db: { select, insert } as unknown as Database, select, insert, scanWheres, inserted };
}

describe('accessFilterClause', () => {
  test("'all' (and a missing filter) adds no predicate", () => {
    expect(accessFilterClause('all')).toBeUndefined();
    expect(accessFilterClause(undefined)).toBeUndefined();
    expect(accessFilterClause(null)).toBeUndefined();
  });

  test("'allowed' / 'denied' produce access_decision = <filter>", () => {
    for (const filter of ['allowed', 'denied'] as const) {
      const { sql, params } = render(accessFilterClause(filter));
      expect(sql).toBe('"omni_events"."access_decision" = $1');
      expect(params).toEqual([filter]);
    }
  });
});

describe('scanPage access predicate (via pull)', () => {
  function todaysWhere(): { sql: string; params: unknown[] } {
    // The pre-0080 scanPage WHERE: journal_seq bound + type clause, nothing else.
    return render(
      and(gt(omniEvents.journalSeq, 0), undefined, eventTypeFilterClause(['message.received'], undefined)) as SQL,
    );
  }

  test("an 'all' consumer scans with exactly the unfiltered WHERE", async () => {
    const pool = mockPool(consumerRow({ accessFilter: 'all' }));
    await new EventConsumerService(pool.db).pull('c1');
    expect(pool.scanWheres).toHaveLength(1);
    expect(render(pool.scanWheres[0])).toEqual(todaysWhere());
  });

  test("an 'allowed' / 'denied' consumer gets the eq predicate in SQL", async () => {
    for (const filter of ['allowed', 'denied'] as ConsumerAccessFilter[]) {
      const pool = mockPool(consumerRow({ accessFilter: filter }));
      await new EventConsumerService(pool.db).pull('c1');
      const { sql, params } = render(pool.scanWheres[0]);
      expect(sql).toContain('"omni_events"."access_decision" = $');
      expect(sql.startsWith(todaysWhere().sql.slice(0, -1))).toBe(true);
      expect(params).toEqual([...todaysWhere().params, filter]);
    }
  });
});

describe('create() access option', () => {
  let saved: string | undefined;
  beforeEach(() => {
    saved = process.env[ENV];
    delete process.env[ENV];
  });
  afterEach(() => {
    if (saved === undefined) delete process.env[ENV];
    else process.env[ENV] = saved;
  });

  function bus() {
    return { publishGeneric: mock(async () => {}) } as unknown as EventBus & {
      publishGeneric: ReturnType<typeof mock>;
    };
  }

  test('access != all with the env off is a ValidationError naming the env var, before any DB call', async () => {
    for (const access of ['allowed', 'denied'] as const) {
      const pool = mockPool(consumerRow());
      const service = new EventConsumerService(pool.db, bus());
      const error = await service.create({ name: 'c1', eventType: 'message.received', access }).catch((e) => e);
      expect(error).toBeInstanceOf(ValidationError);
      expect(String(error.message)).toContain(ENV);
      expect(pool.select).not.toHaveBeenCalled();
      expect(pool.insert).not.toHaveBeenCalled();
    }
    process.env[ENV] = 'false';
    const pool = mockPool(consumerRow());
    const error = await new EventConsumerService(pool.db)
      .create({ name: 'c1', eventType: 'x', access: 'denied' })
      .catch((e) => e);
    expect(error).toBeInstanceOf(ValidationError);
  });

  test("default and 'all' store accessFilter 'all' with the env off; created event payload unchanged", async () => {
    for (const access of [undefined, 'all'] as const) {
      const pool = mockPool(consumerRow(), 5);
      const eventBus = bus();
      const created = await new EventConsumerService(pool.db, eventBus).create({
        name: 'c1',
        eventType: 'message.received',
        ...(access ? { access } : {}),
      });
      expect(pool.inserted[0]).toMatchObject({ accessFilter: 'all', cursor: 5 });
      expect(created.accessFilter).toBe('all');
      expect(eventBus.publishGeneric).toHaveBeenCalledWith('system.consumer.created', {
        consumerId: created.id,
        name: 'c1',
        eventType: 'message.received',
        cursor: 5,
        shared: false,
      });
    }
  });

  test("with the env on, 'allowed' is stored", async () => {
    process.env[ENV] = 'true';
    const pool = mockPool(consumerRow(), 5);
    const created = await new EventConsumerService(pool.db).create({
      name: 'c1',
      eventType: 'message.received',
      access: 'allowed',
    });
    expect(pool.inserted[0]).toMatchObject({ accessFilter: 'allowed' });
    expect(created.accessFilter).toBe('allowed');
  });
});
