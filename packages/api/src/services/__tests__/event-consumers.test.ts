/**
 * Durable event consumers — pure-logic units (#989).
 *
 * The DB-bound contract (paging, ack monotonicity against real rows, RLS
 * containment) lives in `__tests__/event-consumers-postgres.test.ts`; these
 * cover the pure pieces: the payload matcher (which MUST be the events-wait /
 * automation matcher), initial-cursor resolution, and lag math.
 */

import { describe, expect, test } from 'bun:test';
import type { SharedLeaseState } from '@omni/db';
import {
  type ConsumerCaller,
  canAccessConsumer,
  claimLease,
  computeLag,
  matchesConsumerPayload,
  planSharedPull,
  releaseLease,
  resolveConsumerScope,
  resolveInitialCursor,
} from '../event-consumers';

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

describe('instance-scoped consumers (opt-in, 0080)', () => {
  const INST_A = '11111111-1111-4111-8111-1111111111aa';
  const INST_B = '11111111-1111-4111-8111-1111111111bb';
  const INST_C = '11111111-1111-4111-8111-1111111111cc';
  const KEY_RESTRICTED = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
  const KEY_ADMIN = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
  const restricted: ConsumerCaller = { keyId: KEY_RESTRICTED, instanceIds: [INST_A, INST_B] };
  const otherRestricted: ConsumerCaller = { keyId: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc', instanceIds: [INST_A] };
  const unrestricted: ConsumerCaller = { keyId: KEY_ADMIN, instanceIds: null };
  const callers = { restricted, otherRestricted, unrestricted, keyless: { keyId: null, instanceIds: null } };

  /** Run `fn` and return the thrown error's code (or 'ok'). */
  function codeOf(fn: () => unknown): string {
    try {
      fn();
      return 'ok';
    } catch (error) {
      return (error as { code?: string }).code ?? 'unknown';
    }
  }

  test('no new option: identical behaviour — an unscoped create resolves to the legacy columns for every key', () => {
    for (const caller of [...Object.values(callers), undefined]) {
      expect(resolveConsumerScope({}, caller)).toEqual({ scoped: false, instanceIds: null, ownerKeyId: null });
      expect(resolveConsumerScope({ scoped: false }, caller)).toEqual({
        scoped: false,
        instanceIds: null,
        ownerKeyId: null,
      });
    }
  });

  test('no new option: identical behaviour — an unscoped consumer is accessible to every caller', () => {
    const consumer = { scoped: false, ownerKeyId: null };
    for (const caller of [...Object.values(callers), undefined]) {
      expect(canAccessConsumer(consumer, caller)).toBe(true);
    }
  });

  test('instanceIds without scoped: true is a 400 (never silently ignored)', () => {
    expect(codeOf(() => resolveConsumerScope({ instanceIds: [INST_A] }, restricted))).toBe('VALIDATION');
    expect(codeOf(() => resolveConsumerScope({ scoped: false, instanceIds: [INST_A] }, unrestricted))).toBe(
      'VALIDATION',
    );
    expect(codeOf(() => resolveConsumerScope({ instanceIds: [] }, undefined))).toBe('VALIDATION');
  });

  test('restricted key: omitted instanceIds = all of the key instances; owner = calling key', () => {
    expect(resolveConsumerScope({ scoped: true }, restricted)).toEqual({
      scoped: true,
      instanceIds: [INST_A, INST_B],
      ownerKeyId: KEY_RESTRICTED,
    });
  });

  test('restricted key: requested subset is kept (deduplicated)', () => {
    expect(resolveConsumerScope({ scoped: true, instanceIds: [INST_B, INST_B] }, restricted)).toEqual({
      scoped: true,
      instanceIds: [INST_B],
      ownerKeyId: KEY_RESTRICTED,
    });
  });

  test('restricted key: an instance outside the key is 403, an empty intersection is 400', () => {
    expect(codeOf(() => resolveConsumerScope({ scoped: true, instanceIds: [INST_A, INST_C] }, restricted))).toBe(
      'FORBIDDEN',
    );
    expect(codeOf(() => resolveConsumerScope({ scoped: true, instanceIds: [] }, restricted))).toBe('VALIDATION');
    expect(codeOf(() => resolveConsumerScope({ scoped: true }, { keyId: 'k', instanceIds: [] }))).toBe('VALIDATION');
  });

  test('unrestricted key: instanceIds is required and non-empty', () => {
    expect(codeOf(() => resolveConsumerScope({ scoped: true }, unrestricted))).toBe('VALIDATION');
    expect(codeOf(() => resolveConsumerScope({ scoped: true, instanceIds: [] }, unrestricted))).toBe('VALIDATION');
    expect(resolveConsumerScope({ scoped: true, instanceIds: [INST_C] }, unrestricted)).toEqual({
      scoped: true,
      instanceIds: [INST_C],
      ownerKeyId: KEY_ADMIN,
    });
  });

  test('a scoped consumer needs a calling key (with a uuid id) to own it', () => {
    expect(
      codeOf(() =>
        resolveConsumerScope({ scoped: true, instanceIds: [INST_A] }, { keyId: 'worker-x', instanceIds: null }),
      ),
    ).toBe('VALIDATION');
    expect(codeOf(() => resolveConsumerScope({ scoped: true, instanceIds: [INST_A] }, callers.keyless))).toBe(
      'VALIDATION',
    );
    expect(codeOf(() => resolveConsumerScope({ scoped: true, instanceIds: [INST_A] }, undefined))).toBe('VALIDATION');
  });

  test('a scoped consumer is visible to its owner and unrestricted keys only', () => {
    const consumer = { scoped: true, ownerKeyId: KEY_RESTRICTED };
    expect(canAccessConsumer(consumer, restricted)).toBe(true);
    expect(canAccessConsumer(consumer, unrestricted)).toBe(true);
    expect(canAccessConsumer(consumer, otherRestricted)).toBe(false);
    expect(canAccessConsumer(consumer, { keyId: null, instanceIds: [INST_A] })).toBe(false);
    // An orphaned scoped consumer (owner key gone) stays reachable by unrestricted keys only.
    expect(canAccessConsumer({ scoped: true, ownerKeyId: null }, unrestricted)).toBe(true);
    expect(canAccessConsumer({ scoped: true, ownerKeyId: null }, { keyId: null, instanceIds: [] })).toBe(false);
  });
});
