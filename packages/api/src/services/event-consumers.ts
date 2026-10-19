/**
 * Durable event consumers (#989, RFC #925 G7 — the durable half; the one-shot
 * half is `omni events wait`, PR #972).
 *
 * A registered consumer is a name + filter + cursor over the JOURNAL
 * (`omni_events`, totally ordered by `journal_seq`). Delivery is
 * at-least-once-with-cursor: `pull` pages events strictly after the stored
 * cursor, `ack` advances it monotonically, disconnect/re-pull resumes exactly
 * after the last ack. The journal is the source of truth (NATS is transport),
 * so offsets older than NATS retention are automatically servable.
 *
 * Filter semantics REUSE the `events wait` contract (#966): the type filter
 * is exact-match or a trailing-* prefix glob (same as EventService.list), and
 * payload conditions run through the automation matcher
 * (`evaluateConditions`, @omni/core) against the row's rawPayload.
 *
 * The `pull` cursor advances over SCANNED rows, not merely matched ones —
 * acking the returned cursor skips filtered-out rows, so a sparse filter
 * never re-scans the same journal range. This is also what keeps the model
 * composable with G6 (waits/continuations): "wake a suspended run when a
 * matching event arrives" is `pull(limit: 1)` from a stored cursor with the
 * same matcher — nothing here assumes a live socket or a NATS subscription.
 *
 * Fan-out vs shared (#1188): by default every consumer NAME owns one cursor,
 * so two names on the same type each see every event (observers). A `shared`
 * consumer is a competing-consumer queue: each pull LEASES the next page to
 * that puller alone and the claimed watermark moves past it; ack(leaseId)
 * releases the lease and the cursor advances to the oldest outstanding lease.
 * An unacked lease expires and is redelivered — at-least-once, per page (pull
 * with limit 1 for per-message ack). Leases live on the consumer row and are
 * updated compare-and-set, so concurrent pullers on any replica never share
 * a page.
 *
 * Instance scoping (opt-in, 0080): a consumer created with `scoped: true`
 * stores its owner key and an instance set. Its pull only pages journal rows
 * of those instances (in SQL, inside the same scan, so the scanned cursor and
 * the empty-page advance still move past filtered rows), and only the owner
 * key or an instance-unrestricted key can see, pull, ack or delete it —
 * anyone else gets the same 404 as a missing name. Unscoped consumers (the
 * default) behave exactly as before for every key.
 */

import { randomUUID } from 'node:crypto';
import { ConflictError, ERROR_CODES, NotFoundError, OmniError, ValidationError, evaluateConditions } from '@omni/core';
import type { EventBus } from '@omni/core';
import type {
  AutomationCondition,
  ConsumerLease,
  Database,
  DurableConsumer,
  OmniEvent,
  SharedLeaseState,
} from '@omni/db';
import { durableConsumers, omniEvents } from '@omni/db';
import { type SQL, and, asc, eq, gt, inArray, isNull, lte, or, sql } from 'drizzle-orm';
import { scopedHandle } from '../tenancy/tenant-scope';
import { eventTypeFilterClause } from './events';

/** Where a new consumer's cursor starts: the journal head (default) or 0 (full replay). */
export type ConsumerStartFrom = 'now' | 'beginning';

export interface CreateConsumerInput {
  name: string;
  eventType: string;
  /** Type globs to drop (same syntax as eventType); exclusion wins (#1078). */
  excludeTypes?: string[];
  filters?: AutomationCondition[];
  /** 'now' (default, matches `events wait`) or 'beginning' (projection/backfill replay). */
  startFrom?: ConsumerStartFrom;
  /** Competing consumers: pullers on this name split the stream (#1188). Default false = fan-out. */
  shared?: boolean;
  /** Opt-in instance scoping + ownership (0080). Default false = today's global consumer. */
  scoped?: boolean;
  /** Scoped consumers only: instance UUIDs to page. Rejected without `scoped: true`. */
  instanceIds?: string[];
}

/**
 * The calling API key as the consumer access rules see it. Routes derive it
 * from the request's key; internal callers that pass none keep the legacy
 * unrestricted behaviour.
 */
export interface ConsumerCaller {
  /** api_keys.id of the caller; null when the request carried no key. */
  keyId: string | null;
  /** The key's instance restriction; null = every instance (ApiKeyService.instanceAllowed's rule). */
  instanceIds: string[] | null;
}

/** The ownership columns a create writes: all null/false unless the consumer is scoped. */
export type ConsumerScope =
  | { scoped: false; instanceIds: null; ownerKeyId: null }
  | { scoped: true; instanceIds: string[]; ownerKeyId: string };

/** A consumer row plus its live lag (journal head minus cursor). */
export interface ConsumerWithLag extends DurableConsumer {
  /** Highest journal_seq currently in the journal (0 when empty). */
  head: number;
  /** head - cursor, floored at 0. Counts ALL journal rows past the cursor, not only matching ones. */
  lag: number;
  /** No type-matching journal row past the cursor — nothing left to scan (lag may still be > 0). */
  caughtUp: boolean;
}

export interface PullOptions {
  /** Max events to SCAN in this page (matches are <= this). 1..500, default 100. */
  limit?: number;
  /** Long-poll: when the page is empty, wait up to this long for new rows. 0..30000ms. */
  waitMs?: number;
  /** Shared consumers: how long the leased page stays with this puller before redelivery. */
  leaseMs?: number;
}

export interface PullResult {
  consumer: string;
  /** Rows matching the type filter AND payload conditions, ascending journal_seq. */
  items: OmniEvent[];
  /**
   * Highest journal_seq SCANNED (not merely matched). Ack THIS value: it
   * advances past filtered-out rows so a sparse filter never re-scans.
   * Equals the stored cursor when nothing new was scanned.
   */
  cursor: number;
  head: number;
  /** True when the scan filled the page — more rows are already waiting. */
  hasMore: boolean;
  /**
   * True when a full scan window matched nothing (#1128). The stored cursor
   * was already advanced past those rows, so the next pull scans new ground.
   */
  scanExhausted: boolean;
  /** Shared consumers: ack with this id (not the cursor). Absent when nothing was delivered. */
  leaseId?: string;
}

const PULL_POLL_INTERVAL_MS = 500;
const PULL_MAX_LIMIT = 500;
const PULL_MAX_WAIT_MS = 30_000;
const DEFAULT_LEASE_MS = 60_000;
const SHARED_CAS_ATTEMPTS = 20;

/** Where a shared puller goes next: redeliver an expired lease, or claim new rows after `after`. */
export function planSharedPull(
  state: SharedLeaseState,
  now: number,
): { reclaim: ConsumerLease; after?: never } | { reclaim?: never; after: number } {
  const expired = state.leases.find((lease) => lease.expiresAt <= now);
  return expired ? { reclaim: expired } : { after: state.claimed };
}

/** State after leasing (from, to] to a puller (replacing `replaces` on redelivery). */
export function claimLease(state: SharedLeaseState, lease: ConsumerLease, replaces?: string): SharedLeaseState {
  return {
    claimed: Math.max(state.claimed, lease.to),
    leases: [...state.leases.filter((l) => l.id !== replaces), lease],
  };
}

/**
 * State + cursor after acking a lease, or null when the lease is gone (it
 * expired and was redelivered). The cursor is the oldest outstanding lease
 * start, or the claimed watermark when nothing is outstanding.
 */
export function releaseLease(
  state: SharedLeaseState,
  leaseId: string,
): { state: SharedLeaseState; cursor: number } | null {
  if (!state.leases.some((l) => l.id === leaseId)) return null;
  const leases = state.leases.filter((l) => l.id !== leaseId);
  const cursor = leases.length ? Math.min(...leases.map((l) => l.from)) : state.claimed;
  return { state: { claimed: state.claimed, leases }, cursor };
}

/** True when the row's payload passes the consumer's conditions (wait/automation matcher). */
export function matchesConsumerPayload(
  row: Pick<OmniEvent, 'rawPayload'>,
  filters?: AutomationCondition[] | null,
): boolean {
  if (!filters || filters.length === 0) return true;
  return evaluateConditions(filters, row.rawPayload ?? {});
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** The caller's key id when it can own a consumer (owner_key_id is a uuid column), else null. */
function ownerKeyOf(caller?: ConsumerCaller): string | null {
  const keyId = caller?.keyId ?? null;
  return keyId && UUID_RE.test(keyId) ? keyId : null;
}

/**
 * Resolve the scope columns for a create. Unscoped (the default) is exactly
 * the pre-0080 consumer; `instanceIds` without `scoped: true` is refused
 * rather than silently ignored. Scoped: an instance-restricted key gets the
 * requested instances (all of its own when omitted) and may not name an
 * instance outside its set (403); an unrestricted key must name a non-empty
 * set. An empty result is a 400. Exported for unit tests.
 */
export function resolveConsumerScope(
  input: Pick<CreateConsumerInput, 'scoped' | 'instanceIds'>,
  caller?: ConsumerCaller,
): ConsumerScope {
  if (!input.scoped) {
    if (input.instanceIds !== undefined) {
      throw new ValidationError('instanceIds is only valid with scoped: true', {
        instanceIds: ['requires scoped: true'],
      });
    }
    return { scoped: false, instanceIds: null, ownerKeyId: null };
  }

  const ownerKeyId = ownerKeyOf(caller);
  if (!ownerKeyId) {
    throw new ValidationError('a scoped consumer needs an API key to own it', { scoped: ['no calling API key'] });
  }

  const requested = input.instanceIds ? [...new Set(input.instanceIds)] : undefined;
  const keyInstances = caller?.instanceIds ?? null;
  if (keyInstances === null) {
    if (!requested?.length) {
      throw new ValidationError('a scoped consumer on an instance-unrestricted key needs a non-empty instanceIds', {
        instanceIds: ['required (non-empty) for an unrestricted key'],
      });
    }
    return { scoped: true, instanceIds: requested, ownerKeyId };
  }

  const outside = requested?.filter((id) => !keyInstances.includes(id)) ?? [];
  if (outside.length > 0) {
    throw new OmniError({
      code: ERROR_CODES.FORBIDDEN,
      message: 'API key does not have access to this instance',
      context: { instanceIds: outside },
      recoverable: false,
    });
  }
  const instanceIds = requested ?? [...new Set(keyInstances)];
  if (instanceIds.length === 0) {
    throw new ValidationError('a scoped consumer needs at least one instance the API key can access', {
      instanceIds: ['empty after intersecting with the API key instances'],
    });
  }
  return { scoped: true, instanceIds, ownerKeyId };
}

/**
 * Whether `caller` may see/pull/ack/delete `consumer`. Unscoped consumers and
 * callers with no key context are unrestricted (legacy behaviour); a scoped
 * consumer is visible to its owner key and to instance-unrestricted keys.
 * Exported for unit tests.
 */
export function canAccessConsumer(
  consumer: Pick<DurableConsumer, 'scoped' | 'ownerKeyId'>,
  caller?: ConsumerCaller,
): boolean {
  if (!consumer.scoped || !caller || caller.instanceIds === null) return true;
  return consumer.ownerKeyId !== null && consumer.ownerKeyId === caller.keyId;
}

/** SQL twin of canAccessConsumer for UPDATE/DELETE by name; undefined = no extra predicate. */
function consumerAccessClause(caller?: ConsumerCaller): SQL | undefined {
  if (!caller || caller.instanceIds === null) return undefined;
  const unscoped = eq(durableConsumers.scoped, false);
  const keyId = ownerKeyOf(caller);
  return keyId ? or(unscoped, eq(durableConsumers.ownerKeyId, keyId)) : unscoped;
}

/** A scoped consumer pages only its instances' rows; an empty set fails closed. */
function instanceScopeClause(consumer: Pick<DurableConsumer, 'scoped' | 'instanceIds'>): SQL | undefined {
  if (!consumer.scoped) return undefined;
  return consumer.instanceIds?.length ? inArray(omniEvents.instanceId, consumer.instanceIds) : sql`false`;
}

/** Initial cursor for a new consumer. Exported for unit tests. */
export function resolveInitialCursor(startFrom: ConsumerStartFrom, head: number): number {
  return startFrom === 'beginning' ? 0 : head;
}

/** Lag math: journal head minus cursor, floored at 0. Exported for unit tests. */
export function computeLag(head: number, cursor: number): number {
  return Math.max(0, head - cursor);
}

export class EventConsumerService {
  /**
   * The handle every query in this service uses: the request's tenant-stamped
   * transaction inside a tenant scope, the ambient pool otherwise (G4 —
   * `tenancy/tenant-scope.ts`). Under RLS enforcement this is what keeps
   * `pull` tenant-policed: a tenant-scoped request only pages its own
   * tenant's `omni_events` rows.
   */
  private get db(): Database {
    return scopedHandle(this.pool);
  }

  constructor(
    private readonly pool: Database,
    private readonly eventBus: EventBus | null = null,
  ) {}

  /** Highest journal_seq in the journal, 0 when empty. Cheap (backward index scan). */
  async head(): Promise<number> {
    const [row] = await this.db
      .select({ head: sql<number>`coalesce(max(${omniEvents.journalSeq}), 0)::bigint` })
      .from(omniEvents);
    return Number(row?.head ?? 0);
  }

  /** Every consumer the caller may see (all of them without a caller), with live lag. */
  async list(caller?: ConsumerCaller): Promise<ConsumerWithLag[]> {
    const [rows, head] = await Promise.all([
      this.db.select().from(durableConsumers).orderBy(asc(durableConsumers.name)),
      this.head(),
    ]);
    const visible = rows.filter((row) => canAccessConsumer(row, caller));
    return Promise.all(visible.map((row) => this.withLag(row, head)));
  }

  async getByName(name: string): Promise<DurableConsumer> {
    const [row] = await this.db.select().from(durableConsumers).where(eq(durableConsumers.name, name)).limit(1);
    if (!row) throw new NotFoundError('DurableConsumer', name);
    return row;
  }

  /** getByName, but a scoped consumer the caller may not access is the same 404 as a missing one. */
  private async getVisible(name: string, caller?: ConsumerCaller): Promise<DurableConsumer> {
    const row = await this.getByName(name);
    if (!canAccessConsumer(row, caller)) throw new NotFoundError('DurableConsumer', name);
    return row;
  }

  /** getByName plus live head/lag — the `inspect` surface. */
  async inspect(name: string, caller?: ConsumerCaller): Promise<ConsumerWithLag> {
    const row = await this.getVisible(name, caller);
    return this.withLag(row, await this.head());
  }

  private async withLag(row: DurableConsumer, head: number): Promise<ConsumerWithLag> {
    const caughtUp = row.cursor >= head || (await this.scanPage(row, 1)).length === 0;
    return { ...row, head, lag: computeLag(head, row.cursor), caughtUp };
  }

  async create(input: CreateConsumerInput, caller?: ConsumerCaller): Promise<ConsumerWithLag> {
    const scope = resolveConsumerScope(input, caller);
    const head = await this.head();
    const cursor = resolveInitialCursor(input.startFrom ?? 'now', head);

    const [created] = await this.db
      .insert(durableConsumers)
      .values({
        name: input.name,
        eventType: input.eventType,
        excludeTypes: input.excludeTypes?.length ? input.excludeTypes : null,
        filters: input.filters ?? null,
        cursor,
        shared: input.shared ?? false,
        leaseState: input.shared ? { claimed: cursor, leases: [] } : null,
        ...(scope.scoped ? scope : {}),
      })
      .onConflictDoNothing({ target: durableConsumers.name })
      .returning();
    if (!created) {
      throw new ConflictError('DurableConsumer', `a consumer named '${input.name}' already exists`, {
        name: input.name,
      });
    }

    // State change → event (repo contract). webhook_sources CRUD publishes
    // nothing; the local pattern followed here is the agents CRUD one
    // (`system.agent.registered`). system.* events live on the bus, not in
    // the journal — journaling every registry mutation would be noise.
    if (this.eventBus) {
      await this.eventBus.publishGeneric('system.consumer.created', {
        consumerId: created.id,
        name: created.name,
        eventType: created.eventType,
        cursor: created.cursor,
        shared: created.shared,
        ...(created.scoped ? { scoped: true, instanceIds: created.instanceIds, ownerKeyId: created.ownerKeyId } : {}),
      });
    }

    return this.withLag(created, head);
  }

  async delete(name: string, caller?: ConsumerCaller): Promise<void> {
    const [deleted] = await this.db
      .delete(durableConsumers)
      .where(and(eq(durableConsumers.name, name), consumerAccessClause(caller)))
      .returning();
    if (!deleted) throw new NotFoundError('DurableConsumer', name);

    if (this.eventBus) {
      await this.eventBus.publishGeneric('system.consumer.deleted', {
        consumerId: deleted.id,
        name: deleted.name,
        eventType: deleted.eventType,
        cursor: deleted.cursor,
      });
    }
  }

  /**
   * Page journal events strictly after the stored cursor. Does NOT advance
   * the cursor past delivered rows — that is `ack`'s job (at-least-once: a
   * client that crashes mid-page re-pulls the same page).
   *
   * Exception (#1128): a page that scanned rows but matched NONE delivers
   * nothing, so the cursor is advanced past it here. Otherwise a client that
   * never acks an empty page (peek, or acking only delivered items) re-scans
   * the same window forever once non-matching rows exceed `limit`.
   */
  async pull(name: string, options: PullOptions = {}, caller?: ConsumerCaller): Promise<PullResult> {
    const limit = Math.min(Math.max(options.limit ?? 100, 1), PULL_MAX_LIMIT);
    const waitMs = Math.min(Math.max(options.waitMs ?? 0, 0), PULL_MAX_WAIT_MS);
    const consumer = await this.getVisible(name, caller);
    if (consumer.shared) return this.pullShared(consumer, limit, waitMs, options.leaseMs ?? DEFAULT_LEASE_MS);

    const deadline = Date.now() + waitMs;
    let scanned = await this.scanPage(consumer, limit);
    while (scanned.length === 0 && Date.now() < deadline) {
      await new Promise<void>((resolve) => setTimeout(resolve, PULL_POLL_INTERVAL_MS));
      scanned = await this.scanPage(consumer, limit);
    }

    const items = scanned.filter((row) => matchesConsumerPayload(row, consumer.filters));
    const last = scanned[scanned.length - 1];
    const cursor = last ? Number(last.journalSeq) : consumer.cursor;
    if (items.length === 0 && cursor > consumer.cursor) {
      // Monotonic (lte guard): a concurrent ack further ahead wins.
      await this.db
        .update(durableConsumers)
        .set({ cursor, updatedAt: new Date() })
        .where(and(eq(durableConsumers.name, name), lte(durableConsumers.cursor, cursor)));
    }
    const head = await this.head();
    const hasMore = scanned.length === limit;
    return {
      consumer: consumer.name,
      items,
      cursor,
      head,
      hasMore,
      scanExhausted: hasMore && items.length === 0,
    };
  }

  /**
   * Advance the cursor. Monotonic: an ack equal to the stored cursor is an
   * idempotent no-op (client retry), an ack BEHIND it is refused with a 400 —
   * a durable cursor never moves backwards.
   */
  async ack(name: string, cursor: number, leaseId?: string, caller?: ConsumerCaller): Promise<ConsumerWithLag> {
    if (leaseId) {
      await this.getVisible(name, caller);
      return this.ackShared(name, leaseId);
    }

    const [updated] = await this.db
      .update(durableConsumers)
      .set({ cursor, updatedAt: new Date() })
      .where(
        and(
          eq(durableConsumers.name, name),
          eq(durableConsumers.shared, false),
          lte(durableConsumers.cursor, cursor),
          consumerAccessClause(caller),
        ),
      )
      .returning();

    if (!updated) {
      const existing = await this.getVisible(name, caller); // throws NotFoundError when absent or not visible
      if (existing.shared) {
        throw new ValidationError(`consumer '${name}' is shared — ack with the pull result's leaseId`, undefined, {
          name,
        });
      }
      throw new ValidationError(
        `ack cursor ${cursor} is behind the stored cursor ${existing.cursor} — acks are monotonic`,
        undefined,
        { name, cursor, storedCursor: existing.cursor },
      );
    }

    return this.withLag(updated, await this.head());
  }

  /** Shared pull: lease the next page (or an expired one) to this caller alone. */
  private async pullShared(
    initial: DurableConsumer,
    limit: number,
    waitMs: number,
    leaseMs: number,
  ): Promise<PullResult> {
    const deadline = Date.now() + waitMs;
    let claim = await this.claimShared(initial, limit, leaseMs);
    while (!claim.lease && Date.now() < deadline) {
      await new Promise<void>((resolve) => setTimeout(resolve, PULL_POLL_INTERVAL_MS));
      claim = await this.claimShared(await this.getByName(initial.name), limit, leaseMs);
    }

    const { consumer, lease, scanned } = claim;
    const items = scanned.filter((row) => matchesConsumerPayload(row, consumer.filters));
    if (lease && items.length === 0) {
      // Nothing to deliver: release now so the cursor moves past filtered-out rows (#1128).
      await this.ackShared(consumer.name, lease.id).catch(() => undefined);
    }
    const hasMore = scanned.length === limit;
    return {
      consumer: consumer.name,
      items,
      cursor: lease?.to ?? consumer.cursor,
      head: await this.head(),
      hasMore,
      scanExhausted: hasMore && items.length === 0,
      ...(lease && items.length > 0 ? { leaseId: lease.id } : {}),
    };
  }

  private async claimShared(
    start: DurableConsumer,
    limit: number,
    leaseMs: number,
  ): Promise<{ consumer: DurableConsumer; lease: ConsumerLease | null; scanned: OmniEvent[] }> {
    let consumer = start;
    for (let attempt = 0; attempt < SHARED_CAS_ATTEMPTS; attempt++) {
      const state = consumer.leaseState ?? { claimed: consumer.cursor, leases: [] };
      const now = Date.now();
      const plan = planSharedPull(state, now);
      const scanned = plan.reclaim
        ? await this.scanPage(consumer, PULL_MAX_LIMIT, plan.reclaim.from, plan.reclaim.to)
        : await this.scanPage(consumer, limit, plan.after);
      const last = scanned[scanned.length - 1];
      if (!plan.reclaim && !last) return { consumer, lease: null, scanned };

      const lease: ConsumerLease = plan.reclaim
        ? { ...plan.reclaim, id: randomUUID(), expiresAt: now + leaseMs }
        : { id: randomUUID(), from: plan.after, to: Number(last?.journalSeq), expiresAt: now + leaseMs };
      if (await this.casLeaseState(consumer, claimLease(state, lease, plan.reclaim?.id))) {
        return { consumer, lease, scanned };
      }
      consumer = await this.getByName(consumer.name);
    }
    throw new ConflictError('DurableConsumer', `too much contention leasing '${start.name}', retry`, {
      name: start.name,
    });
  }

  private async ackShared(name: string, leaseId: string): Promise<ConsumerWithLag> {
    for (let attempt = 0; attempt < SHARED_CAS_ATTEMPTS; attempt++) {
      const consumer = await this.getByName(name);
      if (!consumer.shared) {
        throw new ValidationError(`consumer '${name}' is not shared — ack with a cursor`, undefined, { name });
      }
      const released = releaseLease(consumer.leaseState ?? { claimed: consumer.cursor, leases: [] }, leaseId);
      if (!released) {
        throw new ValidationError(
          `lease ${leaseId} is unknown or expired — its page was (or will be) redelivered`,
          undefined,
          { name, leaseId },
        );
      }
      const updated = await this.casLeaseState(consumer, released.state, Math.max(consumer.cursor, released.cursor));
      if (updated) return this.withLag(updated, await this.head());
    }
    throw new ConflictError('DurableConsumer', `too much contention acking '${name}', retry`, { name });
  }

  /** Compare-and-set on lease_state: null when another puller changed it first. */
  private async casLeaseState(
    consumer: DurableConsumer,
    next: SharedLeaseState,
    cursor?: number,
  ): Promise<DurableConsumer | null> {
    const unchanged: SQL = consumer.leaseState
      ? sql`${durableConsumers.leaseState} = ${JSON.stringify(consumer.leaseState)}::jsonb`
      : isNull(durableConsumers.leaseState);
    const [updated] = await this.db
      .update(durableConsumers)
      .set({ leaseState: next, ...(cursor === undefined ? {} : { cursor }), updatedAt: new Date() })
      .where(and(eq(durableConsumers.id, consumer.id), unchanged))
      .returning();
    return updated ?? null;
  }

  /**
   * One ascending journal page after the consumer's cursor, pre-filtered by
   * the type filter in SQL (exact match, or trailing-* prefix glob — the
   * EventService.list / #966 contract) minus the exclude globs (#1078).
   * A scoped consumer (0080) also keeps only its instances' rows — in the
   * same query, so the page's last row (the scanned cursor) is a row it may
   * see, and the empty-page advance skips other instances' rows.
   */
  private async scanPage(
    consumer: DurableConsumer,
    limit: number,
    after = consumer.cursor,
    through?: number,
  ): Promise<OmniEvent[]> {
    const typeClause = eventTypeFilterClause([consumer.eventType], consumer.excludeTypes ?? undefined);

    return this.db
      .select()
      .from(omniEvents)
      .where(
        and(
          gt(omniEvents.journalSeq, after),
          through === undefined ? undefined : lte(omniEvents.journalSeq, through),
          typeClause,
          instanceScopeClause(consumer),
        ),
      )
      .orderBy(asc(omniEvents.journalSeq))
      .limit(limit);
  }
}
