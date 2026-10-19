/**
 * Durable event consumer routes (#989, RFC #925 G7).
 *
 * Register / list / inspect / delete named consumers over the event journal,
 * plus the delivery surface: `pull` (page from the stored cursor, optional
 * long-poll) and `ack` (advance the cursor, monotonic). Backs
 * `omni events consumers ...` and `omni events follow --consumer <name>`.
 * `stream` is the opt-in SSE push over the same scan (read-only: it never
 * moves the cursor; see services/event-consumer-stream.ts).
 *
 * Mounted at root BEFORE `/events` (see routes/v2/index.ts): eventsRoutes
 * carries a `/:id` catch-all that would otherwise swallow `/events/consumers`
 * — same invariant as eventSchemasRoutes (#959).
 */

import { zValidator } from '@hono/zod-validator';
import { createLogger } from '@omni/core';
import { Hono } from 'hono';
import { streamSSE } from 'hono/streaming';
import {
  AckConsumerSchema,
  CreateConsumerSchema,
  PullConsumerQuerySchema,
  StreamConsumerHeaderSchema,
  StreamConsumerQuerySchema,
} from '../../schemas/openapi/event-consumers';
import {
  DEFAULT_STREAM_HEARTBEAT_MS,
  DEFAULT_STREAM_POLL_FALLBACK_MS,
  consumerStreamWakeHub,
  resolveStreamIntervalMs,
  resolveStreamStart,
  runConsumerStream,
  wakePrefixesFor,
} from '../../services/event-consumer-stream';
import { currentTenantScope, runDetachedFromTenantScope } from '../../tenancy/tenant-scope';
import type { AppVariables } from '../../types';

const log = createLogger('event-consumer-stream');

const eventConsumersRoutes = new Hono<{ Variables: AppVariables }>();

/**
 * GET /events/consumers - List durable consumers (with live lag)
 */
eventConsumersRoutes.get('/events/consumers', async (c) => {
  const services = c.get('services');
  const items = await services.eventConsumers.list();
  return c.json({ items });
});

/**
 * POST /events/consumers - Register a durable consumer
 */
eventConsumersRoutes.post('/events/consumers', zValidator('json', CreateConsumerSchema), async (c) => {
  const input = c.req.valid('json');
  const services = c.get('services');
  const data = await services.eventConsumers.create(input);
  return c.json({ data }, 201);
});

/**
 * GET /events/consumers/:name - Inspect one consumer (filter + cursor + lag)
 */
eventConsumersRoutes.get('/events/consumers/:name', async (c) => {
  const name = c.req.param('name');
  const services = c.get('services');
  const data = await services.eventConsumers.inspect(name);
  return c.json({ data });
});

/**
 * DELETE /events/consumers/:name - Remove a consumer registration
 */
eventConsumersRoutes.delete('/events/consumers/:name', async (c) => {
  const name = c.req.param('name');
  const services = c.get('services');
  await services.eventConsumers.delete(name);
  return c.json({ success: true });
});

/**
 * POST /events/consumers/:name/pull - Page events from the stored cursor
 *
 * Read-only w.r.t. the cursor (ack commits progress), but POST: a pull with
 * waitMs long-polls server-side, and the paging contract ("strictly after the
 * stored cursor") is consumer state, not a cacheable resource read.
 */
eventConsumersRoutes.post('/events/consumers/:name/pull', zValidator('query', PullConsumerQuerySchema), async (c) => {
  const name = c.req.param('name');
  const { limit, waitMs, leaseMs } = c.req.valid('query');
  const services = c.get('services');
  const result = await services.eventConsumers.pull(name, { limit, waitMs, leaseMs });
  return c.json(result);
});

/**
 * POST /events/consumers/:name/ack - Advance the cursor (monotonic)
 */
eventConsumersRoutes.post('/events/consumers/:name/ack', zValidator('json', AckConsumerSchema), async (c) => {
  const name = c.req.param('name');
  const { cursor, leaseId } = c.req.valid('json');
  const services = c.get('services');
  const data = await services.eventConsumers.ack(name, cursor ?? 0, leaseId);
  return c.json({ data });
});

/**
 * GET /events/consumers/:name/stream - SSE push from the journal (opt-in)
 *
 * Read-only w.r.t. the consumer: the position lives in memory, the stored
 * cursor is never written and no lease is taken (shared consumers are
 * refused). Clients keep acking with POST /events/consumers/:name/ack.
 */
eventConsumersRoutes.get(
  '/events/consumers/:name/stream',
  zValidator('query', StreamConsumerQuerySchema),
  zValidator('header', StreamConsumerHeaderSchema),
  async (c) => {
    const name = c.req.param('name');
    const { after } = c.req.valid('query');
    const lastEventId = c.req.valid('header')['last-event-id'];
    const services = c.get('services');
    // 404 / 400 (shared) while a JSON error can still be returned.
    const consumer = await services.eventConsumers.getStreamable(name);
    const start = resolveStreamStart({
      lastEventId: lastEventId === undefined ? undefined : Number(lastEventId),
      after: after === undefined ? undefined : Number(after),
      cursor: consumer.cursor,
    });
    // The request's transaction commits once this handler returns; the stream
    // re-enters the tenant per page (scanForStream) instead of inheriting it.
    const tenantId = currentTenantScope()?.tenantId ?? null;
    const heartbeatMs = resolveStreamIntervalMs(process.env.EVENT_STREAM_HEARTBEAT_MS, DEFAULT_STREAM_HEARTBEAT_MS);
    const fallbackMs = resolveStreamIntervalMs(process.env.STREAM_POLL_FALLBACK_MS, DEFAULT_STREAM_POLL_FALLBACK_MS);

    return streamSSE(c, (stream) =>
      runDetachedFromTenantScope(async () => {
        const disconnect = new AbortController();
        stream.onAbort(() => disconnect.abort());
        const write = async (chunk: () => Promise<void>) => {
          try {
            await chunk();
          } catch (error) {
            disconnect.abort(); // the client is gone
            throw error;
          }
        };

        try {
          await write(() =>
            stream.writeSSE({ event: 'connected', data: JSON.stringify({ consumer: consumer.name, after: start }) }),
          );
        } catch {
          return; // disconnected before the first byte
        }
        await runConsumerStream({
          start,
          scan: (position) => services.eventConsumers.scanForStream(name, position, tenantId),
          send: (row) =>
            write(() =>
              stream.writeSSE({ id: String(row.journalSeq), event: row.eventType, data: JSON.stringify(row) }),
            ),
          heartbeat: () =>
            write(async () => {
              await stream.write(`: heartbeat ${Date.now()}\n\n`);
            }),
          close: (error) =>
            write(() =>
              stream.writeSSE({ event: 'error', data: JSON.stringify({ consumer: name, message: error.message }) }),
            ),
          subscribeWake: (wake) => consumerStreamWakeHub.register(wakePrefixesFor(consumer.eventType), wake),
          signal: disconnect.signal,
          heartbeatMs,
          fallbackMs,
          onError: (error) => log.warn('Consumer stream scan failed; retrying', { name, error: String(error) }),
        });
      }),
    );
  },
);

export { eventConsumersRoutes };
