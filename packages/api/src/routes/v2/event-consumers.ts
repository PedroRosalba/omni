/**
 * Durable event consumer routes (#989, RFC #925 G7).
 *
 * Register / list / inspect / delete named consumers over the event journal,
 * plus the delivery surface: `pull` (page from the stored cursor, optional
 * long-poll) and `ack` (advance the cursor, monotonic). Backs
 * `omni events consumers ...` and `omni events follow --consumer <name>`.
 *
 * Mounted at root BEFORE `/events` (see routes/v2/index.ts): eventsRoutes
 * carries a `/:id` catch-all that would otherwise swallow `/events/consumers`
 * — same invariant as eventSchemasRoutes (#959).
 *
 * Every handler passes the caller's key (id + instance restriction, read
 * the same way as the /events routes) to the service. It only matters for
 * consumers created with `scoped: true`; unscoped consumers ignore it.
 */

import { zValidator } from '@hono/zod-validator';
import { type Context, Hono } from 'hono';
import {
  AckConsumerSchema,
  CreateConsumerSchema,
  PullConsumerQuerySchema,
} from '../../schemas/openapi/event-consumers';
import type { ConsumerCaller } from '../../services/event-consumers';
import type { AppVariables } from '../../types';

const eventConsumersRoutes = new Hono<{ Variables: AppVariables }>();

/** The request's key as the consumer access rules see it; no key = unrestricted (events.ts rule). */
function consumerCaller(c: Context<{ Variables: AppVariables }>): ConsumerCaller {
  const apiKey = c.get('apiKey');
  return { keyId: apiKey?.id ?? null, instanceIds: apiKey?.instanceIds ?? null };
}

/**
 * GET /events/consumers - List durable consumers (with live lag)
 */
eventConsumersRoutes.get('/events/consumers', async (c) => {
  const services = c.get('services');
  const items = await services.eventConsumers.list(consumerCaller(c));
  return c.json({ items });
});

/**
 * POST /events/consumers - Register a durable consumer
 */
eventConsumersRoutes.post('/events/consumers', zValidator('json', CreateConsumerSchema), async (c) => {
  const input = c.req.valid('json');
  const services = c.get('services');
  const data = await services.eventConsumers.create(input, consumerCaller(c));
  return c.json({ data }, 201);
});

/**
 * GET /events/consumers/:name - Inspect one consumer (filter + cursor + lag)
 */
eventConsumersRoutes.get('/events/consumers/:name', async (c) => {
  const name = c.req.param('name');
  const services = c.get('services');
  const data = await services.eventConsumers.inspect(name, consumerCaller(c));
  return c.json({ data });
});

/**
 * DELETE /events/consumers/:name - Remove a consumer registration
 */
eventConsumersRoutes.delete('/events/consumers/:name', async (c) => {
  const name = c.req.param('name');
  const services = c.get('services');
  await services.eventConsumers.delete(name, consumerCaller(c));
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
  const result = await services.eventConsumers.pull(name, { limit, waitMs, leaseMs }, consumerCaller(c));
  return c.json(result);
});

/**
 * POST /events/consumers/:name/ack - Advance the cursor (monotonic)
 */
eventConsumersRoutes.post('/events/consumers/:name/ack', zValidator('json', AckConsumerSchema), async (c) => {
  const name = c.req.param('name');
  const { cursor, leaseId } = c.req.valid('json');
  const services = c.get('services');
  const data = await services.eventConsumers.ack(name, cursor ?? 0, leaseId, consumerCaller(c));
  return c.json({ data });
});

export { eventConsumersRoutes };
