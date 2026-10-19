/**
 * GET /events/consumers/:name/stream — route contract with a mocked service.
 *
 * Start-position precedence and validation, JSON errors before the stream
 * opens (404 / shared 400), the SSE wire format, and disconnect cleanup. The
 * scan loop itself is covered in services/__tests__/event-consumer-stream.test.ts.
 */

import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { NotFoundError, ValidationError } from '@omni/core';
import type { DurableConsumer, OmniEvent } from '@omni/db';
import { Hono } from 'hono';
import { errorHandler } from '../../../middleware/error';
import type { StreamScanResult } from '../../../services/event-consumers';
import type { AppVariables } from '../../../types';
import { eventConsumersRoutes } from '../event-consumers';

const CONSUMER = {
  id: '11111111-1111-4111-8111-111111111111',
  name: 'vox',
  eventType: 'message.received',
  excludeTypes: null,
  filters: null,
  cursor: 10,
  shared: false,
  waitForReady: false,
  leaseState: null,
  createdAt: new Date(),
  updatedAt: new Date(),
} as unknown as DurableConsumer;

function row(seq: number): OmniEvent {
  return {
    id: `evt-${seq}`,
    journalSeq: seq,
    eventType: 'message.received',
    rawPayload: { seq },
  } as unknown as OmniEvent;
}

function buildApp(options: { journal?: OmniEvent[]; getStreamable?: (name: string) => Promise<DurableConsumer> } = {}) {
  const journal = options.journal ?? [];
  const scans: number[] = [];
  const touched: string[] = [];
  const forbidden = (method: string) => async () => {
    touched.push(method);
    throw new Error(`${method} must not be called by the stream`);
  };
  const services = {
    eventConsumers: {
      getStreamable: options.getStreamable ?? (async () => CONSUMER),
      scanForStream: async (_name: string, after: number): Promise<StreamScanResult> => {
        scans.push(after);
        const rows = journal.filter((r) => r.journalSeq > after);
        const last = rows[rows.length - 1];
        return { items: rows, through: last ? last.journalSeq : null, held: false, full: false };
      },
      pull: forbidden('pull'),
      ack: forbidden('ack'),
    },
  } as unknown as AppVariables['services'];

  const app = new Hono<{ Variables: AppVariables }>();
  app.onError(errorHandler);
  app.use('*', async (c, next) => {
    c.set('services', services);
    await next();
  });
  app.route('/api/v2', eventConsumersRoutes);
  return { app, scans, touched };
}

/** Read SSE text until `until` matches (or the body ends), then cancel the body (client disconnect). */
async function readUntil(response: Response, until: RegExp): Promise<string> {
  const reader = response.body?.getReader();
  if (!reader) throw new Error('no body');
  const decoder = new TextDecoder();
  let text = '';
  while (!until.test(text)) {
    const { value, done } = await reader.read();
    if (done) break;
    text += decoder.decode(value);
  }
  await reader.cancel();
  return text;
}

const saved = { heartbeat: process.env.EVENT_STREAM_HEARTBEAT_MS, fallback: process.env.STREAM_POLL_FALLBACK_MS };
beforeEach(() => {
  process.env.EVENT_STREAM_HEARTBEAT_MS = '20';
  process.env.STREAM_POLL_FALLBACK_MS = '10';
});
afterEach(() => {
  for (const [key, value] of [
    ['EVENT_STREAM_HEARTBEAT_MS', saved.heartbeat],
    ['STREAM_POLL_FALLBACK_MS', saved.fallback],
  ] as const) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
});

describe('GET /api/v2/events/consumers/:name/stream', () => {
  test('streams matching rows as SSE events after a connected event, from the stored cursor', async () => {
    const { app, scans, touched } = buildApp({ journal: [row(9), row(11), row(12)] });
    const res = await app.request('/api/v2/events/consumers/vox/stream');
    expect(res.status).toBe(200);
    expect(res.headers.get('content-type')).toContain('text/event-stream');
    const text = await readUntil(res, /id: 12\n/);

    expect(text).toContain(`event: connected\ndata: ${JSON.stringify({ consumer: 'vox', after: 10 })}\n`);
    expect(text).toContain(`event: message.received\ndata: ${JSON.stringify(row(11))}\nid: 11\n\n`);
    expect(text).toContain('id: 12\n');
    expect(text).not.toContain('id: 9\n');
    expect(scans[0]).toBe(10);
    expect(touched).toEqual([]); // never pulls, never acks
  });

  test('?after overrides the stored cursor', async () => {
    const { app, scans } = buildApp({ journal: [row(11), row(12)] });
    const text = await readUntil(await app.request('/api/v2/events/consumers/vox/stream?after=11'), /id: 12\n/);
    expect(scans[0]).toBe(11);
    expect(text).not.toContain('id: 11\n');
  });

  test('Last-Event-ID overrides ?after', async () => {
    const { app, scans } = buildApp({ journal: [row(11), row(12), row(13)] });
    const res = await app.request('/api/v2/events/consumers/vox/stream?after=11', {
      headers: { 'Last-Event-ID': '12' },
    });
    const text = await readUntil(res, /id: 13\n/);
    expect(scans[0]).toBe(12);
    expect(text).not.toContain('id: 12\n');
  });

  test('a malformed ?after or Last-Event-ID is a 400 before the stream opens', async () => {
    const { app, scans } = buildApp();
    for (const after of ['-1', 'abc', '1.5', '1e3', '']) {
      const res = await app.request(`/api/v2/events/consumers/vox/stream?after=${after}`);
      expect(res.status).toBe(400);
    }
    const header = await app.request('/api/v2/events/consumers/vox/stream', { headers: { 'Last-Event-ID': 'x' } });
    expect(header.status).toBe(400);
    expect(scans).toEqual([]);
  });

  test('an unknown consumer is a 404 JSON error', async () => {
    const { app } = buildApp({
      getStreamable: async (name) => {
        throw new NotFoundError('DurableConsumer', name);
      },
    });
    const res = await app.request('/api/v2/events/consumers/nope/stream');
    expect(res.status).toBe(404);
  });

  test('a shared consumer is refused with a 400 JSON error', async () => {
    const { app } = buildApp({
      getStreamable: async (name) => {
        throw new ValidationError(`consumer '${name}' is shared — streams serve fan-out consumers only`);
      },
    });
    const res = await app.request('/api/v2/events/consumers/vox/stream');
    expect(res.status).toBe(400);
    expect(JSON.stringify(await res.json())).toContain('shared');
  });

  test('sends heartbeat comments and stops scanning once the client disconnects', async () => {
    const { app, scans } = buildApp();
    const text = await readUntil(await app.request('/api/v2/events/consumers/vox/stream'), /: heartbeat \d+\n\n/);
    expect(text).toMatch(/: heartbeat \d+\n\n/);
    await new Promise((resolve) => setTimeout(resolve, 30));
    const after = scans.length;
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(scans.length).toBe(after);
  });
});
