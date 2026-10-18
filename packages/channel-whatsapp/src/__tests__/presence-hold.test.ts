/**
 * Server-held presence (opt-in, POST /messages/send/presence hold=true).
 *
 * Time is driven with bun:test fake timers — no test here waits on the wall
 * clock. Sends use `outboundPacing: 'immediate'` so the humanized delay never
 * parks a send behind a (frozen) fake timer.
 */

import { afterEach, beforeEach, describe, expect, it, jest, mock } from 'bun:test';
import type { OutgoingMessage } from '@omni/channel-sdk';
import { WhatsAppPlugin } from '../plugin';

const INSTANCE_ID = 'hold-instance';
const PHONE = '5511888888888';
const PHONE_JID = '5511888888888@s.whatsapp.net';
const OTHER_JID = '5511777777777@s.whatsapp.net';

type Call = [kind: 'presence' | 'send', state: string, jid: string];

type HoldInternals = {
  sockets: Map<string, unknown>;
  eventBus: { publish: (type: string, payload: unknown) => Promise<void> };
  logger: Record<string, unknown>;
  presenceHolds: Map<string, unknown>;
};

function createMockSocket(calls: Call[], lidFor?: Record<string, string>) {
  return {
    user: { id: '5511999999999@s.whatsapp.net' },
    sendPresenceUpdate: mock(async (state: string, jid: string) => {
      calls.push(['presence', state, jid]);
    }),
    sendMessage: mock(async (jid: string) => {
      calls.push(['send', 'message', jid]);
      return { key: { id: `MSG-${calls.length}` }, message: { conversation: 'x' } };
    }),
    ev: { removeAllListeners: mock(() => {}) },
    end: mock(() => {}),
    ...(lidFor
      ? { signalRepository: { lidMapping: { getLIDForPN: mock(async (pn: string) => lidFor[pn] ?? null) } } }
      : {}),
  };
}

function createPlugin(lidFor?: Record<string, string>) {
  const plugin = new WhatsAppPlugin();
  const internals = plugin as unknown as HoldInternals;
  const calls: Call[] = [];
  const sock = createMockSocket(calls, lidFor);
  internals.sockets = new Map([[INSTANCE_ID, sock]]);
  internals.eventBus = { publish: mock(async () => {}) };
  internals.logger = { info: mock(), debug: mock(), warn: mock(), error: mock() };
  return { plugin, internals, calls, sock };
}

function presences(calls: Call[], state?: string, jid?: string): Call[] {
  return calls.filter((c) => c[0] === 'presence' && (!state || c[1] === state) && (!jid || c[2] === jid));
}

const immediateText = (to: string): OutgoingMessage => ({
  to,
  content: { type: 'text', text: 'reply' },
  metadata: { outboundPacing: 'immediate' },
});

async function advance(ms: number): Promise<void> {
  jest.advanceTimersByTime(ms);
  // let refresh promises settle
  await Promise.resolve();
}

describe('WhatsAppPlugin.holdPresence', () => {
  const savedEnv = {
    refresh: process.env.WHATSAPP_PRESENCE_REFRESH_MS,
    lifetime: process.env.WHATSAPP_PRESENCE_MAX_LIFETIME_MS,
  };

  beforeEach(() => {
    Reflect.deleteProperty(process.env, 'WHATSAPP_PRESENCE_REFRESH_MS');
    Reflect.deleteProperty(process.env, 'WHATSAPP_PRESENCE_MAX_LIFETIME_MS');
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
    if (savedEnv.refresh === undefined) Reflect.deleteProperty(process.env, 'WHATSAPP_PRESENCE_REFRESH_MS');
    else process.env.WHATSAPP_PRESENCE_REFRESH_MS = savedEnv.refresh;
    if (savedEnv.lifetime === undefined) Reflect.deleteProperty(process.env, 'WHATSAPP_PRESENCE_MAX_LIFETIME_MS');
    else process.env.WHATSAPP_PRESENCE_MAX_LIFETIME_MS = savedEnv.lifetime;
  });

  it("'typing' sends composing at once and re-sends it every 10s by default", async () => {
    const { plugin, calls } = createPlugin();

    const result = await plugin.holdPresence(INSTANCE_ID, PHONE, 'typing');

    expect(result.lifetimeMs).toBe(60_000);
    expect(presences(calls)).toEqual([['presence', 'composing', PHONE_JID]]);

    await advance(9_999);
    expect(presences(calls, 'composing')).toHaveLength(1);
    await advance(1);
    expect(presences(calls, 'composing')).toHaveLength(2);
    await advance(10_000);
    expect(presences(calls, 'composing')).toHaveLength(3);
  });

  it('refreshes at the configured WHATSAPP_PRESENCE_REFRESH_MS cadence', async () => {
    process.env.WHATSAPP_PRESENCE_REFRESH_MS = '4000';
    const { plugin, calls } = createPlugin();

    await plugin.holdPresence(INSTANCE_ID, PHONE, 'typing');
    await advance(12_000);

    expect(presences(calls, 'composing')).toHaveLength(4);
  });

  it("'recording' maps to Baileys 'recording' for the first send and every refresh", async () => {
    const { plugin, calls } = createPlugin();

    await plugin.holdPresence(INSTANCE_ID, PHONE, 'recording');
    await advance(20_000);

    expect(presences(calls)).toEqual([
      ['presence', 'recording', PHONE_JID],
      ['presence', 'recording', PHONE_JID],
      ['presence', 'recording', PHONE_JID],
    ]);
  });

  it("a send to the held chat clears the hold and sends 'paused' right before the frame", async () => {
    const { plugin, calls, internals } = createPlugin();

    await plugin.holdPresence(INSTANCE_ID, PHONE, 'typing');
    const sent = await plugin.sendMessage(INSTANCE_ID, immediateText(PHONE));

    expect(sent.success).toBe(true);
    expect(calls.slice(-2)).toEqual([
      ['presence', 'paused', PHONE_JID],
      ['send', 'message', PHONE_JID],
    ]);
    expect(internals.presenceHolds.size).toBe(0);

    await advance(60_000);
    expect(presences(calls, 'composing')).toHaveLength(1);
  });

  it("a send to a different chat emits no 'paused' and leaves the hold running", async () => {
    const { plugin, calls } = createPlugin();

    await plugin.holdPresence(INSTANCE_ID, PHONE, 'typing');
    await plugin.sendMessage(INSTANCE_ID, immediateText(OTHER_JID));

    expect(presences(calls, 'paused')).toHaveLength(0);
    await advance(10_000);
    expect(presences(calls, 'composing', PHONE_JID)).toHaveLength(2);
  });

  it('a send with no hold active emits no extra presence update', async () => {
    const { plugin, calls } = createPlugin();

    await plugin.sendMessage(INSTANCE_ID, immediateText(PHONE));

    expect(presences(calls)).toHaveLength(0);
  });

  it("an explicit 'paused' hold call stops the hold and sends 'paused'", async () => {
    const { plugin, calls, internals } = createPlugin();

    await plugin.holdPresence(INSTANCE_ID, PHONE, 'typing');
    const stopped = await plugin.holdPresence(INSTANCE_ID, PHONE, 'paused');

    expect(stopped).toEqual({ expiresAt: null, lifetimeMs: 0 });
    expect(calls.at(-1)).toEqual(['presence', 'paused', PHONE_JID]);
    expect(internals.presenceHolds.size).toBe(0);

    await advance(30_000);
    expect(presences(calls, 'composing')).toHaveLength(1);
  });

  it('stops at the default max lifetime (60s) and refreshes no more', async () => {
    const { plugin, calls, internals } = createPlugin();

    await plugin.holdPresence(INSTANCE_ID, PHONE, 'typing');
    await advance(59_999);
    expect(internals.presenceHolds.size).toBe(1);
    await advance(1);
    expect(internals.presenceHolds.size).toBe(0);

    const atExpiry = presences(calls, 'composing').length;
    await advance(60_000);
    expect(presences(calls, 'composing')).toHaveLength(atExpiry);
  });

  it('honours an explicit lifetime and caps any lifetime at 300000', async () => {
    const { plugin, internals } = createPlugin();

    const short = await plugin.holdPresence(INSTANCE_ID, PHONE, 'typing', 20_000);
    expect(short.lifetimeMs).toBe(20_000);
    await advance(20_000);
    expect(internals.presenceHolds.size).toBe(0);

    const capped = await plugin.holdPresence(INSTANCE_ID, PHONE, 'typing', 900_000);
    expect(capped.lifetimeMs).toBe(300_000);
    await advance(299_999);
    expect(internals.presenceHolds.size).toBe(1);
    await advance(1);
    expect(internals.presenceHolds.size).toBe(0);
  });

  it('duration 0 means the default max lifetime', async () => {
    const { plugin } = createPlugin();

    const held = await plugin.holdPresence(INSTANCE_ID, PHONE, 'typing', 0);

    expect(held.lifetimeMs).toBe(60_000);
    expect(held.expiresAt).toBe(Date.now() + 60_000);
  });

  it('instance disconnect clears every hold of that instance', async () => {
    const { plugin, calls, internals } = createPlugin();

    await plugin.holdPresence(INSTANCE_ID, PHONE, 'typing');
    await plugin.holdPresence(INSTANCE_ID, OTHER_JID, 'recording');
    expect(internals.presenceHolds.size).toBe(2);

    await plugin.disconnect(INSTANCE_ID);

    expect(internals.presenceHolds.size).toBe(0);
    const before = presences(calls).length;
    await advance(30_000);
    expect(presences(calls)).toHaveLength(before);
  });

  it('a new hold on the same chat replaces the previous one', async () => {
    const { plugin, calls, internals } = createPlugin();

    await plugin.holdPresence(INSTANCE_ID, PHONE, 'typing');
    await plugin.holdPresence(INSTANCE_ID, PHONE, 'recording');
    expect(internals.presenceHolds.size).toBe(1);

    await advance(10_000);
    expect(presences(calls, 'composing')).toHaveLength(1);
    expect(presences(calls, 'recording')).toHaveLength(2);
  });

  it('each refresh uses the socket current at fire time', async () => {
    const { plugin, calls, internals } = createPlugin();
    await plugin.holdPresence(INSTANCE_ID, PHONE, 'typing');

    const laterCalls: Call[] = [];
    internals.sockets.set(INSTANCE_ID, createMockSocket(laterCalls));
    await advance(10_000);

    expect(presences(calls, 'composing')).toHaveLength(1);
    expect(presences(laterCalls, 'composing')).toEqual([['presence', 'composing', PHONE_JID]]);
  });

  it('targets the LID that sendMessage resolves for the chat', async () => {
    const LID = '178035101794451@lid';
    const { plugin, calls } = createPlugin({ [PHONE_JID]: LID });

    await plugin.holdPresence(INSTANCE_ID, PHONE, 'typing');
    await plugin.sendMessage(INSTANCE_ID, immediateText(PHONE));

    expect(calls[0]).toEqual(['presence', 'composing', LID]);
    expect(calls.slice(-2)).toEqual([
      ['presence', 'paused', LID],
      ['send', 'message', LID],
    ]);
  });

  describe('legacy sendTyping pending pause (11b)', () => {
    it('still sends composing then paused after the duration when no hold starts', async () => {
      const { plugin, calls } = createPlugin();

      await plugin.sendTyping(INSTANCE_ID, PHONE, 5_000);
      expect(presences(calls)).toEqual([['presence', 'composing', PHONE_JID]]);

      await advance(5_000);
      expect(presences(calls)).toEqual([
        ['presence', 'composing', PHONE_JID],
        ['presence', 'paused', PHONE_JID],
      ]);
    });

    it('is cancelled when a hold starts on that chat, so the hold survives it', async () => {
      const { plugin, calls, internals } = createPlugin();

      await plugin.sendTyping(INSTANCE_ID, PHONE, 5_000);
      await plugin.holdPresence(INSTANCE_ID, PHONE, 'typing');

      await advance(5_000);
      expect(presences(calls, 'paused')).toHaveLength(0);
      expect(internals.presenceHolds.size).toBe(1);
    });

    it('is not cancelled by a hold on a different chat', async () => {
      const { plugin, calls } = createPlugin();

      await plugin.sendTyping(INSTANCE_ID, PHONE, 5_000);
      await plugin.holdPresence(INSTANCE_ID, OTHER_JID, 'typing');

      await advance(5_000);
      expect(presences(calls, 'paused')).toEqual([['presence', 'paused', PHONE_JID]]);
    });
  });
});
