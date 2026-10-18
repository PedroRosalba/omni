import { describe, expect, test } from 'bun:test';
import {
  DEFAULT_WHATSAPP_OUTBOUND_TIMING,
  DEFAULT_WHATSAPP_PRESENCE_HOLD,
  DEFAULT_WHATSAPP_RATE_LIMIT,
  getWhatsAppOutboundTimingConfig,
  getWhatsAppPresenceHoldConfig,
  getWhatsAppRateLimitConfig,
} from '../env';

describe('WhatsApp env config', () => {
  test('uses production-safe outbound timing defaults', () => {
    expect(getWhatsAppOutboundTimingConfig({})).toEqual(DEFAULT_WHATSAPP_OUTBOUND_TIMING);
  });

  test('parses outbound timing overrides and disable flags', () => {
    const config = getWhatsAppOutboundTimingConfig({
      WHATSAPP_HUMAN_DELAY_ENABLED: 'false',
      WHATSAPP_HUMAN_DELAY_MIN_MS: '0',
      WHATSAPP_HUMAN_DELAY_MAX_MS: '0',
      WHATSAPP_TYPING_SIMULATION_ENABLED: 'off',
      WHATSAPP_TYPING_DELAY_BASE_MS: '100',
      WHATSAPP_TYPING_DELAY_PER_CHAR_MS: '2',
      WHATSAPP_TYPING_DELAY_MAX_MS: '500',
      WHATSAPP_TYPING_DEFAULT_MS: '750',
    });

    expect(config).toEqual({
      humanDelayEnabled: false,
      humanDelayMinMs: 0,
      humanDelayMaxMs: 0,
      typingSimulationEnabled: false,
      typingDelayBaseMs: 100,
      typingDelayPerCharMs: 2,
      typingDelayMaxMs: 500,
      typingDefaultMs: 750,
    });
  });

  test('normalizes invalid values and inverted ranges', () => {
    const config = getWhatsAppOutboundTimingConfig({
      WHATSAPP_HUMAN_DELAY_ENABLED: 'maybe',
      WHATSAPP_HUMAN_DELAY_MIN_MS: '3000',
      WHATSAPP_HUMAN_DELAY_MAX_MS: '1000',
      WHATSAPP_TYPING_DELAY_BASE_MS: '-1',
      WHATSAPP_TYPING_DELAY_PER_CHAR_MS: 'nope',
    });

    expect(config.humanDelayEnabled).toBe(true);
    expect(config.humanDelayMinMs).toBe(3000);
    expect(config.humanDelayMaxMs).toBe(3000);
    expect(config.typingDelayBaseMs).toBe(DEFAULT_WHATSAPP_OUTBOUND_TIMING.typingDelayBaseMs);
    expect(config.typingDelayPerCharMs).toBe(DEFAULT_WHATSAPP_OUTBOUND_TIMING.typingDelayPerCharMs);
  });

  test('uses rate-limit defaults', () => {
    expect(getWhatsAppRateLimitConfig({})).toEqual(DEFAULT_WHATSAPP_RATE_LIMIT);
  });

  test('parses rate-limit overrides and normalizes max backoff', () => {
    expect(
      getWhatsAppRateLimitConfig({
        WHATSAPP_RATE_LIMIT_INITIAL_BACKOFF_MS: '5000',
        WHATSAPP_RATE_LIMIT_MAX_BACKOFF_MS: '1000',
        WHATSAPP_RATE_LIMIT_JITTER_FACTOR: '0',
      }),
    ).toEqual({
      initialBackoffMs: 5000,
      maxBackoffMs: 5000,
      jitterFactor: 0,
    });
  });

  test('outbound timing defaults are unchanged by the presence-hold settings', () => {
    expect(DEFAULT_WHATSAPP_OUTBOUND_TIMING).toEqual({
      humanDelayEnabled: true,
      humanDelayMinMs: 1500,
      humanDelayMaxMs: 3500,
      typingSimulationEnabled: true,
      typingDelayBaseMs: 800,
      typingDelayPerCharMs: 30,
      typingDelayMaxMs: 4000,
      typingDefaultMs: 3000,
    });
  });

  test('uses presence-hold defaults (10s refresh, 60s max lifetime)', () => {
    expect(DEFAULT_WHATSAPP_PRESENCE_HOLD).toEqual({ refreshMs: 10_000, maxLifetimeMs: 60_000 });
    expect(getWhatsAppPresenceHoldConfig({})).toEqual(DEFAULT_WHATSAPP_PRESENCE_HOLD);
  });

  test('parses presence-hold overrides', () => {
    expect(
      getWhatsAppPresenceHoldConfig({
        WHATSAPP_PRESENCE_REFRESH_MS: '4000',
        WHATSAPP_PRESENCE_MAX_LIFETIME_MS: '120000',
      }),
    ).toEqual({ refreshMs: 4000, maxLifetimeMs: 120_000 });
  });

  test('presence-hold: zero or invalid values fall back to defaults, lifetime capped at 300000', () => {
    expect(
      getWhatsAppPresenceHoldConfig({
        WHATSAPP_PRESENCE_REFRESH_MS: '0',
        WHATSAPP_PRESENCE_MAX_LIFETIME_MS: 'nope',
      }),
    ).toEqual(DEFAULT_WHATSAPP_PRESENCE_HOLD);
    expect(getWhatsAppPresenceHoldConfig({ WHATSAPP_PRESENCE_MAX_LIFETIME_MS: '900000' }).maxLifetimeMs).toBe(300_000);
  });
});
