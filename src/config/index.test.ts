import { describe, it, expect } from 'vitest';
import { loadConfig } from './index.js';

// KAFKA_BROKERS is the only variable without a default.
const minimalEnv = { KAFKA_BROKERS: 'localhost:9092' };

describe('loadConfig', () => {
  it('applies defaults for everything except KAFKA_BROKERS', () => {
    const config = loadConfig(minimalEnv);

    expect(config).toMatchObject({
      KAFKA_TOPIC: 'log-events',
      KAFKA_DLQ_TOPIC: 'log-events-dlq',
      KAFKA_GROUP_ID: 'log-pipeline-consumer',
      CLICKHOUSE_URL: 'http://localhost:8123',
      CLICKHOUSE_DB: 'default',
      CLICKHOUSE_USER: 'default',
      CLICKHOUSE_PASSWORD: '',
      BATCH_SIZE: 5000,
      FLUSH_INTERVAL_MS: 1000,
      PRODUCER_RATE: 10000,
      API_PORT: 3000,
      PRODUCER_METRICS_PORT: 9101,
      CONSUMER_METRICS_PORT: 9102,
      NODE_ENV: 'development',
    });
  });

  it('splits KAFKA_BROKERS into a list', () => {
    expect(loadConfig({ KAFKA_BROKERS: 'a:9092,b:9092,c:9092' }).KAFKA_BROKERS)
      .toEqual([ 'a:9092', 'b:9092', 'c:9092' ]);
  });

  it('yields a single-element list for one broker', () => {
    expect(loadConfig(minimalEnv).KAFKA_BROKERS).toEqual([ 'localhost:9092' ]);
  });

  it('coerces numeric vars from the strings the environment always provides', () => {
    const config = loadConfig({ ...minimalEnv, BATCH_SIZE: '250', API_PORT: '8080' });

    expect(config.BATCH_SIZE).toBe(250);
    expect(config.API_PORT).toBe(8080);
  });

  it('throws when KAFKA_BROKERS is missing', () => {
    expect(() => loadConfig({})).toThrow(/Invalid environment configuration/);
  });

  it.each([
    [ 'a non-numeric BATCH_SIZE', { BATCH_SIZE: 'lots' } ],
    [ 'a zero BATCH_SIZE', { BATCH_SIZE: '0' } ],
    [ 'a negative FLUSH_INTERVAL_MS', { FLUSH_INTERVAL_MS: '-1' } ],
    [ 'a fractional API_PORT', { API_PORT: '80.5' } ],
    [ 'a malformed CLICKHOUSE_URL', { CLICKHOUSE_URL: 'not-a-url' } ],
    [ 'an unknown NODE_ENV', { NODE_ENV: 'staging' } ],
  ])('throws on %s', (_label, override) => {
    expect(() => loadConfig({ ...minimalEnv, ...override })).toThrow(/Invalid environment configuration/);
  });

  it('names the offending variable in the error', () => {
    expect(() => loadConfig({ ...minimalEnv, API_PORT: 'nope' })).toThrow(/API_PORT/);
  });

  it('ignores unrelated environment variables', () => {
    expect(() => loadConfig({ ...minimalEnv, HOME: '/root', PATH: '/usr/bin' })).not.toThrow();
  });
});
