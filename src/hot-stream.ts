import { randomUUID } from 'node:crypto';
import type { Redis } from 'ioredis';

export interface StreamEntry { id: string; data: string }

const BUSYGROUP = 'BUSYGROUP';
const NOGROUP = 'NOGROUP';

/** Acknowledge then delete, but only for IDs this group actually holds pending. */
const ACK_DELETE_LUA = `
local group = ARGV[1]
local deleted = 0
for i = 2, #ARGV do
  local id = ARGV[i]
  if redis.call('XACK', KEYS[1], group, id) == 1 then
    if redis.call('XDEL', KEYS[1], id) == 1 then deleted = deleted + 1 end
  end
end
return deleted
`;

function message(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/**
 * Bounded Redis Streams adapter for the asynchronous PostgreSQL projector.
 * Reads are non-blocking and bounded; ack/delete happens only after the caller
 * commits PG, so an unacked entry is always replayed.
 */
export class HotStream {
  private readonly redis: Redis;
  private readonly key: string;
  private readonly group: string;
  private readonly claimIdleMs: number;
  private readonly consumer = `pg-${randomUUID()}`;
  private initialized = false;
  private cursor = '0-0';

  constructor(redis: Redis, key: string, group = 'pg-hot-v1', claimIdleMs = 30000) {
    this.redis = redis;
    this.key = key;
    this.group = group;
    this.claimIdleMs = claimIdleMs;
  }

  private async ensureGroup(): Promise<void> {
    if (this.initialized) return;
    try {
      await this.redis.call('XGROUP', 'CREATE', this.key, this.group, '0', 'MKSTREAM');
    } catch (err) {
      if (!message(err).includes(BUSYGROUP)) throw err;
    }
    this.initialized = true;
  }

  /** Recreate the group on the next read after a Redis reset; never silent. */
  private reset(err: unknown): never {
    if (message(err).includes(NOGROUP)) this.initialized = false;
    throw err;
  }

  async read(limit = 50): Promise<StreamEntry[]> {
    const bound = Math.max(1, Math.min(500, Math.trunc(limit)));
    await this.ensureGroup();
    const out: StreamEntry[] = [];
    const seen = new Set<string>();
    const drain = (raw: unknown): void => {
      for (const item of Array.isArray(raw) ? raw : []) {
        if (out.length >= bound) return;
        const entry = this.decode(item);
        if (seen.has(entry.id)) continue;
        seen.add(entry.id);
        out.push(entry);
      }
    };
    try {
      // 1. Reclaim messages abandoned by a crashed consumer, resuming from the
      // stored cursor so repeated reads do not starve the tail of the PEL.
      const claimed = await this.redis.call('XAUTOCLAIM', this.key, this.group, this.consumer,
        String(this.claimIdleMs), this.cursor, 'COUNT', String(bound)) as unknown[];
      if (Array.isArray(claimed)) {
        if (typeof claimed[0] === 'string') this.cursor = claimed[0];
        drain(claimed[1]);
      }
      // 2. Our own pending entries: a failed PG commit is retried immediately.
      if (out.length < bound) drain(await this.readGroup('0', bound - out.length));
      // 3. Never-delivered entries.
      if (out.length < bound) drain(await this.readGroup('>', bound - out.length));
    } catch (err) {
      this.reset(err);
    }
    return out;
  }

  private async readGroup(position: string, count: number): Promise<unknown> {
    const raw = await this.redis.call('XREADGROUP', 'GROUP', this.group, this.consumer,
      'COUNT', String(Math.max(1, count)), 'STREAMS', this.key, position) as unknown;
    if (!Array.isArray(raw)) return [];
    const reply = raw as Array<[string, unknown]>;
    return reply[0]?.[1] ?? [];
  }

  private decode(item: unknown): StreamEntry {
    if (!Array.isArray(item) || typeof item[0] !== 'string') {
      throw new Error('hot stream entry is malformed');
    }
    const fields = item[1];
    if (!Array.isArray(fields) || fields.length !== 2 || fields[0] !== 'data' || typeof fields[1] !== 'string') {
      throw new Error('hot stream entry must contain exactly one data field');
    }
    return { id: item[0], data: fields[1] };
  }

  /** Call only after the PG transaction committed; unacked IDs are replayed. */
  async ack(ids: string[]): Promise<void> {
    if (!ids.length) return;
    try {
      await this.redis.call('EVAL', ACK_DELETE_LUA, '1', this.key, this.group, ...ids);
    } catch (err) {
      this.reset(err);
    }
  }
}
