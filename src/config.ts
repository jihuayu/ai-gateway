import { z } from 'zod';

const envSchema = z.object({
  DATABASE_URL: z.url().refine(v => /^postgres(ql)?:/.test(v), 'PostgreSQL URL required'),
  REDIS_URL: z.url().refine(v => /^rediss?:/.test(v), 'Redis URL required'),
  ENCRYPTION_KEY: z.string().regex(/^[a-fA-F0-9]{64}$/, 'ENCRYPTION_KEY must be 32 bytes encoded as hex'),
  HOST: z.string().default('127.0.0.1'),
  PORT: z.coerce.number().int().min(1).max(65535).default(3000),
  ALLOW_PRIVATE_UPSTREAM: z.enum(['true', 'false']).default('false'),
  KAFKA_BROKERS: z.string().default(''),
  DATA_PLANE_NAMESPACE:z.string().regex(/^[A-Za-z0-9_-]{1,80}$/).default('gateway'),
  HOT_DURABILITY:z.enum(['aof','memory']).default('aof'),
  HOT_JOURNAL_CAPACITY:z.coerce.number().int().min(7).max(1000000).default(10000),
});
export function loadConfig(env = process.env) {
  const parsed = envSchema.safeParse(env);
  if (!parsed.success) throw new Error('Invalid configuration: ' + parsed.error.issues.map(e => `${e.path.join('.')}: ${e.message}`).join('; '));
  if(env.NODE_ENV==='production'&&parsed.data.HOT_DURABILITY==='memory')throw Error('Memory-only Redis durability is restricted to development/test');
  return parsed.data;
}
