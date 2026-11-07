import { z } from 'zod';

const envSchema = z.object({
  DATABASE_URL: z.url().refine(v => /^postgres(ql)?:/.test(v), 'PostgreSQL URL required'),
  REDIS_URL: z.url().refine(v => /^rediss?:/.test(v), 'Redis URL required'),
  ENCRYPTION_KEY: z.string().regex(/^[a-fA-F0-9]{64}$/, 'ENCRYPTION_KEY must be 32 bytes encoded as hex'),
  HOST: z.string().default('127.0.0.1'),
  PORT: z.coerce.number().int().min(1).max(65535).default(3000),
  ALLOW_PRIVATE_UPSTREAM: z.enum(['true', 'false']).default('false'),
  KAFKA_BROKERS: z.string().default(''),
});
export function loadConfig(env = process.env) {
  const parsed = envSchema.safeParse(env);
  if (!parsed.success) throw new Error('Invalid configuration: ' + parsed.error.issues.map(e => `${e.path.join('.')}: ${e.message}`).join('; '));
  return parsed.data;
}
