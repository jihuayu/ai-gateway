import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto';
import { lookup } from 'node:dns/promises';
import { isIP } from 'node:net';

const KEY_RE = /^[a-fA-F0-9]{64}$/;
const IPV4_BLOCKED: Array<[string, number]> = [
  ['0.0.0.0', 8], ['10.0.0.0', 8], ['100.64.0.0', 10], ['127.0.0.0', 8],
  ['169.254.0.0', 16], ['172.16.0.0', 12], ['192.0.0.0', 24], ['192.0.2.0', 24],
  ['192.168.0.0', 16], ['198.18.0.0', 15], ['198.51.100.0', 24],
  ['203.0.113.0', 24], ['224.0.0.0', 4], ['240.0.0.0', 4],
];

export class ProviderUrlError extends Error {
  readonly statusCode = 400;
  readonly code = 'invalid_provider_url';
}

export function hashKey(value: string): string {
  return createHash('sha256').update(value, 'utf8').digest('hex');
}

function keyBuffer(key: string): Buffer {
  if (!KEY_RE.test(key)) throw new Error('encryption key must be 64 hex characters (32 bytes)');
  return Buffer.from(key, 'hex');
}

/** AES-256-GCM; output format: v1.<iv b64url>.<tag b64url>.<ciphertext b64url>. */
export function encrypt(value: string, key: string): string {
  const iv = randomBytes(12);
  const cipher = createCipheriv('aes-256-gcm', keyBuffer(key), iv);
  const ciphertext = Buffer.concat([cipher.update(value, 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  return ['v1', iv.toString('base64url'), tag.toString('base64url'), ciphertext.toString('base64url')].join('.');
}

export function decrypt(value: string, key: string): string {
  const parts = value.split('.');
  if (parts.length !== 4 || parts[0] !== 'v1') throw new Error('invalid encrypted value');
  const iv = Buffer.from(parts[1], 'base64url');
  const tag = Buffer.from(parts[2], 'base64url');
  const ciphertext = Buffer.from(parts[3], 'base64url');
  if (iv.length !== 12 || tag.length !== 16) throw new Error('invalid encrypted value');
  const decipher = createDecipheriv('aes-256-gcm', keyBuffer(key), iv);
  decipher.setAuthTag(tag);
  return Buffer.concat([decipher.update(ciphertext), decipher.final()]).toString('utf8');
}

function ipv4ToInt(ip: string): number {
  return ip.split('.').reduce((acc, p) => (acc << 8) + Number(p), 0) >>> 0;
}

function ipv4Blocked(ip: string): boolean {
  const n = ipv4ToInt(ip);
  return IPV4_BLOCKED.some(([base, bits]) => {
    const mask = bits === 0 ? 0 : (0xffffffff << (32 - bits)) >>> 0;
    return (n & mask) >>> 0 === (ipv4ToInt(base) & mask) >>> 0;
  });
}

function ipv6Blocked(ip: string): boolean {
  const lower = ip.toLowerCase();
  const mapped = /^::ffff:(\d+\.\d+\.\d+\.\d+)$/.exec(lower)?.[1]
    ?? (() => {
      const m = /^::ffff:([0-9a-f]{1,4}):([0-9a-f]{1,4})$/.exec(lower);
      if (!m) return null;
      const hi = parseInt(m[1], 16);
      const lo = parseInt(m[2], 16);
      return `${hi >> 8}.${hi & 255}.${lo >> 8}.${lo & 255}`;
    })();
  if (mapped) return ipv4Blocked(mapped);
  if (lower === '::' || lower === '::1') return true;
  const first = parseInt(lower.split(':')[0] || '0', 16);
  if ((first & 0xfe00) === 0xfc00) return true; // fc00::/7 ULA
  if ((first & 0xffc0) === 0xfe80) return true; // link-local
  if ((first & 0xff00) === 0xff00) return true; // multicast
  if (first === 0x2001 && lower.split(':')[1] === 'db8') return true;
  return (first & 0xe000) !== 0x2000; // only global unicast 2000::/3
}

function normalizeHost(hostname: string): string {
  const host = hostname.toLowerCase().replace(/^\[|\]$/g, '').replace(/\.$/, '');
  if (!host || host === 'localhost' || host.endsWith('.localhost')) {
    throw new ProviderUrlError('provider URL host is not allowed');
  }
  return host;
}

export function isBlockedAddress(address: string): boolean {
  const family = isIP(address);
  if (family === 4) return ipv4Blocked(address);
  if (family === 6) return ipv6Blocked(address);
  return true;
}

/**
 * Validate an upstream provider URL. Rejects credentials, non-HTTPS endpoints
 * (unless private upstreams are explicitly enabled at deployment level),
 * and loopback/private/metadata addresses. Every DNS answer is inspected.
 */
export async function validateProviderUrl(url: string, allowPrivate = false): Promise<void> {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    throw new ProviderUrlError('provider URL is invalid');
  }
  if (parsed.username || parsed.password) throw new ProviderUrlError('provider URL must not contain credentials');
  if (parsed.hash) throw new ProviderUrlError('provider URL must not contain a fragment');
  if (parsed.protocol !== 'https:' && !(allowPrivate && parsed.protocol === 'http:')) {
    throw new ProviderUrlError('provider URL must use HTTPS');
  }
  if (parsed.port && !/^\d+$/.test(parsed.port)) throw new ProviderUrlError('provider URL port is invalid');
  const host = normalizeHost(parsed.hostname);
  let addresses: string[];
  if (isIP(host)) {
    addresses = [host];
  } else {
    try {
      const answers = await lookup(host, { all: true, verbatim: true });
      addresses = answers.map(a => a.address);
    } catch {
      throw new ProviderUrlError('provider host could not be resolved');
    }
    if (addresses.length === 0) throw new ProviderUrlError('provider host could not be resolved');
  }
  if (!allowPrivate && addresses.some(isBlockedAddress)) {
    throw new ProviderUrlError('provider URL resolves to a private or reserved address');
  }
}
