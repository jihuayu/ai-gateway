import type { AttemptRecord } from './gateway.js';

/** Full, sanitized request snapshot. Newer revisions subsume older journal events. */
export interface HotRequest {
  version: 1;
  epoch: string;
  revision: number;
  requestId: string;
  projectId: string;
  keyId: string;
  period: string;
  reserveMicros: number;
  routeVersion: unknown;
  priceSnapshot: unknown;
  state: 'reserved' | 'dispatched' | 'settled' | 'unknown' | 'cancelled';
  status: string;
  actualMicros: number | null;
  chargedMicros: number | null;
  inputTokens: number | null;
  outputTokens: number | null;
  aliasName: string | null;
  errorCode: string | null;
  attempts: AttemptRecord[];
  createdAt: string;
  updatedAt: string;
  leaseUntil: number;
  owner: string;
  remainingEvents: number;
}

export function hotKeys(namespace: string) {
  if (!/^[A-Za-z0-9_-]{1,80}$/.test(namespace)) throw Error('Invalid data-plane namespace');
  // V1 is deliberately one Redis hash slot: budget + journal append are atomic.
  const prefix = `gateway:{${namespace}}:hot:v1:`;
  return {prefix, runtime: prefix+'runtime', config:prefix+'config', journal: prefix+'journal', slots: prefix+'slots',
    projects:prefix+'projects', active:prefix+'active',
    request:(id:string)=>prefix+'request:'+id, project:(id:string)=>prefix+'project:'+id,
    snapshot:(revision:string)=>prefix+'snapshot:'+revision};
}
export type HotKeys = ReturnType<typeof hotKeys>;
