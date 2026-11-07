import test from 'node:test';
import assert from 'node:assert/strict';
import { KeyedQueue } from '../src/keyed-queue.js';

type Log = string[];

function defer<T = void>() {
  let resolve!: (value: T | Promise<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

const tick = () => new Promise<void>((resolve) => setImmediate(resolve));
async function until(predicate: () => boolean, label: string): Promise<void> {
  for (let i = 0; i < 1000; i++) {
    if (predicate()) return;
    await tick();
  }
  throw new Error(`timed out waiting for ${label}`);
}

test('same key runs tasks FIFO without overlap', async () => {
  const q = new KeyedQueue();
  const log: Log = [];
  let running = 0;
  let maxRunning = 0;
  const gate = defer();

  const make = (name: string, wait?: Promise<void>) => q.run('k', async () => {
    running += 1;
    maxRunning = Math.max(maxRunning, running);
    log.push(`${name}:start`);
    if (wait) await wait;
    log.push(`${name}:end`);
    running -= 1;
    return name;
  });

  const first = make('a', gate.promise);
  const second = make('b');
  const third = make('c');
  await until(() => log.includes('a:start'), 'first task start');
  await tick();
  assert.deepEqual(log, ['a:start'], 'later same-key tasks must wait for the first');
  assert.equal(q.waiting, 2);

  gate.resolve();
  assert.deepEqual(await Promise.all([first, second, third]), ['a', 'b', 'c']);
  assert.deepEqual(log, ['a:start', 'a:end', 'b:start', 'b:end', 'c:start', 'c:end']);
  assert.equal(maxRunning, 1);
  assert.equal(q.activeKeys, 0);
});

test('different keys run concurrently', async () => {
  const q = new KeyedQueue();
  const gate = defer();
  const log: Log = [];
  let running = 0;
  let maxRunning = 0;

  const task = (key: string) => q.run(key, async () => {
    running += 1;
    maxRunning = Math.max(maxRunning, running);
    log.push(`${key}:start`);
    await gate.promise;
    log.push(`${key}:end`);
    running -= 1;
    return key;
  });

  const a = task('project-a');
  const b = task('project-b');
  await until(() => log.length === 2, 'both keys running');
  assert.deepEqual([...log].sort(), ['project-a:start', 'project-b:start']);
  assert.equal(maxRunning, 2, 'distinct keys must not serialize each other');
  assert.equal(q.activity().active, 2);

  gate.resolve();
  assert.deepEqual(await Promise.all([a, b]), ['project-a', 'project-b']);
  assert.equal(maxRunning, 2);
  assert.equal(q.activeKeys, 0);
});

test('a rejected task rejects only its caller and releases the next waiter', async () => {
  const q = new KeyedQueue();
  const log: Log = [];
  const order: string[] = [];
  const unhandled: unknown[] = [];
  const onUnhandled = (reason: unknown) => { unhandled.push(reason); };
  process.on('unhandledRejection', onUnhandled);
  try {
    const boom = new Error('boom');
    const failing = q.run('k', async () => {
      log.push('failing:start');
      await tick();
      throw boom;
    });
    const follower = q.run('k', async () => {
      log.push('follower:start');
      await tick();
      order.push('follower');
      return 'ok';
    });
    await assert.rejects(failing, (err) => err === boom);
    assert.equal(await follower, 'ok');
    assert.deepEqual(order, ['follower'], 'next waiter must run after a rejection');
    assert.deepEqual(log, ['failing:start', 'follower:start']);
    await until(() => q.activeKeys === 0 && q.activity().pending === 0, 'queue idle');
    await tick();
    assert.deepEqual(unhandled, [], 'a handled rejection must not surface as unhandledRejection');
  } finally {
    process.off('unhandledRejection', onUnhandled);
  }
});

test('idle keys are dropped after success and after failure', async () => {
  const q = new KeyedQueue();
  const failure = new Error('nope');
  assert.equal(await q.run('success-key', async () => 1), 1);
  await assert.rejects(q.run('failure-key', async () => { throw failure; }), (err) => err === failure);
  await until(() => q.activeKeys === 0, 'keys released');
  assert.equal(q.pendingFor('success-key'), 0);
  assert.equal(q.pendingFor('failure-key'), 0);
  assert.deepEqual(q.activity(), { keys: 0, active: 0, waiting: 0, pending: 0 });

  // A key reused after idling starts fresh and still runs.
  assert.equal(await q.run('success-key', async () => 2), 2);
  await until(() => q.activeKeys === 0, 'keys released again');
});

test('drain waits for running and queued tasks, including failures', async () => {
  const q = new KeyedQueue();
  const gate = defer();
  const finished: string[] = [];
  let drained = false;

  const first = q.run('k', async () => { await gate.promise; finished.push('first'); });
  const second = q.run('k', async () => { finished.push('second'); });
  q.run('other', async () => { throw new Error('ignored by drain but still settles'); }).catch(() => undefined);

  const drain = q.drain().then(() => { drained = true; });
  await tick();
  assert.equal(drained, false, 'drain must not resolve while work is running');
  assert.equal(finished.length, 0);

  gate.resolve();
  await Promise.all([first, second]);
  await drain;
  assert.deepEqual(finished, ['first', 'second']);
  assert.equal(q.activeKeys, 0);
  assert.equal(q.activity().pending, 0);
});

test('drain on an empty queue resolves immediately and later work still runs', async () => {
  const q = new KeyedQueue();
  await q.drain();
  assert.equal(await q.run('k', async () => 'later'), 'later');
  await q.drain();
  assert.equal(q.activeKeys, 0);
});
