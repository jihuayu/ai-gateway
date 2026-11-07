/**
 * In-process FIFO queue keyed by a string (for example a project id).
 *
 * Tasks with the same key run strictly one at a time in submission order.
 * Tasks with different keys run concurrently. The queue is a *scheduling*
 * aid only: it never replaces durable cross-process guarantees.
 *
 * A key is kept in the map only while it has queued or running work, so idle
 * keys disappear and the structure cannot grow without bound. A rejected task
 * rejects its own caller and still releases the next waiter for that key.
 */
interface QueueEntry {
  task: () => Promise<unknown>;
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
}

export interface KeyedQueueActivity {
  /** Keys that currently have at least one queued or running task. */
  keys: number;
  /** Running tasks (one per active key). */
  active: number;
  /** Tasks waiting behind a running task. */
  waiting: number;
  /** active + waiting. */
  pending: number;
}

export class KeyedQueue {
  readonly #queues = new Map<string, QueueEntry[]>();
  #active = 0;
  #idleWaiters: Array<() => void> = [];

  run<T>(key: string, task: () => Promise<T>): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const entry: QueueEntry = {
        task: task as () => Promise<unknown>,
        resolve: resolve as (value: unknown) => void,
        reject,
      };
      const existing = this.#queues.get(key);
      if (existing) {
        existing.push(entry);
        return;
      }
      this.#queues.set(key, [entry]);
      this.#pump(key);
    });
  }

  /** Number of keys with queued work (keys are dropped once they go idle). */
  get activeKeys(): number {
    return this.#queues.size;
  }

  /** Tasks currently waiting behind a running task for their key. */
  get waiting(): number {
    let waiting = 0;
    for (const queue of this.#queues.values()) waiting += queue.length;
    return Math.max(0, waiting - this.#active);
  }

  activity(): KeyedQueueActivity {
    const waiting = this.waiting;
    return { keys: this.#queues.size, active: this.#active, waiting, pending: waiting + this.#active };
  }

  /** Queued or running tasks for a single key. */
  pendingFor(key: string): number {
    return this.#queues.get(key)?.length ?? 0;
  }

  /** Resolves once every queued task has settled (success or failure). */
  async drain(): Promise<void> {
    while (this.#active > 0 || this.#queues.size > 0) {
      await new Promise<void>((resolve) => { this.#idleWaiters.push(resolve); });
    }
  }

  #pump(key: string): void {
    const queue = this.#queues.get(key);
    if (!queue || queue.length === 0) {
      this.#queues.delete(key);
      this.#notifyIdle();
      return;
    }
    this.#active++;
    void this.#execute(key, queue[0]);
  }

  async #execute(key: string, entry: QueueEntry): Promise<void> {
    try {
      const value = await entry.task();
      entry.resolve(value);
    } catch (err) {
      entry.reject(err);
    } finally {
      this.#active--;
      const queue = this.#queues.get(key);
      if (queue && queue[0] === entry) queue.shift();
      if (queue && queue.length === 0) this.#queues.delete(key);
      this.#pump(key);
      this.#notifyIdle();
    }
  }

  #notifyIdle(): void {
    if (this.#active > 0 || this.#queues.size > 0) return;
    const waiters = this.#idleWaiters;
    if (waiters.length === 0) return;
    this.#idleWaiters = [];
    for (const resolve of waiters) resolve();
  }
}
