import { beforeEach, describe, expect, it, vi } from 'vitest';
import { from, take, toArray, lastValueFrom } from 'rxjs';

// --- Fake Tauri IPC -------------------------------------------------------------------------
interface FakeSub {
  id: number;
  source: string;
  args: unknown;
  channel: { onmessage: (m: unknown) => void };
  closed: boolean;
}
const backend = {
  subs: [] as FakeSub[],
  nextId: 1,
  /** When set, `subscribe` waits for this promise before resolving. */
  gate: null as Promise<void> | null,
  failWith: null as unknown,
  calls: [] as Array<[string, any]>,
};

vi.mock('@tauri-apps/api/core', () => {
  class Channel<T> {
    onmessage: (message: T) => void = () => {};
  }
  const invoke = vi.fn(async (cmd: string, args: any) => {
    backend.calls.push([cmd, args]);
    switch (cmd) {
      case 'plugin:reactive|subscribe': {
        if (backend.failWith) throw backend.failWith;
        const sub: FakeSub = { id: backend.nextId++, source: args.source, args: args.args, channel: args.onEvent, closed: false };
        backend.subs.push(sub);
        if (backend.gate) await backend.gate;
        return sub.id;
      }
      case 'plugin:reactive|unsubscribe': {
        const sub = backend.subs.find((s) => s.id === args.id);
        if (sub) sub.closed = true;
        return !!sub;
      }
      case 'plugin:reactive|get':
        return 42;
      case 'plugin:reactive|set':
        if (args.value === 'bad') throw { code: 'InvalidValue', message: 'nope' };
        return null;
    }
    throw new Error(`unexpected command ${cmd}`);
  });
  return { Channel, invoke };
});

import { createReactive, ReactiveError, type State, type Stream, type WritableState } from '../guest-js/index';

type Sources = {
  counter: WritableState<number>;
  status: State<string>;
  ticks: Stream<number, { intervalMs: number; label?: string }>;
  events: Stream<string>;
};

const flush = () => new Promise((r) => setTimeout(r, 0));
const live = () => backend.subs.filter((s) => !s.closed);
const emit = (sub: FakeSub, value: unknown) => sub.channel.onmessage({ type: 'next', value });

beforeEach(() => {
  backend.subs = [];
  backend.nextId = 1;
  backend.gate = null;
  backend.failWith = null;
  backend.calls = [];
});

describe('sharing and lifecycle', () => {
  it('shares one backend subscription and replays state to late subscribers', async () => {
    const rx = createReactive<Sources>();
    const a: number[] = [];
    const b: number[] = [];

    rx.state('counter').subscribe((v) => a.push(v));
    expect(live()).toHaveLength(1);
    emit(live()[0], 1);

    rx.state('counter').subscribe((v) => b.push(v)); // same source instance, no new backend sub
    expect(live()).toHaveLength(1);
    emit(live()[0], 2);

    expect(a).toEqual([1, 2]);
    expect(b).toEqual([1, 2]); // 1 was replayed
    expect(rx.state('counter').value).toBe(2);
  });

  it('streams do not replay to late subscribers', () => {
    const rx = createReactive<Sources>();
    const late: string[] = [];
    rx.stream('events').subscribe(() => {});
    emit(live()[0], 'early');
    rx.stream('events').subscribe((v) => late.push(v));
    emit(live()[0], 'later');
    expect(late).toEqual(['later']);
  });

  it('unsubscribes from the backend after the last subscriber leaves', async () => {
    const rx = createReactive<Sources>();
    const u1 = rx.state('status').subscribe(() => {});
    const u2 = rx.state('status').subscribe(() => {});
    u1();
    await flush();
    expect(live()).toHaveLength(1);
    u2.unsubscribe();
    await flush();
    expect(live()).toHaveLength(0);
  });

  it('absorbs synchronous unsubscribe/resubscribe churn (React StrictMode)', async () => {
    const rx = createReactive<Sources>();
    const u = rx.state('status').subscribe(() => {});
    u();
    rx.state('status').subscribe(() => {});
    await flush();
    expect(backend.subs).toHaveLength(1);
    expect(live()).toHaveLength(1);
  });

  it('honours keepAliveMs', async () => {
    vi.useFakeTimers();
    try {
      const rx = createReactive<Sources>({ keepAliveMs: 1000 });
      rx.state('status').subscribe(() => {})();
      await vi.advanceTimersByTimeAsync(500);
      rx.state('status').subscribe(() => {});
      await vi.advanceTimersByTimeAsync(2000);
      expect(backend.subs).toHaveLength(1);
      expect(live()).toHaveLength(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it('ignores events from a previous connection after reconnecting', async () => {
    const rx = createReactive<Sources>();
    const seen: number[] = [];
    rx.state('counter').subscribe(() => {})();
    await flush();
    const old = backend.subs[0];

    rx.state('counter').subscribe((v) => seen.push(v));
    expect(backend.subs).toHaveLength(2);
    emit(old, 99); // stale
    emit(backend.subs[1], 1);
    expect(seen).toEqual([1]);
  });

  it('unsubscribes correctly even if the subscribe call has not resolved yet', async () => {
    let open!: () => void;
    backend.gate = new Promise((r) => (open = r));
    const rx = createReactive<Sources>();
    rx.state('status').subscribe(() => {})();
    await flush();
    expect(backend.calls.some(([c]) => c === 'plugin:reactive|unsubscribe')).toBe(false);
    open();
    await flush();
    expect(backend.calls).toContainEqual(['plugin:reactive|unsubscribe', { id: 1 }]);
  });
});

describe('streams with args', () => {
  it('passes args and shares by structural equality', () => {
    const rx = createReactive<Sources>();
    const s1 = rx.stream('ticks', { intervalMs: 10, label: 'x' });
    const s2 = rx.stream('ticks', { label: 'x', intervalMs: 10 });
    const s3 = rx.stream('ticks', { intervalMs: 20 });
    expect(s1).toBe(s2);
    expect(s1).not.toBe(s3);

    s1.subscribe(() => {});
    s3.subscribe(() => {});
    expect(live().map((s) => s.args)).toEqual([{ intervalMs: 10, label: 'x' }, { intervalMs: 20 }]);
  });

  it('sends null args for streams without args', () => {
    const rx = createReactive<Sources>();
    rx.stream('events').subscribe(() => {});
    expect(live()[0].args).toBeNull();
  });
});

describe('termination', () => {
  it('delivers backend errors as ReactiveError and allows a fresh resubscribe', () => {
    const rx = createReactive<Sources>();
    const errors: ReactiveError[] = [];
    const ticks = rx.stream('ticks', { intervalMs: 5 });
    ticks.subscribe({ error: (e) => errors.push(e) });
    live()[0].channel.onmessage({ type: 'error', message: 'sensor unplugged' });

    expect(errors).toHaveLength(1);
    expect(errors[0]).toBeInstanceOf(ReactiveError);
    expect(errors[0].code).toBe('StreamError');
    expect(ticks.error?.message).toBe('sensor unplugged');
    expect(ticks.connected).toBe(false);

    rx.stream('ticks', { intervalMs: 5 }).subscribe(() => {});
    expect(backend.subs).toHaveLength(2);
  });

  it('turns subscribe failures into typed errors', async () => {
    backend.failWith = { code: 'UnknownSource', message: 'unknown reactive source `status`' };
    const rx = createReactive<Sources>();
    const err = await new Promise<ReactiveError>((resolve) => rx.state('status').subscribe({ error: resolve }));
    expect(err.code).toBe('UnknownSource');
  });

  it('completes observers', () => {
    const rx = createReactive<Sources>();
    const complete = vi.fn();
    rx.stream('events').subscribe({ complete });
    live()[0].channel.onmessage({ type: 'complete' });
    expect(complete).toHaveBeenCalledOnce();
  });
});

describe('get / set', () => {
  it('invokes get and set', async () => {
    const rx = createReactive<Sources>();
    expect(await rx.state('status').get()).toBe(42);
    await rx.state('counter').set(7);
    expect(backend.calls).toContainEqual(['plugin:reactive|set', { source: 'counter', value: 7 }]);
    await expect(rx.state('counter').set('bad' as any)).rejects.toMatchObject({ code: 'InvalidValue' });
  });

  it('only exposes set on writable states (type-level)', () => {
    const rx = createReactive<Sources>();
    // @ts-expect-error status is read-only
    rx.state('status').set;
    // @ts-expect-error ticks requires args
    rx.stream('ticks');
    // @ts-expect-error counter is a state, not a stream
    rx.stream('counter');
  });
});

describe('interop', () => {
  it('works with RxJS from()', async () => {
    const rx = createReactive<Sources>();
    const result = lastValueFrom(from(rx.state('counter')).pipe(take(3), toArray()));
    for (const v of [1, 2, 3]) emit(live()[0], v);
    expect(await result).toEqual([1, 2, 3]);
    await flush();
    expect(live()).toHaveLength(0); // take(3) unsubscribed -> backend torn down
  });

  it('satisfies the Svelte store contract', async () => {
    const rx = createReactive<Sources>();
    const store = rx.state('status');
    const values: string[] = [];
    const unsub = store.subscribe((v) => values.push(v));
    emit(live()[0], 'online');
    expect(values).toEqual(['online']);
    expect(typeof unsub).toBe('function');
    unsub();
    await flush();
    expect(live()).toHaveLength(0);
  });
});
