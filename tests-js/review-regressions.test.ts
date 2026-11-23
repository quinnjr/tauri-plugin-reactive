// Regression tests for issues found in code review.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { concat, from, of, throwError } from 'rxjs';

interface FakeSub {
  id: number;
  source: string;
  page: unknown;
  channel: { onmessage: (m: unknown) => void };
  closed: boolean;
}
const backend = { subs: [] as FakeSub[], nextId: 1, closeError: null as unknown, calls: [] as Array<[string, any]> };

vi.mock('@tauri-apps/api/core', () => {
  class Channel {
    onmessage: (m: unknown) => void = () => {};
  }
  const invoke = vi.fn(async (cmd: string, args: any) => {
    backend.calls.push([cmd, args]);
    switch (cmd) {
      case 'plugin:reactive|subscribe': {
        const sub = { id: backend.nextId++, source: args.source, page: args.page, channel: args.onEvent, closed: false };
        backend.subs.push(sub);
        return sub.id;
      }
      case 'plugin:reactive|unsubscribe':
        backend.subs.find((s) => s.id === args.id)!.closed = true;
        return true;
      case 'plugin:reactive|sink_open':
        return backend.nextId++;
      case 'plugin:reactive|sink_push':
        return null;
      case 'plugin:reactive|sink_abort':
        return null;
      case 'plugin:reactive|sink_close':
        if (backend.closeError) throw backend.closeError;
        return null;
    }
  });
  return { Channel, invoke };
});

import { createReactive, type Sink, type State } from '../guest-js/index';

type Sources = { status: State<string>; count: State<number>; p: Sink<number> };
const flush = () => new Promise((r) => setTimeout(r, 0));
const live = () => backend.subs.filter((s) => !s.closed);
const emit = (sub: FakeSub, event: unknown) => sub.channel.onmessage(event);

beforeEach(() => {
  backend.subs = [];
  backend.nextId = 1;
  backend.closeError = null;
  backend.calls = [];
});
afterEach(() => vi.restoreAllMocks());

describe('review regressions', () => {
  it('a held Source stays the canonical instance after teardown (no second backend subscription)', async () => {
    const rx = createReactive<Sources>();
    const held = rx.state('status');
    held.subscribe(() => {})();
    await flush(); // teardown ran
    const again = rx.state('status');
    expect(again).toBe(held);
    again.subscribe(() => {});
    held.subscribe(() => {});
    expect(live()).toHaveLength(1);
  });

  it('store-style subscribers survive a backend error and get values after a reconnect', async () => {
    const rx = createReactive<Sources>();
    const source = rx.state('count');
    const x = vi.fn();
    const rxjsObserver = { next: vi.fn(), error: vi.fn() };
    source.listen(x); // what React's useSyncExternalStore does
    const storeValues: number[] = [];
    source.subscribe((v) => storeValues.push(v)); // Svelte / Vue / Angular adapters
    source.subscribe(rxjsObserver);

    emit(live()[0], { type: 'error', message: 'boom' });
    expect(rxjsObserver.error).toHaveBeenCalledOnce();
    expect(x).toHaveBeenCalledTimes(1); // notified of the error

    // Another component subscribes: the source reconnects and the earlier store subscribers follow.
    source.subscribe(() => {});
    expect(backend.subs).toHaveLength(2); // exactly one reconnect (the errored one ended on the backend)
    emit(backend.subs[1], { type: 'next', value: 7 });
    expect(x).toHaveBeenCalledTimes(3); // + the reconnect clearing the error, + the new value
    expect(storeValues).toEqual([7]);
    expect(rxjsObserver.next).not.toHaveBeenCalled(); // Observable semantics: it ended
  });

  it('an observer unsubscribed during a dispatch is not called', () => {
    const rx = createReactive<Sources>();
    const source = rx.state('count');
    const second = vi.fn();
    let unsubSecond = () => {};
    source.subscribe(() => unsubSecond());
    unsubSecond = source.subscribe(second);
    emit(live()[0], { type: 'next', value: 1 });
    expect(second).not.toHaveBeenCalled();
  });

  it('a throwing observer does not stop the others', () => {
    const thrown: unknown[] = [];
    const realQueue = globalThis.queueMicrotask;
    vi.spyOn(globalThis, 'queueMicrotask').mockImplementation((fn) =>
      realQueue(() => {
        try {
          fn();
        } catch (e) {
          thrown.push(e);
        }
      }),
    );
    const rx = createReactive<Sources>();
    const source = rx.state('count');
    const other = vi.fn();
    source.subscribe(() => {
      throw new Error('observer bug');
    });
    source.subscribe(other);
    emit(live()[0], { type: 'next', value: 1 });
    expect(other).toHaveBeenCalledWith(1);
    return flush().then(() => expect(thrown).toEqual([new Error('observer bug')]));
  });

  it('a sink driven by RxJS still reports a failing handler', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    backend.closeError = { code: 'SinkFailed', message: 'handler failed' };
    const rx = createReactive<Sources>();
    const w = rx.sink('p');
    from([1, 2]).subscribe(w); // RxJS calls complete() and discards the promise
    await flush();
    await flush();
    expect(w.failure?.code).toBe('SinkFailed');
    expect(error).toHaveBeenCalledOnce();
  });

  it('an awaited complete() owns the failure (no console report)', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    backend.closeError = { code: 'SinkFailed', message: 'handler failed' };
    const rx = createReactive<Sources>();
    const w = rx.sink('p');
    w.next(1);
    await expect(w.complete()).rejects.toMatchObject({ code: 'SinkFailed' });
    await flush();
    expect(error).not.toHaveBeenCalled();
  });

  it('a real `await` in try/catch owns the sink failure (no console report)', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    backend.closeError = { code: 'SinkFailed', message: 'handler failed' };
    const rx = createReactive<Sources>();
    const w = rx.sink('p');
    w.next(1);
    let caught: unknown;
    try {
      await w.complete();
    } catch (e) {
      caught = e;
    }
    expect(caught).toMatchObject({ code: 'SinkFailed' });
    const w2 = rx.sink('p');
    await Promise.allSettled([w2.complete()]);
    await flush();
    expect(error).not.toHaveBeenCalled();
  });

  it('an observer object without an error callback still gets the error reported', () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const rx = createReactive<Sources>();
    rx.state('count').subscribe({ next: () => {} });
    emit(live()[0], { type: 'error', message: 'boom' });
    expect(error).toHaveBeenCalledOnce();
  });

  it('an error while nobody is subscribed (keep-alive window) is not reported as unhandled', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const rx = createReactive<Sources>({ keepAliveMs: 1000 });
    rx.state('count').subscribe(() => {})();
    emit(live()[0], { type: 'error', message: 'boom' });
    expect(error).not.toHaveBeenCalled();
  });

  it('a stream Source does not follow later mutations of the args object', () => {
    type S2 = { t: import('../guest-js/index').Stream<number, { i: number }> };
    const rx = createReactive<S2>();
    const a = { i: 500 };
    const s1 = rx.stream('t', a);
    a.i = 1000;
    rx.stream('t', a);
    expect(rx.stream('t', { i: 500 })).toBe(s1);
    s1.subscribe(() => {});
    expect(backend.calls.find(([c]) => c.endsWith('subscribe'))![1].args).toEqual({ i: 500 });
  });

  it('after a reconnect, every reader sees the same snapshot until a new value arrives', () => {
    const rx = createReactive<Sources>();
    const s = rx.state('count');
    const a = vi.fn();
    s.listen(a);
    emit(backend.subs[0], { type: 'next', value: 5 });
    emit(backend.subs[0], { type: 'error', message: 'boom' });
    const late = vi.fn();
    s.subscribe(late); // reconnects
    expect(s.snapshot()).toBe(5); // A and B both read 5: no tearing
    expect(late).not.toHaveBeenCalled(); // but the stale value is not replayed
    emit(backend.subs[1], { type: 'next', value: 6 });
    expect(s.snapshot()).toBe(6);
    expect(late).toHaveBeenCalledWith(6);
  });

  it('React (listen) re-renders on an error and the error is still reported', () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const rx = createReactive<Sources>();
    const s = rx.state('count');
    const onChange = vi.fn();
    s.listen(onChange);
    emit(live()[0], { type: 'next', value: 1 });
    const before = s.snapshotState();
    emit(live()[0], { type: 'error', message: 'boom' });
    const after = s.snapshotState();
    expect(after).not.toBe(before); // useSyncExternalStore sees a change -> re-render
    expect(after).toMatchObject({ value: 1, error: { code: 'StreamError' } });
    expect(s.snapshotState()).toBe(after); // stable while nothing changes
    expect(error).toHaveBeenCalledOnce();
  });

  it("Svelte's subscribe(run, invalidate) does not swallow errors", () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const rx = createReactive<Sources>();
    const invalidate = vi.fn();
    rx.state('count').subscribe(() => {}, invalidate);
    emit(live()[0], { type: 'error', message: 'boom' });
    expect(invalidate).toHaveBeenCalledOnce(); // still called
    expect(error).toHaveBeenCalledOnce(); // but not treated as handling the error
  });

  it('an observer object with an error callback handles the error (no report)', () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const rx = createReactive<Sources>();
    rx.state('count').subscribe({ error: () => {} });
    emit(live()[0], { type: 'error', message: 'boom' });
    expect(error).not.toHaveBeenCalled();
  });

  it('a producer error aborts the sink instead of completing it', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const rx = createReactive<Sources>();
    const w = rx.sink('p');
    concat(of(1, 2), throwError(() => new Error('producer failed'))).subscribe(w);
    await flush();
    await flush();
    const cmds = backend.calls.map(([c]) => c.replace('plugin:reactive|', ''));
    expect(cmds).toContain('sink_abort');
    expect(cmds).not.toContain('sink_close');
    expect(w.failure).toMatchObject({ code: 'Aborted' });
    expect(w.failure!.message).toContain('producer failed');
    expect(error).toHaveBeenCalledOnce(); // nobody awaited the outcome: reported
  });

  it('abort() rejects the outcome with Aborted and ignores later values', async () => {
    const rx = createReactive<Sources>();
    const w = rx.sink('p');
    w.next(1);
    await expect(w.abort('user cancelled')).rejects.toMatchObject({ code: 'Aborted' });
    w.next(2);
    await expect(w.complete()).rejects.toMatchObject({ code: 'Aborted' });
    await flush();
    expect(backend.calls.filter(([c]) => c.endsWith('sink_abort'))).toHaveLength(1);
  });

  it('awaiting abort() (or allSettled) owns the failure: no false console report', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const rx = createReactive<Sources>();
    try {
      await rx.sink('p').abort('cancelled');
    } catch {
      // handled
    }
    await Promise.allSettled([rx.sink('p').abort()]);
    const returned = async () => rx.sink('p').abort();
    await returned().catch(() => {});
    await new Promise((r) => setTimeout(r, 20));
    expect(error).not.toHaveBeenCalled();
  });

  it('listen subscribers are told when a reconnect clears the error (no stale error in React)', () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const rx = createReactive<Sources>();
    const s = rx.state('count');
    const onChange = vi.fn();
    s.listen(onChange);
    emit(backend.subs[0], { type: 'error', message: 'boom' });
    expect(s.snapshotState().error).toBeDefined();
    const calls = onChange.mock.calls.length;
    s.subscribe(() => {}); // another component reconnects
    expect(s.snapshotState().error).toBeUndefined();
    expect(onChange.mock.calls.length).toBe(calls + 1);
  });

  it('sends a stable per-page token with subscribe and sink_open', () => {
    const rx = createReactive<Sources>();
    rx.state('status').subscribe(() => {});
    rx.sink('p');
    const pages = backend.calls.filter(([c]) => c.endsWith('subscribe') || c.endsWith('sink_open')).map(([, a]) => a.page);
    expect(pages).toHaveLength(2);
    expect(typeof pages[0]).toBe('string');
    expect(pages[0]).toBe(pages[1]);
  });
});
