import { beforeEach, describe, expect, it, vi } from 'vitest';
import { from, interval, map, take } from 'rxjs';

// --- Fake backend sink ------------------------------------------------------------------------
const backend = {
  nextId: 1,
  sinks: new Map<number, { source: string; args: unknown; received: unknown[]; closed: boolean; aborted?: boolean }>(),
  calls: [] as Array<[string, any]>,
  /** Resolves pushes; tests can hold them open to simulate a slow handler. */
  pushGate: null as Promise<void> | null,
  pushError: null as unknown,
  closeError: null as unknown,
  openError: null as unknown,
  /** What sink_abort answers: 'aborted' | 'completed', or an error object to reject with. */
  abortResult: 'aborted' as unknown,
};

vi.mock('@tauri-apps/api/core', () => {
  class Channel {
    onmessage: (m: unknown) => void = () => {};
  }
  const invoke = vi.fn(async (cmd: string, args: any) => {
    backend.calls.push([cmd, structuredClone(args)]);
    switch (cmd) {
      case 'plugin:reactive|sink_open': {
        if (backend.openError) throw backend.openError;
        const id = backend.nextId++;
        backend.sinks.set(id, { source: args.source, args: args.args, received: [], closed: false });
        return id;
      }
      case 'plugin:reactive|sink_push': {
        if (backend.pushGate) await backend.pushGate;
        if (backend.pushError) throw backend.pushError;
        backend.sinks.get(args.id)!.received.push(...args.values);
        return null;
      }
      case 'plugin:reactive|sink_close': {
        backend.sinks.get(args.id)!.closed = true;
        if (backend.closeError) throw backend.closeError;
        return null;
      }
      case 'plugin:reactive|sink_abort': {
        backend.sinks.get(args.id)!.aborted = true;
        if (typeof backend.abortResult !== 'string') throw backend.abortResult;
        return backend.abortResult;
      }
    }
    throw new Error(`unexpected ${cmd}`);
  });
  return { Channel, invoke };
});

import { createReactive, ReactiveError, type Sink, type State } from '../guest-js/index';

type Sources = {
  count: State<number>;
  pointer: Sink<{ x: number; y: number }>;
  log: Sink<string, { level: string }>;
};

const flush = () => new Promise((r) => setTimeout(r, 0));
const pushes = () => backend.calls.filter(([c]) => c === 'plugin:reactive|sink_push').map(([, a]) => a.values);

beforeEach(() => {
  backend.nextId = 1;
  backend.sinks.clear();
  backend.calls = [];
  backend.pushGate = null;
  backend.pushError = null;
  backend.closeError = null;
  backend.openError = null;
  backend.abortResult = 'aborted';
});

describe('SinkWriter', () => {
  it('opens with args, batches values pushed in the same tick, and completes', async () => {
    const rx = createReactive<Sources>();
    const w = rx.sink('log', { level: 'info' });
    w.next('a');
    w.next('b');
    w.next('c');
    await flush();
    w.next('d');
    await w.complete();

    const sink = backend.sinks.get(1)!;
    expect(sink).toMatchObject({ source: 'log', args: { level: 'info' }, closed: true });
    expect(sink.received).toEqual(['a', 'b', 'c', 'd']);
    expect(pushes()).toEqual([['a', 'b', 'c'], ['d']]);
    expect(w.isOpen).toBe(false);
  });

  it('keeps one push in flight and preserves order while the handler is slow', async () => {
    let release!: () => void;
    backend.pushGate = new Promise((r) => (release = r));
    const rx = createReactive<Sources>();
    const w = rx.sink('log', { level: 'x' });

    w.next('1');
    await flush();
    for (const v of ['2', '3', '4']) w.next(v); // queued behind the in-flight push
    await flush();
    expect(pushes()).toEqual([['1']]);

    release();
    backend.pushGate = null;
    await w.complete();
    expect(pushes()).toEqual([['1'], ['2', '3', '4']]);
    expect(backend.sinks.get(1)!.received).toEqual(['1', '2', '3', '4']);
  });

  it('ignores values after complete()', async () => {
    const rx = createReactive<Sources>();
    const w = rx.sink('pointer');
    w.next({ x: 1, y: 1 });
    const done = w.complete();
    w.next({ x: 2, y: 2 });
    await done;
    expect(backend.sinks.get(1)!.received).toEqual([{ x: 1, y: 1 }]);
    expect(backend.calls[0][1].args).toBeNull(); // no-args sink sends null
  });

  it('rejects complete() with the handler error', async () => {
    backend.closeError = { code: 'SinkFailed', message: 'out of order' };
    const rx = createReactive<Sources>();
    const w = rx.sink('pointer');
    w.next({ x: 1, y: 1 });
    await expect(w.complete()).rejects.toMatchObject({ code: 'SinkFailed', message: 'out of order' });
    expect(w.failure).toBeInstanceOf(ReactiveError);
  });

  it('on a push failure, closes the backend sink and reports the handler error', async () => {
    backend.pushError = { code: 'SinkClosed', message: 'sink `pointer` has stopped accepting values' };
    backend.closeError = { code: 'SinkFailed', message: 'handler gave up' };
    const rx = createReactive<Sources>();
    const w = rx.sink('pointer');
    const closed = w.closed;
    w.next({ x: 1, y: 1 });
    await expect(closed).rejects.toMatchObject({ code: 'SinkFailed', message: 'handler gave up' });
    expect(backend.sinks.get(1)!.closed).toBe(true); // freed on the backend
    expect(w.isOpen).toBe(false);
  });

  it('an invalid value aborts the sink (never closes it: the run is incomplete)', async () => {
    backend.pushError = { code: 'InvalidValue', message: 'invalid value for `pointer`' };
    const rx = createReactive<Sources>();
    const w = rx.sink('pointer');
    const closed = w.closed;
    w.next({ x: 1, y: 1 });
    await expect(closed).rejects.toMatchObject({ code: 'InvalidValue' });
    expect(backend.sinks.get(1)).toMatchObject({ aborted: true, closed: false });
  });

  it('complete() then abort() in the same tick aborts (never closes) the backend sink', async () => {
    const rx = createReactive<Sources>();
    const w = rx.sink('pointer');
    w.next({ x: 1, y: 1 });
    await flush(); // push done, nothing in flight
    void w.complete();
    await expect(w.abort('cancel')).rejects.toMatchObject({ code: 'Aborted' });
    await flush();
    const cmds = backend.calls.map(([c]) => c.replace('plugin:reactive|', ''));
    expect(cmds).toContain('sink_abort');
    expect(cmds).not.toContain('sink_close');
    expect(backend.sinks.get(1)).toMatchObject({ aborted: true, closed: false });
  });

  it('complete() then abort() while the open is still pending also aborts', async () => {
    const rx = createReactive<Sources>();
    const w = rx.sink('pointer'); // sink_open not resolved yet
    void w.complete();
    void w.abort('cancel').then(undefined, () => {});
    await flush();
    const cmds = backend.calls.map(([c]) => c.replace('plugin:reactive|', ''));
    expect(cmds).not.toContain('sink_close');
    expect(cmds).toContain('sink_abort');
  });

  it('abort() after sink_close was sent is too late: the outcome is the close result', async () => {
    const rx = createReactive<Sources>();
    const w = rx.sink('pointer');
    await flush(); // opened
    const outcome = w.complete();
    await flush(); // sink_close sent (and resolved)
    await expect(w.abort('late')).resolves.toBeUndefined();
    await expect(outcome).resolves.toBeUndefined();
    const cmds = backend.calls.map(([c]) => c.replace('plugin:reactive|', ''));
    expect(cmds).toContain('sink_close');
    expect(cmds).not.toContain('sink_abort');
  });

  it('abort() of a handler that already finished on its own reports the real outcome', async () => {
    const rx = createReactive<Sources>();
    // Completed: the run committed, so the outcome resolves instead of claiming an abort.
    backend.abortResult = 'completed';
    await expect(rx.sink('pointer').abort('cancel')).resolves.toBeUndefined();
    // Failed: its own error, not "Aborted".
    backend.abortResult = { code: 'SinkFailed', message: 'gave up' };
    await expect(rx.sink('pointer').abort('cancel')).rejects.toMatchObject({ code: 'SinkFailed', message: 'gave up' });
    // Still running: really aborted.
    backend.abortResult = 'aborted';
    await expect(rx.sink('pointer').abort('cancel')).rejects.toMatchObject({ code: 'Aborted' });
  });

  it('reports open failures (e.g. Forbidden)', async () => {
    backend.openError = { code: 'Forbidden', message: 'not allowed' };
    const rx = createReactive<Sources>();
    const w = rx.sink('pointer');
    await expect(w.complete()).rejects.toMatchObject({ code: 'Forbidden' });
  });

  it('works as an RxJS observer, ending when the observable completes', async () => {
    const rx = createReactive<Sources>();
    const w = rx.sink('log', { level: 'rx' });
    from(['x', 'y', 'z']).subscribe(w);
    await w.closed;
    expect(backend.sinks.get(1)!.received).toEqual(['x', 'y', 'z']);

    const w2 = rx.sink('pointer');
    interval(1).pipe(take(5), map((n) => ({ x: n, y: n }))).subscribe(w2);
    await w2.closed;
    expect(backend.sinks.get(2)!.received).toHaveLength(5);
  });

  it('is typed per sink', () => {
    const rx = createReactive<Sources>();
    // @ts-expect-error log requires args
    rx.sink('log');
    // @ts-expect-error count is a state, not a sink
    rx.sink('count');
    // @ts-expect-error wrong value type
    rx.sink('pointer').next('nope');
    // @ts-expect-error sinks cannot be subscribed to as streams
    rx.stream('pointer');
  });
});
