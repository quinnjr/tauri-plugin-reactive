import { Channel, invoke } from '@tauri-apps/api/core';

// ---------------------------------------------------------------------------
// Schema types: describe the sources your Rust side registers.
//
//   type Sources = {
//     counter: WritableState<number>;
//     status:  State<{ online: boolean }>;
//     ticks:   Stream<number, number>;          // args: interval in ms
//   };
//   export const rx = createReactive<Sources>();
// ---------------------------------------------------------------------------

/** A read-only state registered with `reactive.state(...)`. */
export interface State<T> {
  readonly __kind: 'state';
  readonly __value: T;
  readonly __writable: false;
}

/** A state registered with `reactive.writable_state(...)`. */
export interface WritableState<T> {
  readonly __kind: 'state';
  readonly __value: T;
  readonly __writable: true;
}

/** A stream registered with `reactive.stream(...)`; `A` is the factory's argument type. */
export interface Stream<T, A = void> {
  readonly __kind: 'stream';
  readonly __value: T;
  readonly __args: A;
}

/** A frontend → Rust stream registered with `reactive.sink(...)`; `A` is the handler's argument type. */
export interface Sink<T, A = void> {
  readonly __kind: 'sink';
  readonly __value: T;
  readonly __args: A;
}

export type SourceDef = State<any> | WritableState<any> | Stream<any, any> | Sink<any, any>;
export type Schema<S> = { [K in keyof S]: SourceDef };

type StateKey<S> = { [K in keyof S]: S[K] extends { __kind: 'state' } ? K : never }[keyof S] & string;
type StreamKey<S> = { [K in keyof S]: S[K] extends { __kind: 'stream' } ? K : never }[keyof S] & string;
type SinkKey<S> = { [K in keyof S]: S[K] extends { __kind: 'sink' } ? K : never }[keyof S] & string;
type ValueOf<D> = D extends { __value: infer T } ? T : never;
type ArgsOf<D> = D extends { __args: infer A } ? A : never;
type ArgsParam<A> = [A] extends [void] ? [args?: undefined] : undefined extends A ? [args?: A] : [args: A];
type StateSourceFor<D> = D extends { __writable: true } ? WritableStateSource<ValueOf<D>> : StateSource<ValueOf<D>>;

// ---------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------

export class ReactiveError extends Error {
  constructor(
    /**
     * `UnknownSource`, `Forbidden`, `ReadOnly`, `NotAState`, `NotSubscribable`, `NotASink`, `InvalidArgs`,
     * `InvalidValue`, `Serialize`, `StreamError`, `SinkClosed`, `SinkFailed` or `UnknownSink`.
     */
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = 'ReactiveError';
  }

  static from(err: unknown): ReactiveError {
    if (err instanceof ReactiveError) return err;
    if (err && typeof err === 'object' && 'code' in err && 'message' in err) {
      return new ReactiveError(String(err.code), String(err.message));
    }
    return new ReactiveError('Error', err instanceof Error ? err.message : String(err));
  }
}

export interface Observer<T> {
  next?: (value: T) => void;
  error?: (error: ReactiveError) => void;
  complete?: () => void;
}

/** Callable to unsubscribe, and also has `.unsubscribe()` (RxJS / Svelte compatible). */
export type Unsubscribe = (() => void) & { unsubscribe(): void };

type WireEvent =
  | { type: 'next'; value: unknown }
  | { type: 'error'; message: string }
  | { type: 'complete' };

/** `{ value, error }` of a source, as returned by `Source.snapshotState`. */
export interface SourceSnapshot<T> {
  readonly value: T | undefined;
  readonly error: ReactiveError | undefined;
}

interface ClientInternals {
  readonly keepAliveMs: number;
}

interface Entry<T> {
  observer: Observer<T>;
  /** Store-style subscription: survives backend termination (see `Source.subscribe`). */
  persistent: boolean;
  /** False once unsubscribed (or terminated, for non-persistent entries). */
  active: boolean;
  /** For `listen`: also called when only the snapshot changed (e.g. an error cleared on reconnect). */
  onSnapshot?: () => void;
}

const CMD = 'plugin:reactive|';

/**
 * Marks an object so Vue never wraps it in a reactive proxy (what `markRaw` does). Sources and
 * writers keep their state in `#private` fields, which throw when accessed through a proxy, e.g.
 * after `ref(rx.state('x'))` or storing one in `reactive()` state.
 */
function markRaw(obj: object) {
  Object.defineProperty(obj, '__v_skip', { value: true });
}

/**
 * Identifies this page load. Sent with `subscribe` / `sink_open` so the plugin can refuse calls
 * that the previous page sent just before a reload but that arrive after its cleanup.
 */
const PAGE =
  typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID()
    : `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;

/**
 * A backend source as seen from the frontend. All subscribers to the same source (and args) share
 * a single backend subscription, opened on the first `subscribe` and closed after the last
 * subscriber leaves.
 *
 * Works as an RxJS interop observable (`from(source)`), a Svelte store (`$source`), and with
 * React via `useReactive` from `tauri-plugin-reactive-api/react`.
 */
export class Source<T> {
  #observers = new Set<Entry<T>>();
  #conn: { id: Promise<number> } | null = null;
  #session = 0;
  #teardownToken = 0;
  #value: T | undefined = undefined;
  #snapshotState: SourceSnapshot<T> = { value: undefined, error: undefined };
  #hasValue = false;
  #error: ReactiveError | undefined = undefined;

  /** @internal */
  constructor(
    protected readonly client: ClientInternals,
    readonly kind: 'state' | 'stream',
    readonly name: string,
    readonly args: unknown,
    /** @internal */
    readonly key: string,
  ) {
    markRaw(this);
  }

  /** The latest value received. Kept after the last subscriber leaves and across reconnects, until a new value arrives. */
  get value(): T | undefined {
    return this.#value;
  }

  get hasValue(): boolean {
    return this.#hasValue;
  }

  /** The error that ended the last backend subscription, if any. Cleared on reconnect. */
  get error(): ReactiveError | undefined {
    return this.#error;
  }

  /** Whether a backend subscription is currently open (or opening). */
  get connected(): boolean {
    return this.#conn !== null;
  }

  /**
   * Subscribes to the source.
   *
   * - With an **observer object** (`{ next, error, complete }`, e.g. from RxJS) the subscription
   *   follows Observable semantics: a backend error or completion ends it.
   * - With **callbacks** (`subscribe(next, error?, complete?)`, the store style used by Svelte and
   *   the framework adapters) it stays registered: it is told about errors/completion, and keeps
   *   receiving values if the source is reconnected later (by any new subscriber).
   */
  subscribe(
    observerOrNext?: Observer<T> | ((value: T) => void) | null,
    error?: ((error: ReactiveError) => void) | null,
    complete?: (() => void) | null,
  ): Unsubscribe {
    // Wrap rather than store the caller's object: preserves `this` for RxJS subscribers and makes
    // each subscription distinct even when the same function is passed twice.
    const o = observerOrNext;
    if (o && typeof o === 'object') {
      return this.#add(
        {
          next: (v) => o.next?.(v),
          // Only when the caller has one: otherwise an error must still be reported as unhandled.
          error: o.error ? (e) => o.error!(e) : undefined,
          complete: () => o.complete?.(),
        },
        false,
      );
    }
    return this.#add({ next: o ?? undefined, error: error ?? undefined, complete: complete ?? undefined }, true);
  }

  #add(observer: Observer<T>, persistent: boolean, onSnapshot?: () => void): Unsubscribe {
    const entry: Entry<T> = { observer, persistent, active: true, onSnapshot };
    this.#observers.add(entry);
    this.#teardownToken++; // cancel any pending teardown
    if (!this.#conn) this.#connect();
    else if (this.kind === 'state' && this.#hasValue) this.#call(entry, (o) => o.next?.(this.#value as T));

    const unsubscribe = () => {
      if (!entry.active) return;
      entry.active = false;
      if (this.#observers.delete(entry) && this.#observers.size === 0) this.#scheduleTeardown();
    };
    return Object.assign(unsubscribe, { unsubscribe });
  }

  /** Calls one observer; an exception is rethrown asynchronously so other observers still run. */
  #call(entry: Entry<T>, fn: (o: Observer<T>) => void) {
    if (!entry.active) return; // unsubscribed earlier in this same dispatch
    try {
      fn(entry.observer);
    } catch (e) {
      queueMicrotask(() => {
        throw e;
      });
    }
  }

  /** Stable `subscribe` for `useSyncExternalStore`-style APIs (store semantics, survives termination). */
  readonly listen = (onChange: () => void): (() => void) =>
    this.#add({ next: onChange, error: onChange, complete: onChange }, true, onChange);

  /** Stable snapshot getter for `useSyncExternalStore`-style APIs. */
  readonly snapshot = (): T | undefined => this.#value;

  /**
   * Stable getter for `{ value, error }`. A new object whenever either changes, so a
   * `useSyncExternalStore`-style consumer also re-renders when the source errors.
   */
  readonly snapshotState = (): SourceSnapshot<T> => this.#snapshotState;

  /** Refreshes `snapshotState`; returns whether it changed. */
  #publish(): boolean {
    if (this.#snapshotState.value === this.#value && this.#snapshotState.error === this.#error) return false;
    this.#snapshotState = { value: this.#value, error: this.#error };
    return true;
  }

  #connect() {
    const session = ++this.#session;
    // Not replayed to new subscribers any more (it's from the previous connection), but kept as
    // the snapshot until a new value arrives, so every reader of `value`/`snapshot` stays consistent.
    this.#hasValue = false;
    this.#error = undefined;
    if (this.#publish()) {
      // Only the snapshot changed (the previous error was cleared): snapshot readers must re-read it.
      for (const entry of [...this.#observers]) if (entry.onSnapshot) this.#call(entry, () => entry.onSnapshot!());
    }

    const channel = new Channel<WireEvent>();
    channel.onmessage = (event) => {
      if (session === this.#session) this.#onEvent(event);
    };
    const id = invoke<number>(`${CMD}subscribe`, { source: this.name, args: this.args ?? null, page: PAGE, onEvent: channel });
    this.#conn = { id };
    id.catch((err) => {
      if (session === this.#session) this.#terminate(ReactiveError.from(err));
    });
  }

  #onEvent(event: WireEvent) {
    switch (event.type) {
      case 'next': {
        const value = event.value as T;
        this.#value = value;
        this.#publish();
        this.#hasValue = true;
        for (const entry of [...this.#observers]) this.#call(entry, (o) => o.next?.(value));
        break;
      }
      case 'error':
        this.#terminate(new ReactiveError('StreamError', event.message));
        break;
      case 'complete':
        this.#terminate();
        break;
    }
  }

  /** The backend ended the subscription (completed, errored, or failed to open). */
  #terminate(error?: ReactiveError) {
    const entries = [...this.#observers];
    // Observer-object subscriptions end here; store-style ones stay registered (see `subscribe`).
    for (const entry of entries) if (!entry.persistent) this.#observers.delete(entry);
    this.#conn = null;
    this.#session++;
    this.#teardownToken++;
    this.#error = error;
    this.#publish();

    for (const entry of entries) this.#call(entry, (o) => (error ? o.error?.(error) : o.complete?.()));
    for (const entry of entries) if (!entry.persistent) entry.active = false;
    // Unhandled = someone was listening but nobody could handle it (not: nobody was listening).
    // Only an observer object with an `error` callback handles errors. Store-style callbacks don't
    // count: their second argument may not be an error handler at all (Svelte passes `invalidate`),
    // and `listen` (React) only re-renders.
    if (error && entries.length > 0 && !entries.some((e) => !e.persistent && e.observer.error)) {
      console.error(`[tauri-plugin-reactive] unhandled error from "${this.name}":`, error);
    }
  }

  #scheduleTeardown() {
    const token = ++this.#teardownToken;
    const run = () => {
      if (token === this.#teardownToken && this.#observers.size === 0) this.#disconnect();
    };
    // Deferring (even by a microtask) absorbs unsubscribe/resubscribe churn such as React StrictMode.
    if (this.client.keepAliveMs > 0) setTimeout(run, this.client.keepAliveMs);
    else queueMicrotask(run);
  }

  #disconnect() {
    const conn = this.#conn;
    if (!conn) return;
    this.#conn = null;
    this.#session++;
    conn.id.then((id) => invoke(`${CMD}unsubscribe`, { id })).catch(() => {});
  }
}

// Same global declaration RxJS ships, so `from(source)` type-checks.
declare global {
  interface SymbolConstructor {
    readonly observable: symbol;
  }
}
export interface Source<T> {
  [Symbol.observable](): Source<T>;
}

const observableKey: symbol | undefined =
  typeof Symbol === 'function' ? (Symbol as unknown as { observable?: symbol }).observable : undefined;
for (const key of [observableKey, '@@observable']) {
  if (key) {
    Object.defineProperty(Source.prototype, key, {
      value: function (this: Source<unknown>) {
        return this;
      },
    });
  }
}

export class StateSource<T> extends Source<T> {
  /** Fetches the current value directly, without subscribing. */
  get(): Promise<T> {
    return invoke<T>(`${CMD}get`, { source: this.name }).catch((e) => Promise.reject(ReactiveError.from(e)));
  }
}

export class WritableStateSource<T> extends StateSource<T> {
  /** Sets the value in Rust. Subscribers (including this one) receive it through the normal update path. */
  set(value: T): Promise<void> {
    return invoke<void>(`${CMD}set`, { source: this.name, value }).catch((e) => Promise.reject(ReactiveError.from(e)));
  }
}

export type StreamSource<T> = Source<T>;

/**
 * The outcome of a sink: awaitable like a promise (`await`, `.then`, `.catch`, `.finally`,
 * `Promise.all`). It is a thenable rather than a native Promise on purpose: `await` on a native
 * Promise never calls its `then`, so only a thenable can tell that someone is handling the result.
 * A failure nobody handles (e.g. a writer driven by RxJS, which drops `complete()`'s result) is
 * reported with `console.error` instead of disappearing.
 */
export class SinkOutcome implements Promise<void> {
  /** Makes it a `Promise<void>` structurally, so it can be passed wherever a Promise is expected. */
  readonly [Symbol.toStringTag] = 'SinkOutcome';

  /** @internal */
  constructor(
    private readonly promise: Promise<void>,
    private readonly markObserved: () => void,
  ) {}

  then<A = void, B = never>(
    onFulfilled?: ((value: void) => A | PromiseLike<A>) | null,
    onRejected?: ((reason: ReactiveError) => B | PromiseLike<B>) | null,
  ): Promise<A | B> {
    this.markObserved();
    return this.promise.then(onFulfilled, onRejected);
  }

  catch<B = never>(onRejected?: ((reason: ReactiveError) => B | PromiseLike<B>) | null): Promise<void | B> {
    return this.then(undefined, onRejected);
  }

  finally(onFinally?: (() => void) | null): Promise<void> {
    this.markObserved();
    return this.promise.finally(onFinally);
  }
}

/**
 * Pushes values from the frontend into a Rust sink handler (`reactive.sink(...)`), in order.
 *
 * Values passed to `next()` in the same tick are sent as one batch, and only one batch is in
 * flight at a time, so a slow Rust handler slows the writer down instead of piling up IPC calls.
 * It is also an RxJS-style observer: `fromEvent(el, 'pointermove').pipe(...).subscribe(rx.sink('pointer'))`.
 */
export class SinkWriter<T> {
  #id: Promise<number>;
  #queue: T[] = [];
  #flushing: Promise<void> | null = null;
  #state: 'open' | 'completing' | 'closed' = 'open';
  #closed: Promise<void>;
  #settle!: (error?: ReactiveError) => void;
  #failure: ReactiveError | undefined;
  #settled = false;
  #observed = false;
  /** `sink_close` was sent: the run completes on the backend and can no longer be aborted. */
  #closeSent = false;
  /** `abort()` was called; its `sink_abort` answer settles the outcome. */
  #aborting = false;

  /** @internal */
  constructor(
    readonly name: string,
    readonly args: unknown,
  ) {
    markRaw(this);
    this.#closed = new Promise<void>((resolve, reject) => {
      this.#settle = (error) => {
        if (this.#settled) return;
        this.#settled = true;
        this.#state = 'closed';
        this.#queue = [];
        if (error) {
          this.#failure = error;
          // Checked after the current turn, so a handler attached right after the failing call counts.
          // A macrotask: `await` / `Promise.all` on a thenable call `then` from a later microtask,
          // which must get the chance to mark the outcome handled first.
          setTimeout(() => {
            if (!this.#observed) console.error(`[tauri-plugin-reactive] sink "${name}" failed:`, error);
          });
          reject(error);
        } else {
          resolve();
        }
      };
    });
    this.#closed.catch(() => {}); // reported via `closed` / `complete()`; never an unhandled rejection
    this.#id = invoke<number>(`${CMD}sink_open`, { source: name, args: args ?? null, page: PAGE });
    this.#id.catch((e) => this.#settle(ReactiveError.from(e)));
  }

  /** Settles when the sink ends: resolves after the Rust handler finished OK, rejects with its error. */
  get closed(): SinkOutcome {
    return new SinkOutcome(this.#closed, () => (this.#observed = true));
  }

  /** Whether the writer still accepts values. */
  get isOpen(): boolean {
    return this.#state === 'open';
  }

  /** Why the sink ended, if it failed. */
  get failure(): ReactiveError | undefined {
    return this.#failure;
  }

  /** Queues a value. Ignored once the writer is completing or closed. */
  next(value: T): void {
    if (this.#state !== 'open') return;
    this.#queue.push(value);
    this.#flushing ??= this.#flush();
  }

  /** Sends anything queued, ends the stream, and resolves with the Rust handler's result. */
  complete(): SinkOutcome {
    this.#end();
    return this.closed;
  }

  /** Observer `error`: the producer failed, so the sink is aborted (see `abort`). */
  error(error?: unknown): void {
    void this.abort(error);
  }

  /**
   * Abandons the sink: queued values are dropped and the Rust handler is aborted (its future is
   * dropped, so it never runs the code after its loop). The outcome rejects with code `Aborted`.
   * Use this when the data is incomplete; use `complete()` when it is done.
   */
  abort(reason?: unknown): SinkOutcome {
    // Once `sink_close` has been sent the backend completes the run, so it's too late to abort:
    // the outcome reports that close's result instead of claiming an abort that didn't happen.
    if (this.#state !== 'closed' && !this.#closeSent) {
      const message = reason instanceof Error ? reason.message : reason === undefined ? 'aborted' : String(reason);
      const aborted = new ReactiveError('Aborted', `sink "${this.name}" aborted: ${message}`);
      // Stop accepting and sending right away; settle from the backend's answer, because the
      // handler may already have finished on its own (and committed, or failed).
      this.#state = 'closed';
      this.#queue = [];
      this.#aborting = true;
      this.#id
        .then((id) => invoke<'aborted' | 'completed'>(`${CMD}sink_abort`, { id }))
        .then(
          (outcome) => (outcome === 'completed' ? this.#settle() : this.#settle(aborted)),
          (e) => {
            const error = ReactiveError.from(e);
            this.#settle(error.code === 'SinkFailed' ? error : aborted); // e.g. UnknownSink: already released
          },
        );
    }
    return this.closed;
  }

  #end() {
    if (this.#state === 'open') {
      this.#state = 'completing';
      void this.#finish();
    }
  }

  async #flush(): Promise<void> {
    let id: number;
    try {
      id = await this.#id;
    } catch {
      this.#flushing = null;
      return; // open failed; already settled
    }
    try {
      while (this.#queue.length > 0 && this.#state !== 'closed') {
        const values = this.#queue.splice(0);
        await invoke(`${CMD}sink_push`, { id, values });
      }
    } catch (e) {
      await this.#failAndClose(id, ReactiveError.from(e));
    } finally {
      this.#flushing = null;
    }
  }

  /** A method (not an inline comparison) so TypeScript doesn't narrow `#state` across awaits. */
  #isClosed(): boolean {
    return this.#state === 'closed';
  }

  async #finish(): Promise<void> {
    while (this.#flushing) await this.#flushing;
    if (this.#state === 'closed') return;
    try {
      const id = await this.#id;
      // Re-check: `abort()` may have been called while we waited for the sink to open. Its
      // `sink_abort` is queued behind this await, so sending `sink_close` now would complete
      // (commit) a run the caller just abandoned.
      if (this.#isClosed()) return;
      this.#closeSent = true;
      await invoke(`${CMD}sink_close`, { id });
      this.#settle();
    } catch (e) {
      this.#settle(ReactiveError.from(e));
    }
  }

  /** A push failed. Close the backend sink (freeing it) and prefer the handler's own error. */
  /**
   * A push failed. The stream is incomplete, so the handler must not see a normal end: abort it.
   * The one exception is `SinkClosed`, meaning the handler already finished on its own; then close
   * to collect its result (its own error is more useful than "closed").
   */
  async #failAndClose(id: number, error: ReactiveError) {
    if (this.#aborting) return; // an abort() in progress owns the backend sink and the outcome
    this.#state = 'closed'; // stop accepting and sending values right away
    if (error.code === 'SinkClosed') {
      try {
        await invoke(`${CMD}sink_close`, { id });
      } catch (e) {
        error = ReactiveError.from(e);
      }
    } else {
      await invoke(`${CMD}sink_abort`, { id }).catch(() => {}); // may already be aborted by the backend
    }
    this.#settle(error);
  }
}

export interface ReactiveOptions {
  /**
   * How long to keep a backend subscription open after its last subscriber leaves, so a quick
   * resubscribe (route change, list re-render) reuses it. Default `0` (end of the current microtask).
   */
  keepAliveMs?: number;
}

export interface ReactiveClient<S> {
  state<K extends StateKey<S>>(name: K): StateSourceFor<S[K]>;
  stream<K extends StreamKey<S>>(name: K, ...args: ArgsParam<ArgsOf<S[K]>>): StreamSource<ValueOf<S[K]>>;
  /** Opens a new writer to a Rust sink. Each call starts its own run of the Rust handler. */
  sink<K extends SinkKey<S>>(name: K, ...args: ArgsParam<ArgsOf<S[K]>>): SinkWriter<ValueOf<S[K]>>;
}

function stableStringify(value: unknown): string {
  return JSON.stringify(value, (_key, v) =>
    v && typeof v === 'object' && !Array.isArray(v)
      ? Object.fromEntries(Object.entries(v).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)))
      : v,
  );
}

/**
 * Creates a typed client. Calling `state(name)` / `stream(name, args)` repeatedly with the same
 * name and (structurally equal) args returns the same `Source` while it is in use.
 */
export function createReactive<S extends Schema<S>>(options: ReactiveOptions = {}): ReactiveClient<S> {
  const internals: ClientInternals = { keepAliveMs: options.keepAliveMs ?? 0 };

  // One canonical Source per key for as long as anything references it. Weak references mean an
  // unused Source (e.g. from a discarded render) is collected, while one that is still held, or
  // connected (its IPC channel callback references it), is always the one handed out, so two
  // Sources can never hold separate backend subscriptions for the same key.
  const weak = typeof WeakRef === 'function';
  const cache = new Map<string, WeakRef<Source<any>> | Source<any>>();
  const cleanup =
    weak && typeof FinalizationRegistry === 'function'
      ? new FinalizationRegistry<string>((key) => {
          const ref = cache.get(key);
          if (ref instanceof WeakRef && ref.deref() === undefined) cache.delete(key);
        })
      : undefined;

  function lookup<T extends Source<any>>(kind: 'state' | 'stream', name: string, args: unknown, make: (key: string) => T): T {
    const key = `${kind}\u0000${name}\u0000${stableStringify(args ?? null)}`;
    const cached = cache.get(key);
    let source = (cached instanceof Source ? cached : cached?.deref()) as T | undefined;
    if (!source) {
      source = make(key);
      cache.set(key, weak ? new WeakRef(source) : source);
      cleanup?.register(source, key);
    }
    return source;
  }

  return {
    state(name) {
      // Every state gets the writable class; the schema type decides whether `set` is visible.
      return lookup('state', name, null, (key) => new WritableStateSource(internals, 'state', name, null, key)) as any;
    },
    stream(name, ...[args]) {
      // Store a snapshot of the args: the Source is cached under a key built from them, so it must
      // not follow later mutations of the caller's object (e.g. a Vue `reactive()` or a reused object).
      const snapshot = args === undefined ? null : JSON.parse(stableStringify(args));
      return lookup('stream', name, args, (key) => new Source(internals, 'stream', name, snapshot, key)) as any;
    },
    sink(name, ...[args]) {
      return new SinkWriter(name, args ?? null) as any;
    },
  };
}
