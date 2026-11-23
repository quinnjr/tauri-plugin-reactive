/**
 * End-to-end checks over the real Tauri IPC. Run with `npm run selftest` in any example: the app
 * runs these, prints a report via the `self_test_report` command and exits 0 (pass) or 1 (fail).
 *
 * Phase 1 runs most checks (including the framework probe), then reloads the page while holding
 * subscriptions. Phase 2 (after the reload) checks that the backend released them.
 */
import { Channel, invoke } from '@tauri-apps/api/core';
import { emit, listen } from '@tauri-apps/api/event';
import { WebviewWindow } from '@tauri-apps/api/webviewWindow';
import { concat, from, lastValueFrom, range, take, throwError, toArray } from 'rxjs';
import type { ReactiveError, Source } from 'tauri-plugin-reactive-api';
import { rx } from './reactive';

const SELF_TEST_RELOAD_KEY = 'reactive-self-test-results';

/**
 * Each example mounts a small component through its framework adapter. The component must render
 * the text `clicks:<n> ticks:<n> volume:<n>` using `rx.state('clicks')`,
 * `rx.stream('ticks', { intervalMs: 40 })` and `rx.state('settings')`.
 */
export interface FrameworkProbe {
  name: string;
  mount(container: HTMLElement): Promise<() => void | Promise<void>>;
}

interface Result {
  name: string;
  ok: boolean;
  detail?: string;
}

type Check = [string, () => Promise<void>];

const active = () => invoke<number>('active_subscriptions');
const activeSinks = () => invoke<number>('active_sinks');
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function eventually(what: string, check: () => boolean | Promise<boolean>, timeoutMs = 3000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await sleep(20);
  }
  throw new Error(`timed out waiting for: ${what}`);
}

/** Bounds an await, so one hang fails its own check instead of the whole run (via the watchdog). */
function withTimeout<T>(promise: PromiseLike<T>, ms: number, what: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out after ${ms}ms: ${what}`)), ms);
    promise.then(
      (v) => (clearTimeout(timer), resolve(v)),
      (e) => (clearTimeout(timer), reject(e)),
    );
  });
}

/** Like `eventually`, but the condition must then keep holding for `holdMs` (no transient match). */
async function eventuallyStable(what: string, check: () => boolean | Promise<boolean>, holdMs = 300, timeoutMs = 3000) {
  const deadline = Date.now() + timeoutMs;
  let since: number | undefined;
  while (Date.now() < deadline) {
    if (await check()) {
      since ??= Date.now();
      if (Date.now() - since >= holdMs) return;
    } else {
      since = undefined;
    }
    await sleep(20);
  }
  throw new Error(`timed out waiting for (stable): ${what}`);
}

/** JSON with object keys sorted, so equality doesn't depend on key order (e.g. after a round trip through Rust). */
function canonical(value: unknown): string {
  return JSON.stringify(value, (_k, v) =>
    v && typeof v === 'object' && !Array.isArray(v) ? Object.fromEntries(Object.entries(v).sort(([a], [b]) => a.localeCompare(b))) : v,
  );
}

function assertEq(actual: unknown, expected: unknown, what: string) {
  if (canonical(actual) !== canonical(expected)) {
    throw new Error(`${what}: expected ${canonical(expected)}, got ${canonical(actual)}`);
  }
}

/**
 * Everything a check opens is registered here and torn down after the check, pass or fail, so a
 * failing check doesn't leak subscriptions/sinks into the counts later checks assert on. Best effort:
 * a sink whose close was already sent can't be aborted any more, so one stuck in `complete()` stays
 * open until its handler finishes (later sink counts would then fail too).
 */
const cleanups: Array<() => void> = [];
function track<U extends () => void>(unsubscribe: U): U {
  cleanups.push(unsubscribe);
  return unsubscribe;
}
function trackSink<W extends { abort(): unknown; isOpen: boolean }>(writer: W): W {
  cleanups.push(() => {
    if (writer.isOpen) void writer.abort();
  });
  return writer;
}

/** Subscribes until the source completes or errors, collecting everything. */
function collect<T>(source: Source<T>, timeoutMs = 3000) {
  return new Promise<{ values: T[]; error?: ReactiveError; completed: boolean }>((resolve, reject) => {
    const values: T[] = [];
    const timer = setTimeout(() => reject(new Error(`no terminal event; got ${JSON.stringify(values)}`)), timeoutMs);
    track(
      source.subscribe({
        next: (v) => values.push(v),
        error: (error) => (clearTimeout(timer), resolve({ values, error, completed: false })),
        complete: () => (clearTimeout(timer), resolve({ values, completed: true })),
      }),
    );
  });
}

async function expectReject(p: Promise<unknown>, code: string) {
  try {
    await p;
  } catch (e) {
    assertEq((e as ReactiveError).code, code, 'error code');
    return;
  }
  throw new Error(`expected rejection with ${code}`);
}

function probeCheck(probe: FrameworkProbe): Check {
  return [`${probe.name}: component renders values from Rust and releases them on unmount`, async () => {
    const el = document.createElement('div');
    document.body.append(el);
    const read = (key: string) => {
      const m = new RegExp(`${key}:(\\d+)`).exec(el.textContent ?? '');
      return m ? Number(m[1]) : undefined;
    };
    const unmount = await probe.mount(el);
    try {
      await eventually(`rendered clicks (DOM: "${el.textContent}")`, () => read('clicks') !== undefined);
      const before = read('clicks')!;
      await invoke('click');
      await eventually('re-rendered after Rust update', () => read('clicks') === before + 1);
      await eventually('stream ticks rendered', () => (read('ticks') ?? -1) >= 1);
      await rx.state('settings').set({ theme: 'dark', volume: 77 });
      await eventually('re-rendered after set()', () => read('volume') === 77);
      assertEq(await active(), 3, 'backend subscriptions while mounted');
    } finally {
      await unmount();
      el.remove();
    }
    await eventually('released after unmount', async () => (await active()) === 0);
  }];
}

function phase1(probe: FrameworkProbe): Check[] {
  return [
    ['no backend subscriptions or sinks at start', async () => {
      assertEq(await active(), 0, 'active subscriptions');
      assertEq(await activeSinks(), 0, 'active sinks');
    }],

    ['state: current value, then Rust-side updates', async () => {
      const values: number[] = [];
      const unsub = track(rx.state('clicks').subscribe((v) => values.push(v)));
      await eventually('initial value', () => values.length === 1);
      await invoke('click');
      await invoke('click');
      await eventually('two clicks', () => values.at(-1) === values[0] + 2);
      unsub();
    }],

    ['state: get() without subscribing', async () => {
      assertEq(typeof (await rx.state('clicks').get()), 'number', 'typeof clicks');
    }],

    ['writable state: set() reaches subscribers', async () => {
      let latest: unknown;
      const unsub = track(rx.state('settings').subscribe((v) => (latest = v)));
      await rx.state('settings').set({ theme: 'light', volume: 30 });
      await eventually('updated settings', () => JSON.stringify(latest) === JSON.stringify({ theme: 'light', volume: 30 }));
      unsub();
    }],

    ['writable state: Rust validation rejects bad values', async () => {
      await expectReject(rx.state('settings').set({ theme: 'light', volume: 150 }), 'InvalidValue');
      await expectReject(rx.state('settings').set({ theme: 'neon' as 'dark', volume: 10 }), 'InvalidValue');
      assertEq(await rx.state('settings').get(), { theme: 'light', volume: 30 }, 'settings unchanged');
    }],

    ['read-only state rejects set()', async () => {
      await expectReject((rx.state('clicks') as any).set(99), 'ReadOnly');
    }],

    ['stream: args are passed and finite streams complete', async () => {
      const r = await collect(rx.stream('ticks', { intervalMs: 10, count: 4 }));
      assertEq(r.values, [0, 1, 2, 3], 'values');
      assertEq(r.completed, true, 'completed');
    }],

    ['stream: Rust Err is delivered as a terminal StreamError', async () => {
      const r = await collect(rx.stream('flaky', { intervalMs: 10, failAfter: 2 }));
      assertEq(r.values, [0, 1], 'values before error');
      assertEq(r.error?.code, 'StreamError', 'error code');
      if (!r.error?.message.includes('failed after 2')) throw new Error(`unexpected message: ${r.error?.message}`);
    }],

    ['stream: invalid args are rejected', async () => {
      const r = await collect(rx.stream('ticks', { wrong: true } as any));
      assertEq(r.error?.code, 'InvalidArgs', 'error code');
    }],

    ['unknown source is rejected', async () => {
      const r = await collect(rx.state('doesNotExist' as any));
      assertEq(r.error?.code, 'UnknownSource', 'error code');
    }],

    ['finished subscriptions are removed from the backend', async () => {
      await eventually('0 active', async () => (await active()) === 0);
    }],

    ['subscribers to the same source + args share one backend subscription', async () => {
      const seen = [0, 0, 0];
      const unsubs = [
        track(rx.stream('ticks', { intervalMs: 20 }).subscribe(() => seen[0]++)),
        track(rx.stream('ticks', { intervalMs: 20 }).subscribe(() => seen[1]++)),
        track(rx.stream('ticks', { intervalMs: 25 }).subscribe(() => seen[2]++)),
      ];
      await eventually('all receiving', () => seen.every((n) => n >= 2));
      assertEq(await active(), 2, 'active subscriptions (2 distinct args)');
      unsubs.forEach((u) => u());
      await eventually('released after last unsubscribe', async () => (await active()) === 0);
    }],

    ['RxJS from() + take() tears down the backend', async () => {
      const values = await withTimeout(lastValueFrom(from(rx.stream('ticks', { intervalMs: 15 })).pipe(take(3), toArray())), 5000, 'take(3)');
      assertEq(values, [0, 1, 2], 'values');
      await eventually('released', async () => (await active()) === 0);
    }],

    ['permissions: a source denied by the capability is Forbidden for subscribe, get and set', async () => {
      const r = await collect(rx.state('adminToken'));
      assertEq(r.error?.code, 'Forbidden', 'subscribe error code');
      await expectReject(rx.state('adminToken').get(), 'Forbidden');
      await expectReject(rx.state('adminToken').set('hacked'), 'Forbidden');
    }],

    ['permissions: a per-command scope makes motd readable but not writable', async () => {
      assertEq(await rx.state('motd').get(), 'Welcome!', 'get');
      let seen: string | undefined;
      const unsub = track(rx.state('motd').subscribe((v) => (seen = v)));
      await eventually('subscribed to motd', () => seen === 'Welcome!');
      unsub();
      await expectReject(rx.state('motd').set('hacked'), 'Forbidden');
      assertEq(await rx.state('motd').get(), 'Welcome!', 'motd after denied set');
    }],

    ['sink: 150 values (more than the 64-value buffer) reach a slow Rust handler in order; complete() waits for it', async () => {
      const w = trackSink(rx.sink('sequence', { delayMs: 1 }));
      for (let i = 0; i < 150; i++) w.next(i);
      await eventually('sink open', async () => (await activeSinks()) === 1);
      await withTimeout(w.complete(), 10_000, 'complete()');
      assertEq(await rx.state('sequenceTotal').get(), 150, 'values seen by the handler');
      assertEq(await activeSinks(), 0, 'open sinks after complete()');
    }],

    ['sink: a Rust handler error rejects complete() with SinkFailed', async () => {
      const w = trackSink(rx.sink('sequence', { delayMs: 0 }));
      for (const v of [0, 1, 2, 7]) w.next(v);
      try {
        await withTimeout(w.complete(), 10_000, 'complete()');
        throw new Error('complete() resolved');
      } catch (e) {
        assertEq((e as ReactiveError).code, 'SinkFailed', 'error code');
        if (!(e as ReactiveError).message.includes('expected 3, got 7')) throw new Error(`message: ${(e as Error).message}`);
      }
      await eventually('sink released', async () => (await activeSinks()) === 0);
    }],

    ['sink: a value of the wrong type is rejected with InvalidValue and the run is aborted, not committed', async () => {
      const before = await rx.state('sequenceTotal').get(); // must survive the failed run
      const w = trackSink(rx.sink('sequence', { delayMs: 0 }));
      const closed = w.closed;
      w.next(0);
      w.next(1); // delivered to the handler before the bad value...
      w.next('not a number' as unknown as number);
      await expectReject(closed, 'InvalidValue');
      await eventually('sink released', async () => (await activeSinks()) === 0);
      await sleep(100);
      // ...but a committed (completed) run would have set the total to 2.
      assertEq(await rx.state('sequenceTotal').get(), before, 'sequenceTotal after an invalid value');
    }],

    ['sink: an RxJS observable piped into a sink', async () => {
      const w = trackSink(rx.sink('sequence', { delayMs: 0 }));
      range(0, 40).subscribe(w);
      await withTimeout(w.closed, 5000, 'sink closed');
      assertEq(await rx.state('sequenceTotal').get(), 40, 'values seen by the handler');
    }],

    ['sink: a producer error aborts the Rust handler (it never reaches its commit)', async () => {
      const before = await rx.state('sequenceTotal').get();
      const w = trackSink(rx.sink('sequence', { delayMs: 0 }));
      const closed = w.closed;
      // Only fail once the sink is open on the backend, so "released" below proves a release.
      await eventually('sink open', async () => (await activeSinks()) === 1);
      concat(range(0, 5), throwError(() => new Error('producer failed'))).subscribe(w);
      await expectReject(closed, 'Aborted');
      await eventually('sink released', async () => (await activeSinks()) === 0);
      await sleep(100);
      // A completed run would have set the total to 5.
      assertEq(await rx.state('sequenceTotal').get(), before, 'sequenceTotal after an aborted run');
    }],

    ['sink: wrong kinds are rejected', async () => {
      const r = await collect(rx.stream('pointer' as any));
      assertEq(r.error?.code, 'NotSubscribable', 'subscribing to a sink');
      await expectReject((rx as any).sink('clicks').complete(), 'NotASink');
    }],

    probeCheck(probe),

    ['restrictive capability (child window): listed sources work, others are Forbidden; destroying the window releases everything', async () => {
      // The child capability is the README's "allow only specific sources" style (no reactive:default).
      const report = new Promise<Record<string, unknown>>((resolve, reject) => {
        setTimeout(() => reject(new Error('no report from the child window')), 10_000);
        void listen<Record<string, unknown>>('child-report', (e) => resolve(e.payload)).then((u) => track(u));
      });
      const child = new WebviewWindow('child-selftest', { url: 'index.html?child', visible: false });
      track(() => void child.destroy().catch(() => {})); // even if an assertion below fails
      await new Promise<void>((resolve, reject) => {
        setTimeout(() => reject(new Error('timed out creating the child window')), 10_000);
        child.once('tauri://created', () => resolve());
        child.once('tauri://error', (e) => reject(new Error(`window creation failed: ${JSON.stringify(e.payload)}`)));
      });
      assertEq(
        await report,
        { unsubscribe: true, sink: 'completed', abort: 'aborted', allowedGet: 'Welcome!', forbiddenGet: 'Forbidden', forbiddenSubscribe: 'Forbidden' },
        'child report',
      );
      await eventually('child subscribed and opened a sink', async () => (await active()) === 1 && (await activeSinks()) === 1, 10_000);
      await child.destroy();
      await eventually('released after window destroyed', async () => (await active()) === 0 && (await activeSinks()) === 0);
    }],
  ];
}

// ---------------------------------------------------------------------------------------------
// Phase 3: the real demo UI, driven through the DOM like a user would. Framework-agnostic: every
// example renders the same sections, headings, buttons and classes.
// ---------------------------------------------------------------------------------------------

function section(title: string): HTMLElement {
  const s = [...document.querySelectorAll('section')].find((el) => el.querySelector('h2')?.textContent?.trim().startsWith(title));
  if (!s) throw new Error(`section "${title}" not rendered`);
  return s;
}

function findButton(root: ParentNode, label: string) {
  return [...root.querySelectorAll('button')].find((b) => b.textContent?.trim() === label);
}

function button(root: ParentNode, label: string): HTMLButtonElement {
  const b = findButton(root, label);
  if (!b) throw new Error(`button "${label}" not found`);
  return b;
}

function num(el: Element | null | undefined): number | undefined {
  const m = /-?\d+(\.\d+)?/.exec(el?.textContent ?? '');
  return m ? Number(m[0]) : undefined;
}

/** Sets a form control's value the way a user edit would, so React's value tracking sees it too. */
function setValue(el: HTMLInputElement | HTMLSelectElement, value: string, event: 'input' | 'change') {
  const proto = el instanceof HTMLSelectElement ? HTMLSelectElement.prototype : HTMLInputElement.prototype;
  Object.getOwnPropertyDescriptor(proto, 'value')!.set!.call(el, value);
  el.dispatchEvent(new Event(event, { bubbles: true }));
}

const badgeCount = () => num(document.querySelector('.badge'));

function uiChecks(framework: string): Check[] {
  const ui = (name: string, fn: () => Promise<void>): Check => [`${framework} app UI: ${name}`, fn];
  return [
    ui('mounts, and the header badge matches the backend subscription count', async () => {
      await eventually('badge > 0', () => (badgeCount() ?? 0) > 0, 5000);
      await eventually('badge == backend count', async () => badgeCount() === (await active()));
    }),

    ui('Click → Rust updates read-only state → re-render', async () => {
      const s = section('Read-only state');
      // Wait for Rust's value, not the placeholder `0` the apps render before it arrives.
      const rust = await rx.state('clicks').get();
      await eventually(`clicks rendered as ${rust}`, () => num(s.querySelector('.big')) === rust);
      const before = rust;
      button(s, 'Click').click();
      await eventually('clicks incremented', () => num(s.querySelector('.big')) === before + 1);
    }),

    ui('Rust background task drives uptime', async () => {
      const s = section('Rust-driven state');
      await eventually('uptime rendered', () => num(s.querySelector('strong')) !== undefined);
      const first = num(s.querySelector('strong'))!;
      await eventually('uptime advanced', () => (num(s.querySelector('strong')) ?? -1) > first);
    }),

    ui('settings slider + select round-trip through Rust; invalid value shows an error', async () => {
      const s = section('Writable state');
      await eventually('settings loaded', () => !!s.querySelector('input[type=range]'));
      setValue(s.querySelector('input[type=range]')!, '42', 'input');
      await eventually('volume 42 in Rust and on screen', async () => (await rx.state('settings').get()).volume === 42 && s.textContent!.includes('42'));
      setValue(s.querySelector('select')!, 'light', 'change');
      await eventually('theme light in Rust', async () => (await rx.state('settings').get()).theme === 'light');
      // The select must first show Rust's `light` (a controlled React select is reset to the old
      // value until the update comes back), so the next step proves it follows Rust.
      await eventually('select shows light from Rust', () => s.querySelector('select')!.value === 'light');
      // And the other direction: a change made in Rust must re-render the select.
      await rx.state('settings').set({ theme: 'dark', volume: 42 });
      await eventually('select follows Rust back to dark', () => s.querySelector('select')!.value === 'dark');
      button(s, 'Set volume 150 (rejected)').click();
      await eventually('InvalidValue error shown', () => !!s.querySelector('.error')?.textContent?.includes('InvalidValue'));
      assertEq((await rx.state('settings').get()).volume, 42, 'volume after rejected set');
    }),

    // The checks below compare absolute backend subscription counts. The "Stream errors" panel's
    // subscription ends by itself (~2.4s after mount, and again after Retry), so wait for it to
    // end first, and keep the Retry check (which restarts it) after them.
    ui('stream error panel settles (its subscription ends on its own)', async () => {
      const s = section('Stream errors');
      await eventually('StreamError shown', () => !!s.querySelector('.error')?.textContent?.includes('StreamError'), 5000);
    }),

    ui('tick components share one subscription; Stop/Start and interval changes manage it', async () => {
      const s = section('Stream with args');
      const ticks = () =>
        [...s.querySelectorAll('span')].filter((x) => x.textContent?.trim().startsWith('tick')).map((x) => num(x.querySelector('strong')));
      await eventually('two components ticking', () => {
        const t = ticks();
        return t.length === 2 && t.every((n) => n !== undefined && n >= 1);
      });
      const running = await active();

      button(s, 'Stop').click();
      await eventually('Stop releases exactly one (shared) subscription', async () => (await active()) === running - 1);
      assertEq(ticks().length, 0, 'tick components after Stop');

      button(s, 'Start').click();
      await eventually('Start re-opens one subscription', async () => (await active()) === running);
      await eventually('ticking again', () => ticks().length === 2 && ticks().every((n) => n !== undefined));

      // Values must actually advance after a switch (the first value shown may come from a source
      // that was already connected, so seeing a number alone proves nothing).
      // Values may first jump to the new source's current value (or a placeholder), so require two
      // changes: the switch itself, then at least one real tick.
      const advancing = async (what: string) => {
        let last = JSON.stringify(ticks());
        let changes = 0;
        await eventually(
          `${what}: values updating`,
          () => {
            const now = JSON.stringify(ticks());
            if (now !== last) {
              changes++;
              last = now;
            }
            return changes >= 2 && ticks().length === 2 && ticks().every((n) => n !== undefined);
          },
          3000,
        );
      };

      // Unique args: the old source is released and a new one opened, so the count is unchanged.
      setValue(s.querySelector('select')!, '250', 'change');
      await advancing('ticking at 250ms');
      await eventuallyStable('subscriptions settle back to the same count (old released, new opened)', async () => (await active()) === running);

      // 100ms are the RxJS panel's args: the tick components join its existing subscription.
      setValue(s.querySelector('select')!, '100', 'change');
      await eventually('shared with the RxJS panel (one fewer subscription)', async () => (await active()) === running - 1);
      await advancing('ticking at 100ms');
    }),

    ui('child window holds a subscription until closed', async () => {
      const s = section('Window cleanup');
      const before = await active();
      // The child first runs its capability probes (including a brief subscribe/unsubscribe), then
      // reports, then holds its subscription. Wait for the report, then for a *stable* +1.
      const reported = withTimeout(
        new Promise<void>((resolve) => void listen('child-report', () => resolve()).then((u) => track(u))),
        10_000,
        'child window report',
      );
      button(s, 'Open child window').click();
      await reported;
      await eventuallyStable('child window holds its subscription', async () => (await active()) === before + 1);
      await eventually('Close button shown', () => !!findButton(s, 'Close child window'));
      button(s, 'Close child window').click();
      await eventually('released after close', async () => (await active()) === before);
      await eventually('Open button back', () => !!findButton(s, 'Open child window'));
    }),

    ui('RxJS pipeline renders averages', async () => {
      const s = section('RxJS interop');
      await eventually('average rendered', () => num(s.querySelector('strong')) !== undefined);
    }),

    ui('stream error is shown; Retry resubscribes', async () => {
      const s = section('Stream errors');
      await eventually('StreamError shown', () => !!s.querySelector('.error')?.textContent?.includes('StreamError'), 5000);
      if (!s.textContent?.includes('0, 1, 2, 3, 4')) throw new Error(`readings before error: "${s.textContent}"`);
      const retry = button(s, 'Retry');
      assertEq(retry.disabled, false, 'Retry enabled after error');
      retry.click();
      await eventually('error cleared and Retry disabled', () => !s.querySelector('.error') && button(s, 'Retry').disabled);
      // Proof of an actual resubscription: readings start over from 0.
      await eventually('readings restarted', () => /Readings:\s*0(?!\d)/.test(s.textContent ?? ''), 3000);
    }),

    ui('pointer sink: Send burst → Rust aggregates → stats re-render', async () => {
      const s = section('Frontend → Rust stream');
      await eventually('rendered stats match Rust', async () => num(s.querySelector('.big')) === (await rx.state('pointerStats').get()).points);
      await eventually('the panel holds one open sink', async () => (await activeSinks()) === 1);
      const before = num(s.querySelector('.big'))!;
      button(s, 'Send burst').click();
      await eventually('100 more points aggregated by Rust', () => (num(s.querySelector('.big')) ?? 0) >= before + 100); // >=: a real cursor over the pad adds points too
      const pad = s.querySelector('.pad')!;
      for (const [x, y] of [[10, 10], [40, 50]]) {
        pad.dispatchEvent(new PointerEvent('pointermove', { bubbles: true, clientX: pad.getBoundingClientRect().left + x, clientY: pad.getBoundingClientRect().top + y }));
      }
      await eventually('pointer moves aggregated', () => (num(s.querySelector('.big')) ?? 0) >= before + 102);
    }),

    ui('badge still matches the backend after all of that', async () => {
      await eventually('badge == backend count', async () => badgeCount() === (await active()));
    }),
  ];
}

async function run(checks: Check[], results: Result[]) {
  for (const [name, fn] of checks) {
    await runOne(name, fn, results);
    for (const cleanup of cleanups.splice(0)) {
      try {
        cleanup();
      } catch {
        // best effort
      }
    }
  }
}

async function runOne(name: string, fn: () => Promise<void>, results: Result[]) {
  try {
    await fn();
    results.push({ name, ok: true });
  } catch (e) {
    results.push({ name, ok: false, detail: e instanceof Error ? e.message : String(e) });
  }
}

const errorCode = (e: unknown) => (e && typeof e === 'object' && 'code' in e ? String(e.code) : String(e));

/**
 * A child window runs under the restrictive capability (capabilities/child.json): subscribe only
 * to `ticks`, get only `motd`, sink only `pointer`, and unsubscribe/sink-push/close/abort granted
 * without scopes of their own. It exercises each and reports back, then holds one subscription and
 * one sink until the window is destroyed.
 */
async function runChild() {
  const report: Record<string, unknown> = {};
  try {
    const channel = new Channel<unknown>();
    const id = await invoke<number>('plugin:reactive|subscribe', { source: 'ticks', args: { intervalMs: 1000 }, onEvent: channel });
    report.unsubscribe = await invoke('plugin:reactive|unsubscribe', { id });
  } catch (e) {
    report.unsubscribe = errorCode(e);
  }
  try {
    const w = rx.sink('pointer');
    w.next({ x: 0, y: 0 });
    await w.complete();
    report.sink = 'completed';
  } catch (e) {
    report.sink = errorCode(e);
  }
  try {
    // Raw invoke: the client swallows sink_abort errors, but this must actually be permitted.
    const id = await invoke<number>('plugin:reactive|sink_open', { source: 'pointer', args: null });
    await invoke('plugin:reactive|sink_abort', { id });
    report.abort = 'aborted';
  } catch (e) {
    report.abort = errorCode(e);
  }
  report.allowedGet = await rx.state('motd').get().catch(errorCode);
  report.forbiddenGet = await rx.state('clicks').get().then(() => 'allowed', errorCode);
  report.forbiddenSubscribe = await new Promise((resolve) => {
    rx.state('uptime').subscribe({ next: () => resolve('allowed'), error: (e) => resolve(e.code) });
  });

  rx.stream('ticks', { intervalMs: 50 }).subscribe((n) => {
    document.body.textContent = `child window tick ${n}`;
  });
  rx.sink('pointer').next({ x: 0, y: 0 }); // held open until the window is destroyed
  await emit('child-report', report);
}

async function report(results: Result[]) {
  const passed = results.every((r) => r.ok);
  const lines = results.map((r) => `${r.ok ? 'ok  ' : 'FAIL'}  ${r.name}${r.detail ? `\n        ${r.detail}` : ''}`);
  const summary = `${results.filter((r) => r.ok).length}/${results.length} passed`;
  await invoke('self_test_report', { passed, report: [...lines, summary].join('\n') });
}

async function runSelfTest(probe: FrameworkProbe, mountApp: () => void) {
  const status = document.createElement('p');
  status.textContent = `running ${probe.name} self-test…`;
  document.body.append(status);
  const watchdog = setTimeout(
    () => report([{ name: 'watchdog', ok: false, detail: 'self-test did not finish within 90s' }]),
    90_000,
  );

  const stored = sessionStorage.getItem(SELF_TEST_RELOAD_KEY);
  if (!stored) {
    const results: Result[] = [];
    await run(phase1(probe), results);

    // Hold two live subscriptions, then reload. The new page must find them released.
    rx.stream('ticks', { intervalMs: 30 }).subscribe(() => {});
    rx.state('uptime').subscribe(() => {});
    rx.sink('pointer').next({ x: 1, y: 1 });
    try {
      await eventually('2 subscriptions + 1 sink before reload', async () => (await active()) === 2 && (await activeSinks()) === 1);
    } catch (e) {
      results.push({ name: 'set up subscriptions before reload', ok: false, detail: String(e) });
    }
    sessionStorage.setItem(SELF_TEST_RELOAD_KEY, JSON.stringify(results));
    location.reload();
    return;
  }

  sessionStorage.removeItem(SELF_TEST_RELOAD_KEY);
  const results: Result[] = JSON.parse(stored);
  await run(
    [['page reload releases the old page’s subscriptions and sinks', async () => {
      await eventually('0 active after reload', async () => (await active()) === 0 && (await activeSinks()) === 0, 2000);
    }]],
    results,
  );

  // Phase 3: mount the real app and drive it.
  status.remove();
  try {
    mountApp();
    await run(uiChecks(probe.name.split(' ')[0]), results);
  } catch (e) {
    results.push({ name: 'mount app UI', ok: false, detail: e instanceof Error ? e.message : String(e) });
  }
  clearTimeout(watchdog);
  await report(results);
}

/**
 * Entry point for every example: a child window runs its capability checks and holds a
 * subscription and a sink, self-test mode runs the checks (with no app UI mounted at first, so
 * backend subscription counts are deterministic), and otherwise the app is mounted.
 */
export async function startExample(options: { mountApp: () => void; probe: FrameworkProbe }) {
  if (new URLSearchParams(location.search).has('child')) {
    await runChild();
  } else if (sessionStorage.getItem(SELF_TEST_RELOAD_KEY) || (await invoke<boolean>('is_self_test'))) {
    await runSelfTest(options.probe, options.mountApp);
  } else {
    options.mountApp();
  }
}
