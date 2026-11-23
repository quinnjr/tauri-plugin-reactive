import { useEffect, useRef, useState } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { WebviewWindow } from '@tauri-apps/api/webviewWindow';
import { from, bufferCount, map } from 'rxjs';
import { useReactive } from 'tauri-plugin-reactive-api/react';
import type { ReactiveError, SinkWriter } from 'tauri-plugin-reactive-api';
import { rx, type Point, type Settings } from '@reactive-examples/shared/reactive';

export function App() {
  return (
    <main>
      <header>
        <h1>tauri-plugin-reactive</h1>
        <LiveSubscriptions />
      </header>
      <ClickCounter />
      <Uptime />
      <SettingsPanel />
      <Ticks />
      <RxjsAverage />
      <Flaky />
      <PointerPanel />
      <ChildWindow />
    </main>
  );
}

function LiveSubscriptions() {
  const n = useReactive(rx.state('liveSubscriptions'), 0);
  return (
    <span className="badge" title="Backend subscriptions currently open (includes this badge)">
      {n} backend subscription{n === 1 ? '' : 's'}
    </span>
  );
}

function ClickCounter() {
  const clicks = useReactive(rx.state('clicks'), 0);
  return (
    <section>
      <h2>Read-only state</h2>
      <p>
        Rust owns <code>clicks</code>; the button calls a command that does <code>clicks.update(|n| *n += 1)</code>.
      </p>
      <div className="row">
        <button onClick={() => invoke('click')}>Click</button>
        <strong className="big">{clicks}</strong>
      </div>
    </section>
  );
}

function Uptime() {
  const uptime = useReactive(rx.state('uptime'));
  return (
    <section>
      <h2>Rust-driven state</h2>
      <p>
        Updated by a background task every second: <strong>{uptime ?? '…'}s</strong>
      </p>
    </section>
  );
}

function SettingsPanel() {
  const source = rx.state('settings');
  const settings = useReactive(source);
  const [error, setError] = useState<string>();

  const update = (patch: Partial<Settings>) => {
    if (!settings) return;
    source.set({ ...settings, ...patch }).then(
      () => setError(undefined),
      (e: ReactiveError) => setError(`${e.code}: ${e.message}`),
    );
  };

  return (
    <section>
      <h2>Writable state with validation</h2>
      {settings ? (
        <div className="row">
          <label>
            Theme{' '}
            <select value={settings.theme} onChange={(e) => update({ theme: e.target.value as Settings['theme'] })}>
              <option value="dark">dark</option>
              <option value="light">light</option>
            </select>
          </label>
          <label>
            Volume{' '}
            <input
              type="range"
              min={0}
              max={100}
              value={settings.volume}
              onChange={(e) => update({ volume: Number(e.target.value) })}
            />{' '}
            {settings.volume}
          </label>
          <button onClick={() => update({ volume: 150 })}>Set volume 150 (rejected)</button>
        </div>
      ) : (
        <p>loading…</p>
      )}
      {error && <p className="error">{error}</p>}
    </section>
  );
}

function Ticks() {
  const [running, setRunning] = useState(true);
  const [intervalMs, setIntervalMs] = useState(500);
  return (
    <section>
      <h2>Stream with args</h2>
      <p>Mounting subscribes, unmounting unsubscribes. Watch the badge in the header.</p>
      <div className="row">
        <button onClick={() => setRunning((r) => !r)}>{running ? 'Stop' : 'Start'}</button>
        <label>
          interval{' '}
          <select value={intervalMs} onChange={(e) => setIntervalMs(Number(e.target.value))}>
            {[100, 250, 500, 1000].map((ms) => (
              <option key={ms} value={ms}>
                {ms}ms
              </option>
            ))}
          </select>
        </label>
        {running && <TickValue intervalMs={intervalMs} />}
        {running && <TickValue intervalMs={intervalMs} label="(shared)" />}
      </div>
    </section>
  );
}

function TickValue({ intervalMs, label }: { intervalMs: number; label?: string }) {
  // A fresh args object every render is fine: equal args resolve to the same shared source.
  const tick = useReactive(rx.stream('ticks', { intervalMs }));
  return (
    <span>
      tick <strong>{tick ?? '…'}</strong> {label}
    </span>
  );
}

function RxjsAverage() {
  const [avg, setAvg] = useState<number>();
  useEffect(() => {
    const sub = from(rx.stream('ticks', { intervalMs: 100 }))
      .pipe(
        bufferCount(5),
        map((xs) => xs.reduce((a, b) => a + b, 0) / xs.length),
      )
      .subscribe(setAvg);
    return () => sub.unsubscribe();
  }, []);
  return (
    <section>
      <h2>RxJS interop</h2>
      <p>
        <code>from(rx.stream('ticks', ...)).pipe(bufferCount(5), map(avg))</code>: <strong>{avg ?? '…'}</strong>
      </p>
    </section>
  );
}

function Flaky() {
  const [attempt, setAttempt] = useState(0);
  const [values, setValues] = useState<number[]>([]);
  const [error, setError] = useState<string>();
  useEffect(() => {
    setValues([]);
    setError(undefined);
    return rx.stream('flaky', { intervalMs: 400, failAfter: 5 }).subscribe({
      next: (v) => setValues((vs) => [...vs, v]),
      error: (e) => setError(`${e.code}: ${e.message}`),
    });
  }, [attempt]);
  return (
    <section>
      <h2>Stream errors</h2>
      <p>
        Readings: {values.join(', ') || '…'} {error && <span className="error">{error}</span>}
      </p>
      <button onClick={() => setAttempt((a) => a + 1)} disabled={!error}>
        Retry
      </button>
    </section>
  );
}

function PointerPanel() {
  const stats = useReactive(rx.state('pointerStats'));
  const writer = useRef<SinkWriter<Point>>(null);
  useEffect(() => {
    const w = rx.sink('pointer');
    writer.current = w;
    return () => void w.complete();
  }, []);
  const push = (e: React.PointerEvent<HTMLDivElement>) => {
    const r = e.currentTarget.getBoundingClientRect();
    writer.current?.next({ x: e.clientX - r.left, y: e.clientY - r.top });
  };
  const burst = () => {
    for (let i = 0; i < 100; i++) writer.current?.next({ x: i * 3, y: 50 + 40 * Math.sin(i / 8) });
  };
  return (
    <section>
      <h2>Frontend → Rust stream</h2>
      <p>
        Pointer moves over the pad are pushed into a Rust sink (<code>rx.sink('pointer')</code>), which aggregates them
        into the <code>pointerStats</code> state.
      </p>
      <div className="pad" onPointerMove={push} />
      <div className="row">
        <button onClick={burst}>Send burst</button>
        <span>
          points <strong className="big">{stats?.points ?? 0}</strong>
        </span>
        <span>
          distance <strong>{Math.round(stats?.distance ?? 0)}</strong> px
        </span>
      </div>
    </section>
  );
}

function ChildWindow() {
  const [win, setWin] = useState<WebviewWindow>();
  const open = () => {
    const w = new WebviewWindow(`child-${Date.now()}`, { url: 'index.html?child', title: 'child', width: 320, height: 120 });
    w.once('tauri://destroyed', () => setWin(undefined));
    setWin(w);
  };
  return (
    <section>
      <h2>Window cleanup</h2>
      <p>The child window subscribes to a stream. Closing it releases the backend subscription.</p>
      {win ? <button onClick={() => win.destroy()}>Close child window</button> : <button onClick={open}>Open child window</button>}
    </section>
  );
}
