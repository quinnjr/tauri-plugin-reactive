<script lang="ts">
  import { invoke } from '@tauri-apps/api/core';
  import { WebviewWindow } from '@tauri-apps/api/webviewWindow';
  import { from, bufferCount, map } from 'rxjs';
  import type { ReactiveError, SinkWriter } from 'tauri-plugin-reactive-api';
  import { rx, type Point, type Settings } from '@reactive-examples/shared/reactive';
  import TickValue from './TickValue.svelte';

  // Sources are Svelte stores: `$name` subscribes for the component's lifetime. No adapter needed.
  const liveSubscriptions = rx.state('liveSubscriptions');
  const clicks = rx.state('clicks');
  const uptime = rx.state('uptime');
  const settings = rx.state('settings');

  let settingsError = $state<string>();
  function updateSettings(patch: Partial<Settings>) {
    if (!$settings) return;
    settings.set({ ...$settings, ...patch }).then(
      () => (settingsError = undefined),
      (e: ReactiveError) => (settingsError = `${e.code}: ${e.message}`),
    );
  }

  let running = $state(true);
  let intervalMs = $state(500);

  let avg = $state<number>();
  $effect(() => {
    const sub = from(rx.stream('ticks', { intervalMs: 100 }))
      .pipe(
        bufferCount(5),
        map((xs) => xs.reduce((a, b) => a + b, 0) / xs.length),
      )
      .subscribe((v) => (avg = v));
    return () => sub.unsubscribe();
  });

  let attempt = $state(0);
  let readings = $state<number[]>([]);
  let flakyError = $state<string>();
  $effect(() => {
    void attempt; // re-run on retry
    readings = [];
    flakyError = undefined;
    return rx.stream('flaky', { intervalMs: 400, failAfter: 5 }).subscribe({
      next: (v) => (readings = [...readings, v]),
      error: (e) => (flakyError = `${e.code}: ${e.message}`),
    });
  });

  const pointerStats = rx.state('pointerStats');
  let pointer: SinkWriter<Point> | undefined;
  $effect(() => {
    const w = rx.sink('pointer');
    pointer = w;
    return () => void w.complete();
  });
  function pushPoint(e: PointerEvent & { currentTarget: HTMLDivElement }) {
    const r = e.currentTarget.getBoundingClientRect();
    pointer?.next({ x: e.clientX - r.left, y: e.clientY - r.top });
  }
  function burst() {
    for (let i = 0; i < 100; i++) pointer?.next({ x: i * 3, y: 50 + 40 * Math.sin(i / 8) });
  }

  let child = $state<WebviewWindow>();
  function openChild() {
    const w = new WebviewWindow(`child-${Date.now()}`, { url: 'index.html?child', title: 'child', width: 320, height: 120 });
    w.once('tauri://destroyed', () => (child = undefined));
    child = w;
  }
</script>

<main>
  <header>
    <h1>tauri-plugin-reactive · Svelte</h1>
    <span class="badge" title="Backend subscriptions currently open (includes this badge)">
      {$liveSubscriptions ?? 0} backend subscription{$liveSubscriptions === 1 ? '' : 's'}
    </span>
  </header>

  <section>
    <h2>Read-only state</h2>
    <p>Rust owns <code>clicks</code>; the button calls a command that does <code>clicks.update(|n| *n += 1)</code>.</p>
    <div class="row">
      <button onclick={() => invoke('click')}>Click</button>
      <strong class="big">{$clicks ?? 0}</strong>
    </div>
  </section>

  <section>
    <h2>Rust-driven state</h2>
    <p>Updated by a background task every second: <strong>{$uptime ?? '…'}s</strong></p>
  </section>

  <section>
    <h2>Writable state with validation</h2>
    {#if $settings}
      <div class="row">
        <label>
          Theme
          <select value={$settings.theme} onchange={(e) => updateSettings({ theme: e.currentTarget.value as Settings['theme'] })}>
            <option value="dark">dark</option>
            <option value="light">light</option>
          </select>
        </label>
        <label>
          Volume
          <input
            type="range"
            min="0"
            max="100"
            value={$settings.volume}
            oninput={(e) => updateSettings({ volume: Number(e.currentTarget.value) })}
          />
          {$settings.volume}
        </label>
        <button onclick={() => updateSettings({ volume: 150 })}>Set volume 150 (rejected)</button>
      </div>
    {:else}
      <p>loading…</p>
    {/if}
    {#if settingsError}<p class="error">{settingsError}</p>{/if}
  </section>

  <section>
    <h2>Stream with args</h2>
    <p>Mounting subscribes, unmounting unsubscribes. Watch the badge in the header.</p>
    <div class="row">
      <button onclick={() => (running = !running)}>{running ? 'Stop' : 'Start'}</button>
      <label>
        interval
        <select bind:value={intervalMs}>
          {#each [100, 250, 500, 1000] as ms (ms)}
            <option value={ms}>{ms}ms</option>
          {/each}
        </select>
      </label>
      {#if running}
        <TickValue {intervalMs} />
        <TickValue {intervalMs} label="(shared)" />
      {/if}
    </div>
  </section>

  <section>
    <h2>RxJS interop</h2>
    <p><code>from(rx.stream('ticks', ...)).pipe(bufferCount(5), map(avg))</code>: <strong>{avg ?? '…'}</strong></p>
  </section>

  <section>
    <h2>Stream errors</h2>
    <p>
      Readings: {readings.join(', ') || '…'}
      {#if flakyError}<span class="error">{flakyError}</span>{/if}
    </p>
    <button onclick={() => attempt++} disabled={!flakyError}>Retry</button>
  </section>

  <section>
    <h2>Frontend → Rust stream</h2>
    <p>
      Pointer moves over the pad are pushed into a Rust sink (<code>rx.sink('pointer')</code>), which aggregates them
      into the <code>pointerStats</code> state.
    </p>
    <div class="pad" role="application" aria-label="pointer pad" onpointermove={pushPoint}></div>
    <div class="row">
      <button onclick={burst}>Send burst</button>
      <span>points <strong class="big">{$pointerStats?.points ?? 0}</strong></span>
      <span>distance <strong>{Math.round($pointerStats?.distance ?? 0)}</strong> px</span>
    </div>
  </section>

  <section>
    <h2>Window cleanup</h2>
    <p>The child window subscribes to a stream. Closing it releases the backend subscription.</p>
    {#if child}
      <button onclick={() => child?.destroy()}>Close child window</button>
    {:else}
      <button onclick={openChild}>Open child window</button>
    {/if}
  </section>
</main>
