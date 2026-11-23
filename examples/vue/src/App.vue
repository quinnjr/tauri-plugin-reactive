<script setup lang="ts">
import { onScopeDispose, ref, shallowRef, watchEffect } from 'vue';
import { invoke } from '@tauri-apps/api/core';
import { WebviewWindow } from '@tauri-apps/api/webviewWindow';
import { from, bufferCount, map } from 'rxjs';
import type { ReactiveError } from 'tauri-plugin-reactive-api';
import { useReactive } from 'tauri-plugin-reactive-api/vue';
import { rx, type Settings } from '@reactive-examples/shared/reactive';
import TickValue from './TickValue.vue';

const liveSubscriptions = useReactive(rx.state('liveSubscriptions'), 0);
const clicks = useReactive(rx.state('clicks'), 0);
const uptime = useReactive(rx.state('uptime'));

const settingsSource = rx.state('settings');
const settings = useReactive(settingsSource);
const settingsError = ref<string>();
function updateSettings(patch: Partial<Settings>) {
  if (!settings.value) return;
  settingsSource.set({ ...settings.value, ...patch }).then(
    () => (settingsError.value = undefined),
    (e: ReactiveError) => (settingsError.value = `${e.code}: ${e.message}`),
  );
}

const running = ref(true);
const intervalMs = ref(500);

const avg = ref<number>();
const avgSub = from(rx.stream('ticks', { intervalMs: 100 }))
  .pipe(
    bufferCount(5),
    map((xs) => xs.reduce((a, b) => a + b, 0) / xs.length),
  )
  .subscribe((v) => (avg.value = v));
onScopeDispose(() => avgSub.unsubscribe());

const attempt = ref(0);
const readings = ref<number[]>([]);
const flakyError = ref<string>();
watchEffect((onCleanup) => {
  void attempt.value; // re-run on retry
  readings.value = [];
  flakyError.value = undefined;
  onCleanup(
    rx.stream('flaky', { intervalMs: 400, failAfter: 5 }).subscribe({
      next: (v) => readings.value.push(v),
      error: (e) => (flakyError.value = `${e.code}: ${e.message}`),
    }),
  );
});

// Frontend -> Rust: the writer lives as long as this component.
const pointerStats = useReactive(rx.state('pointerStats'));
const pointer = rx.sink('pointer');
onScopeDispose(() => void pointer.complete());
function pushPoint(e: PointerEvent) {
  const r = (e.currentTarget as HTMLElement).getBoundingClientRect();
  pointer.next({ x: e.clientX - r.left, y: e.clientY - r.top });
}
function burst() {
  for (let i = 0; i < 100; i++) pointer.next({ x: i * 3, y: 50 + 40 * Math.sin(i / 8) });
}

const child = shallowRef<WebviewWindow>();
function openChild() {
  const w = new WebviewWindow(`child-${Date.now()}`, { url: 'index.html?child', title: 'child', width: 320, height: 120 });
  w.once('tauri://destroyed', () => (child.value = undefined));
  child.value = w;
}
</script>

<template>
  <main>
    <header>
      <h1>tauri-plugin-reactive · Vue</h1>
      <span class="badge" title="Backend subscriptions currently open (includes this badge)">
        {{ liveSubscriptions }} backend subscription{{ liveSubscriptions === 1 ? '' : 's' }}
      </span>
    </header>

    <section>
      <h2>Read-only state</h2>
      <p>Rust owns <code>clicks</code>; the button calls a command that does <code>clicks.update(|n| *n += 1)</code>.</p>
      <div class="row">
        <button @click="invoke('click')">Click</button>
        <strong class="big">{{ clicks }}</strong>
      </div>
    </section>

    <section>
      <h2>Rust-driven state</h2>
      <p>Updated by a background task every second: <strong>{{ uptime ?? '…' }}s</strong></p>
    </section>

    <section>
      <h2>Writable state with validation</h2>
      <div v-if="settings" class="row">
        <label>
          Theme
          <select
            :value="settings.theme"
            @change="updateSettings({ theme: ($event.target as HTMLSelectElement).value as Settings['theme'] })"
          >
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
            :value="settings.volume"
            @input="updateSettings({ volume: Number(($event.target as HTMLInputElement).value) })"
          />
          {{ settings.volume }}
        </label>
        <button @click="updateSettings({ volume: 150 })">Set volume 150 (rejected)</button>
      </div>
      <p v-else>loading…</p>
      <p v-if="settingsError" class="error">{{ settingsError }}</p>
    </section>

    <section>
      <h2>Stream with args</h2>
      <p>
        Mounting subscribes, unmounting unsubscribes. Changing the interval re-subscribes in place (getter form of
        <code>useReactive</code>). Watch the badge in the header.
      </p>
      <div class="row">
        <button @click="running = !running">{{ running ? 'Stop' : 'Start' }}</button>
        <label>
          interval
          <select v-model.number="intervalMs">
            <option v-for="ms in [100, 250, 500, 1000]" :key="ms" :value="ms">{{ ms }}ms</option>
          </select>
        </label>
        <template v-if="running">
          <TickValue :interval-ms="intervalMs" />
          <TickValue :interval-ms="intervalMs" label="(shared)" />
        </template>
      </div>
    </section>

    <section>
      <h2>RxJS interop</h2>
      <p>
        <code>from(rx.stream('ticks', ...)).pipe(bufferCount(5), map(avg))</code>: <strong>{{ avg ?? '…' }}</strong>
      </p>
    </section>

    <section>
      <h2>Stream errors</h2>
      <p>
        Readings: {{ readings.join(', ') || '…' }}
        <span v-if="flakyError" class="error">{{ flakyError }}</span>
      </p>
      <button :disabled="!flakyError" @click="attempt++">Retry</button>
    </section>

    <section>
      <h2>Frontend → Rust stream</h2>
      <p>
        Pointer moves over the pad are pushed into a Rust sink (<code>rx.sink('pointer')</code>), which aggregates
        them into the <code>pointerStats</code> state.
      </p>
      <div class="pad" @pointermove="pushPoint"></div>
      <div class="row">
        <button @click="burst">Send burst</button>
        <span>points <strong class="big">{{ pointerStats?.points ?? 0 }}</strong></span>
        <span>distance <strong>{{ Math.round(pointerStats?.distance ?? 0) }}</strong> px</span>
      </div>
    </section>

    <section>
      <h2>Window cleanup</h2>
      <p>The child window subscribes to a stream. Closing it releases the backend subscription.</p>
      <button v-if="child" @click="child.destroy()">Close child window</button>
      <button v-else @click="openChild">Open child window</button>
    </section>
  </main>
</template>
