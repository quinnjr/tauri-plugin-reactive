import { ChangeDetectionStrategy, Component, DestroyRef, effect, inject, signal, untracked } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';
import { invoke } from '@tauri-apps/api/core';
import { WebviewWindow } from '@tauri-apps/api/webviewWindow';
import { bufferCount, from, map } from 'rxjs';
import type { ReactiveError } from 'tauri-plugin-reactive-api';
import { reactiveSignal } from 'tauri-plugin-reactive-api/angular';
import { rx, type Settings } from '@reactive-examples/shared/reactive';
import { TickValueComponent } from './tick-value.component';

@Component({
  selector: 'app-root',
  imports: [TickValueComponent],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <main>
      <header>
        <h1>tauri-plugin-reactive · Angular</h1>
        <span class="badge" title="Backend subscriptions currently open (includes this badge)">
          {{ liveSubscriptions() }} backend subscription{{ liveSubscriptions() === 1 ? '' : 's' }}
        </span>
      </header>

      <section>
        <h2>Read-only state</h2>
        <p>Rust owns <code>clicks</code>; the button calls a command that does <code>clicks.update(|n| *n += 1)</code>.</p>
        <div class="row">
          <button (click)="click()">Click</button>
          <strong class="big">{{ clicks() }}</strong>
        </div>
      </section>

      <section>
        <h2>Rust-driven state</h2>
        <p>Updated by a background task every second: <strong>{{ uptime() ?? '…' }}s</strong></p>
      </section>

      <section>
        <h2>Writable state with validation</h2>
        @if (settings(); as s) {
          <div class="row">
            <label>
              Theme
              <select [value]="s.theme" (change)="updateSettings({ theme: $any($event.target).value })">
                <option value="dark">dark</option>
                <option value="light">light</option>
              </select>
            </label>
            <label>
              Volume
              <input type="range" min="0" max="100" [value]="s.volume" (input)="updateSettings({ volume: +$any($event.target).value })" />
              {{ s.volume }}
            </label>
            <button (click)="updateSettings({ volume: 150 })">Set volume 150 (rejected)</button>
          </div>
        } @else {
          <p>loading…</p>
        }
        @if (settingsError(); as err) {
          <p class="error">{{ err }}</p>
        }
      </section>

      <section>
        <h2>Stream with args</h2>
        <p>
          Mounting subscribes, unmounting unsubscribes. Changing the interval re-subscribes in place (function form of
          <code>reactiveSignal</code>). Watch the badge in the header.
        </p>
        <div class="row">
          <button (click)="running.set(!running())">{{ running() ? 'Stop' : 'Start' }}</button>
          <label>
            interval
            <select (change)="intervalMs.set(+$any($event.target).value)">
              @for (ms of intervals; track ms) {
                <option [value]="ms" [selected]="ms === intervalMs()">{{ ms }}ms</option>
              }
            </select>
          </label>
          @if (running()) {
            <app-tick-value [intervalMs]="intervalMs()" />
            <app-tick-value [intervalMs]="intervalMs()" label="(shared)" />
          }
        </div>
      </section>

      <section>
        <h2>RxJS interop</h2>
        <p>
          <code>toSignal(from(rx.stream('ticks', ...)).pipe(bufferCount(5), map(avg)))</code>:
          <strong>{{ avg() ?? '…' }}</strong>
        </p>
      </section>

      <section>
        <h2>Stream errors</h2>
        <p>
          Readings: {{ readings().join(', ') || '…' }}
          @if (flakyError(); as err) {
            <span class="error">{{ err }}</span>
          }
        </p>
        <button [disabled]="!flakyError()" (click)="attempt.set(attempt() + 1)">Retry</button>
      </section>

      <section>
        <h2>Frontend → Rust stream</h2>
        <p>
          Pointer moves over the pad are pushed into a Rust sink (<code>rx.sink('pointer')</code>), which aggregates
          them into the <code>pointerStats</code> state.
        </p>
        <div class="pad" (pointermove)="pushPoint($event)"></div>
        <div class="row">
          <button (click)="burst()">Send burst</button>
          <span>points <strong class="big">{{ pointerStats()?.points ?? 0 }}</strong></span>
          <span>distance <strong>{{ round(pointerStats()?.distance ?? 0) }}</strong> px</span>
        </div>
      </section>

      <section>
        <h2>Window cleanup</h2>
        <p>The child window subscribes to a stream. Closing it releases the backend subscription.</p>
        @if (child(); as w) {
          <button (click)="w.destroy()">Close child window</button>
        } @else {
          <button (click)="openChild()">Open child window</button>
        }
      </section>
    </main>
  `,
})
export class AppComponent {
  readonly liveSubscriptions = reactiveSignal(rx.state('liveSubscriptions'), { initial: 0 });
  readonly clicks = reactiveSignal(rx.state('clicks'), { initial: 0 });
  readonly uptime = reactiveSignal(rx.state('uptime'));

  private readonly settingsSource = rx.state('settings');
  readonly settings = reactiveSignal(this.settingsSource);
  readonly settingsError = signal<string | undefined>(undefined);

  readonly intervals = [100, 250, 500, 1000];
  readonly running = signal(true);
  readonly intervalMs = signal(500);

  // Sources are interop observables, so Angular's own toSignal() works too.
  readonly avg = toSignal(
    from(rx.stream('ticks', { intervalMs: 100 })).pipe(
      bufferCount(5),
      map((xs) => xs.reduce((a, b) => a + b, 0) / xs.length),
    ),
  );

  readonly attempt = signal(0);
  readonly readings = signal<number[]>([]);
  readonly flakyError = signal<string | undefined>(undefined);

  readonly child = signal<WebviewWindow | undefined>(undefined);

  // Frontend -> Rust: the writer lives as long as this component.
  readonly pointerStats = reactiveSignal(rx.state('pointerStats'));
  private readonly pointer = rx.sink('pointer');
  readonly round = Math.round;

  constructor() {
    inject(DestroyRef).onDestroy(() => void this.pointer.complete());

    effect((onCleanup) => {
      this.attempt(); // re-run on retry
      untracked(() => {
        this.readings.set([]);
        this.flakyError.set(undefined);
        onCleanup(
          rx.stream('flaky', { intervalMs: 400, failAfter: 5 }).subscribe({
            next: (v) => this.readings.update((r) => [...r, v]),
            error: (e) => this.flakyError.set(`${e.code}: ${e.message}`),
          }),
        );
      });
    });
  }

  pushPoint(e: PointerEvent) {
    const r = (e.currentTarget as HTMLElement).getBoundingClientRect();
    this.pointer.next({ x: e.clientX - r.left, y: e.clientY - r.top });
  }

  burst() {
    for (let i = 0; i < 100; i++) this.pointer.next({ x: i * 3, y: 50 + 40 * Math.sin(i / 8) });
  }

  click() {
    invoke('click');
  }

  updateSettings(patch: Partial<Settings>) {
    const current = this.settings();
    if (!current) return;
    this.settingsSource.set({ ...current, ...patch }).then(
      () => this.settingsError.set(undefined),
      (e: ReactiveError) => this.settingsError.set(`${e.code}: ${e.message}`),
    );
  }

  openChild() {
    const w = new WebviewWindow(`child-${Date.now()}`, { url: 'index.html?child', title: 'child', width: 320, height: 120 });
    w.once('tauri://destroyed', () => this.child.set(undefined));
    this.child.set(w);
  }
}
