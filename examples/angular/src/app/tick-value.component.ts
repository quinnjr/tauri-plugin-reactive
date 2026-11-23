import { ChangeDetectionStrategy, Component, input } from '@angular/core';
import { reactiveSignal } from 'tauri-plugin-reactive-api/angular';
import { rx } from '@reactive-examples/shared/reactive';

@Component({
  selector: 'app-tick-value',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `<span>tick <strong>{{ tick() ?? '…' }}</strong> {{ label() }}</span>`,
})
export class TickValueComponent {
  readonly intervalMs = input.required<number>();
  readonly label = input('');

  // The function form re-subscribes when the input signal changes, without re-creating the component.
  // Equal args resolve to the same shared source, so two of these share one backend subscription.
  readonly tick = reactiveSignal(() => rx.stream('ticks', { intervalMs: this.intervalMs() }));
}
