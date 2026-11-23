import { ChangeDetectionStrategy, Component } from '@angular/core';
import { reactiveSignal } from 'tauri-plugin-reactive-api/angular';
import { rx } from '@reactive-examples/shared/reactive';

/** Mounted by the self-test to check the Angular adapter end to end. */
@Component({
  selector: 'app-probe',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `<p>clicks:{{ clicks() }} ticks:{{ ticks() }} volume:{{ settings()?.volume }}</p>`,
})
export class ProbeComponent {
  readonly clicks = reactiveSignal(rx.state('clicks'));
  readonly ticks = reactiveSignal(rx.stream('ticks', { intervalMs: 40 }));
  readonly settings = reactiveSignal(rx.state('settings'));
}
