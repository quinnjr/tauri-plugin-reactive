import { createComponent, provideZonelessChangeDetection } from '@angular/core';
import { bootstrapApplication, createApplication } from '@angular/platform-browser';
import { startExample } from '@reactive-examples/shared/selftest';
import { AppComponent } from './app/app.component';
import { ProbeComponent } from './app/probe.component';

// Zoneless: Channel callbacks run outside any zone, and signal writes schedule change detection.
const providers = [provideZonelessChangeDetection()];

startExample({
  mountApp: () => {
    bootstrapApplication(AppComponent, { providers }).catch((e) => console.error(e));
  },
  probe: {
    name: 'Angular reactiveSignal',
    async mount(el) {
      const app = await createApplication({ providers });
      const ref = createComponent(ProbeComponent, { environmentInjector: app.injector, hostElement: el });
      app.attachView(ref.hostView);
      ref.changeDetectorRef.detectChanges();
      return () => {
        ref.destroy();
        app.destroy();
      };
    },
  },
}).catch((e) => console.error(e));
