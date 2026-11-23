import { flushSync, mount, unmount } from 'svelte';
import { startExample } from '@reactive-examples/shared/selftest';
import '@reactive-examples/shared/styles.css';
import App from './App.svelte';
import Probe from './Probe.svelte';

await startExample({
  mountApp: () => mount(App, { target: document.getElementById('app')! }),
  probe: {
    name: 'Svelte $store',
    async mount(el) {
      const component = mount(Probe, { target: el });
      flushSync();
      return () => unmount(component);
    },
  },
});
