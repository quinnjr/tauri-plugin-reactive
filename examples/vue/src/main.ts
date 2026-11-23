import { createApp, nextTick } from 'vue';
import { startExample } from '@reactive-examples/shared/selftest';
import '@reactive-examples/shared/styles.css';
import App from './App.vue';
import Probe from './Probe.vue';

await startExample({
  mountApp: () => createApp(App).mount('#app'),
  probe: {
    name: 'Vue useReactive',
    async mount(el) {
      const app = createApp(Probe);
      app.mount(el);
      await nextTick();
      return () => app.unmount();
    },
  },
});
