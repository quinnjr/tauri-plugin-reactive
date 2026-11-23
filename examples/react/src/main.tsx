import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { flushSync } from 'react-dom';
import { useReactive } from 'tauri-plugin-reactive-api/react';
import { rx } from '@reactive-examples/shared/reactive';
import { startExample } from '@reactive-examples/shared/selftest';
import '@reactive-examples/shared/styles.css';
import { App } from './App';

/** Mounted by the self-test to check the React adapter end to end. */
function Probe() {
  const clicks = useReactive(rx.state('clicks'));
  const ticks = useReactive(rx.stream('ticks', { intervalMs: 40 }));
  const settings = useReactive(rx.state('settings'));
  return (
    <p>
      clicks:{clicks} ticks:{ticks} volume:{settings?.volume}
    </p>
  );
}

await startExample({
  mountApp: () =>
    createRoot(document.getElementById('root')!).render(
      <StrictMode>
        <App />
      </StrictMode>,
    ),
  probe: {
    name: 'React useReactive',
    async mount(el) {
      const root = createRoot(el);
      flushSync(() => root.render(<Probe />));
      return () => root.unmount();
    },
  },
});
