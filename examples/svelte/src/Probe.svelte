<!-- Mounted by the self-test to check Svelte's store contract end to end. -->
<script lang="ts">
  import { rx } from '@reactive-examples/shared/reactive';

  const clicks = rx.state('clicks');
  const ticks = rx.stream('ticks', { intervalMs: 40 });
  const settings = rx.state('settings');
</script>

<p>clicks:{$clicks} ticks:{$ticks} volume:{$settings?.volume}</p>
