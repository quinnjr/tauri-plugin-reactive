<script lang="ts">
  import { rx } from '@reactive-examples/shared/reactive';

  let { intervalMs, label = '' }: { intervalMs: number; label?: string } = $props();

  // Re-derived when the prop changes; Svelte re-subscribes `$tick` to the new source.
  // Equal args resolve to the same shared source, so two of these share one backend subscription.
  const tick = $derived(rx.stream('ticks', { intervalMs }));
</script>

<span>tick <strong>{$tick ?? '…'}</strong> {label}</span>
