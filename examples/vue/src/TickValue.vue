<script setup lang="ts">
import { useReactive } from 'tauri-plugin-reactive-api/vue';
import { rx } from '@reactive-examples/shared/reactive';

const props = defineProps<{ intervalMs: number; label?: string }>();

// A getter re-subscribes when the prop changes, without remounting the component.
// Equal args resolve to the same shared source, so two of these share one backend subscription.
const tick = useReactive(() => rx.stream('ticks', { intervalMs: props.intervalMs }));
</script>

<template>
  <span>tick <strong>{{ tick ?? '…' }}</strong> {{ label }}</span>
</template>
