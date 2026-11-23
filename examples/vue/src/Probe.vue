<!-- Mounted by the self-test to check the Vue adapter end to end. -->
<script setup lang="ts">
import { useReactive } from 'tauri-plugin-reactive-api/vue';
import { rx } from '@reactive-examples/shared/reactive';

const clicks = useReactive(rx.state('clicks'));
const ticks = useReactive(rx.stream('ticks', { intervalMs: 40 }));
const settings = useReactive(rx.state('settings'));
</script>

<template>
  <p>clicks:{{ clicks }} ticks:{{ ticks }} volume:{{ settings?.volume }}</p>
</template>
