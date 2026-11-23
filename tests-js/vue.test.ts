import { describe, expect, it, vi } from 'vitest';
import { effectScope, nextTick, reactive, ref } from 'vue';

const subs: Array<{ args: unknown; channel: { onmessage: (m: unknown) => void }; closed: boolean; id: number }> = [];
vi.mock('@tauri-apps/api/core', () => {
  class Channel {
    onmessage: (m: unknown) => void = () => {};
  }
  let next = 1;
  const invoke = vi.fn(async (cmd: string, args: any) => {
    if (cmd === 'plugin:reactive|subscribe') {
      const sub = { id: next++, args: args.args, channel: args.onEvent, closed: false };
      subs.push(sub);
      return sub.id;
    }
    if (cmd === 'plugin:reactive|unsubscribe') {
      subs.find((s) => s.id === args.id)!.closed = true;
    }
  });
  return { Channel, invoke };
});

import { createReactive, type State, type Stream } from '../guest-js/index';
import { useReactive } from '../guest-js/vue';

type Sources = { count: State<number>; ticks: Stream<number, { ms: number }> };
const flush = () => new Promise((r) => setTimeout(r, 0));
const live = () => subs.filter((s) => !s.closed);

describe('vue useReactive', () => {
  it('works with a Source wrapped in ref() or stored in reactive() state (no proxy errors)', async () => {
    const rx = createReactive<Sources>();
    const scope = effectScope();
    const state = reactive({ source: rx.state('count') });
    const [a, b] = scope.run(() => [useReactive(ref(rx.state('count')), 0), useReactive(() => state.source, 0)])!;
    live().at(-1)!.channel.onmessage({ type: 'next', value: 3 });
    expect(a.value).toBe(3);
    expect(b.value).toBe(3);
    expect(state.source).toBe(rx.state('count')); // not proxied
    scope.stop();
    await flush();
  });

  it('tracks values and re-subscribes when getter args change', async () => {
    const rx = createReactive<Sources>();
    const scope = effectScope();
    const ms = ref(10);
    const [count, tick] = scope.run(() => [
      useReactive(rx.state('count'), 0),
      useReactive(() => rx.stream('ticks', { ms: ms.value })),
    ])!;

    expect(count.value).toBe(0);
    expect(live().map((s) => s.args)).toEqual([null, { ms: 10 }]);
    live()[0].channel.onmessage({ type: 'next', value: 5 });
    live()[1].channel.onmessage({ type: 'next', value: 1 });
    expect(count.value).toBe(5);
    expect(tick.value).toBe(1);

    ms.value = 20;
    await nextTick();
    await flush();
    expect(live().map((s) => s.args)).toEqual([null, { ms: 20 }]);

    scope.stop();
    await flush();
    expect(live()).toHaveLength(0);
  });
});
