import { shallowRef, toRaw, toValue, watch, type MaybeRefOrGetter, type Ref } from 'vue';
import type { Source } from './index.js';

/**
 * The public shape of a Source. Accepting it (rather than `Source<T>`) lets callers pass a Source
 * that went through `ref()` / `reactive()`: Vue's `UnwrapRef` type drops `#private` members, though at
 * runtime Sources are never proxied (they are marked raw).
 */
type SourceLike<T> = Pick<Source<T>, 'subscribe' | 'value'>;

/**
 * Subscribes to a reactive source for the lifetime of the current component (or effect scope).
 *
 * ```ts
 * const clicks = useReactive(rx.state('clicks'), 0);
 * const interval = ref(500);
 * // Pass a getter to re-subscribe when args change:
 * const tick = useReactive(() => rx.stream('ticks', { intervalMs: interval.value }));
 * ```
 */
export function useReactive<T>(source: MaybeRefOrGetter<SourceLike<T>>): Readonly<Ref<T | undefined>>;
export function useReactive<T>(source: MaybeRefOrGetter<SourceLike<T>>, initial: T): Readonly<Ref<T>>;
export function useReactive<T>(source: MaybeRefOrGetter<SourceLike<T>>, initial?: T): Readonly<Ref<T | undefined>> {
  const value = shallowRef<T | undefined>(initial);
  watch(
    // Never call into a Source through a Vue proxy (its #private fields would throw).
    () => toRaw(toValue(source)) as Source<T>,
    (src, _prev, onCleanup) => {
      value.value = src.value === undefined ? initial : src.value;
      onCleanup(src.subscribe((v) => (value.value = v)));
    },
    { immediate: true },
  );
  return value;
}
