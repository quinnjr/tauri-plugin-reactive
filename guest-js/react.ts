import { useSyncExternalStore } from 'react';
import type { ReactiveError, Source, SourceSnapshot } from './index.js';

/**
 * Subscribes a component to a reactive source and re-renders on every value (and on errors).
 *
 * ```tsx
 * const count = useReactive(rx.state('counter'), 0);
 * const tick = useReactive(rx.stream('ticks', 500));
 * ```
 *
 * Passing a fresh `rx.stream(name, { ...args })` each render is fine: equal args resolve to the
 * same shared source. To show errors, use `useReactiveState`.
 */
export function useReactive<T>(source: Source<T>): T | undefined;
export function useReactive<T>(source: Source<T>, initial: T): T;
export function useReactive<T>(source: Source<T>, initial?: T): T | undefined {
  const { value } = useReactiveState(source);
  return value === undefined ? initial : value;
}

/** Like `useReactive`, but returns `{ value, error }`, re-rendering when the source errors. */
export function useReactiveState<T>(source: Source<T>): { value: T | undefined; error: ReactiveError | undefined } {
  const state: SourceSnapshot<T> = useSyncExternalStore(source.listen, source.snapshotState, source.snapshotState);
  return state;
}
