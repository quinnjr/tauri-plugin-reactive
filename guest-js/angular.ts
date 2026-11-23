import { DestroyRef, effect, inject, Injector, signal, untracked, type Signal } from '@angular/core';
import type { Source } from './index.js';

export interface ReactiveSignalOptions<T> {
  initial?: T;
  /** Needed when called outside an injection context (constructor / field initializer). */
  injector?: Injector;
}

/**
 * Exposes a reactive source as an Angular signal, unsubscribing when the owning
 * component/service is destroyed.
 *
 * ```ts
 * clicks = reactiveSignal(rx.state('clicks'), { initial: 0 });
 * interval = signal(500);
 * // Pass a function to re-subscribe when signals it reads change:
 * tick = reactiveSignal(() => rx.stream('ticks', { intervalMs: this.interval() }));
 * ```
 *
 * `toSignal(from(source))` from `@angular/core/rxjs-interop` also works, since sources are interop observables.
 */
export function reactiveSignal<T>(source: Source<T> | (() => Source<T>)): Signal<T | undefined>;
export function reactiveSignal<T>(
  source: Source<T> | (() => Source<T>),
  options: ReactiveSignalOptions<T> & { initial: T },
): Signal<T>;
export function reactiveSignal<T>(
  source: Source<T> | (() => Source<T>),
  options?: ReactiveSignalOptions<T>,
): Signal<T | undefined>;
export function reactiveSignal<T>(
  source: Source<T> | (() => Source<T>),
  options: ReactiveSignalOptions<T> = {},
): Signal<T | undefined> {
  const injector = options.injector ?? inject(Injector);
  const initial = options.initial;
  const value = signal<T | undefined>(initial);

  const attach = (src: Source<T>) => {
    value.set(src.value === undefined ? initial : src.value);
    return src.subscribe((v) => value.set(v));
  };

  if (typeof source === 'function') {
    effect(
      (onCleanup) => {
        const src = source();
        onCleanup(untracked(() => attach(src)));
      },
      { injector },
    );
  } else {
    injector.get(DestroyRef).onDestroy(attach(source));
  }
  return value.asReadonly();
}
