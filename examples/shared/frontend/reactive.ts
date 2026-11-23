import { createReactive } from 'tauri-plugin-reactive-api';
import type { Sources } from './sources.gen';

// All source and value types are generated from the Rust registrations
// (examples/shared/backend/src/lib.rs → register()). Don't edit sources.gen.ts by hand.
export type { Point, PointerStats, Settings, Sources, Theme } from './sources.gen';

export const rx = createReactive<Sources>();
