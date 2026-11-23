// `npm run lib` (from examples/): rebuild the library, reinstall the packed copy, and clear the
// dev servers' dependency caches. Vite (and Angular's dev server) pre-bundle dependencies and key
// that cache on the lockfile, which doesn't change when only the library's code does, so without
// clearing it `tauri dev` keeps serving the previous library build.
import { spawnSync } from 'node:child_process';
import { existsSync, readdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const examples = fileURLToPath(new URL('../..', import.meta.url));

function run(cmd, args, cwd) {
  const r = spawnSync(cmd, args, { cwd, stdio: 'inherit', shell: process.platform === 'win32' });
  if (r.status !== 0) process.exit(r.status ?? 1);
}

run('npm', ['run', 'build'], join(examples, '..'));
rmSync(join(examples, 'node_modules', 'tauri-plugin-reactive-api'), { recursive: true, force: true });
run('npm', ['install', '--no-audit', '--no-fund'], examples);

const caches = [join(examples, 'node_modules', '.vite')];
for (const dir of readdirSync(examples, { withFileTypes: true })) {
  if (!dir.isDirectory()) continue;
  caches.push(join(examples, dir.name, 'node_modules', '.vite'), join(examples, dir.name, '.angular', 'cache'));
}
for (const cache of caches.filter(existsSync)) {
  rmSync(cache, { recursive: true, force: true });
  console.log(`cleared ${cache}`);
}
