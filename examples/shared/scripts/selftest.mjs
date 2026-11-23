// Usage (from an example dir): node ../shared/scripts/selftest.mjs <cargo package> [--dev]
//
// Default: builds the frontend, embeds it in the app (tauri/custom-protocol) and runs the self-test.
// --dev:   runs the self-test under `tauri dev` instead, against the framework's dev server.
// Exits 0 only if the app printed `SELF-TEST PASSED` (and, in --dev mode, wasn't restarted).
import { spawn, spawnSync } from 'node:child_process';
import { join } from 'node:path';

const [pkg, mode] = process.argv.slice(2);
if (!pkg) {
  console.error('usage: selftest.mjs <cargo package> [--dev]');
  process.exit(2);
}
const dev = mode === '--dev';
const exampleDir = process.cwd();
const srcTauri = join(exampleDir, 'src-tauri');
const windows = process.platform === 'win32';
const RUN_TIMEOUT_MS = 180_000;
let timedOut = false;

function runStep(cmd, args, opts = {}) {
  const r = spawnSync(cmd, args, { stdio: 'inherit', shell: windows, ...opts });
  if (r.error) throw r.error;
  if (r.status !== 0) process.exit(r.status ?? 1);
}

/** Runs the app, streaming its output, and resolves with everything it printed and its exit code. */
function runApp(cmd, args, opts) {
  return new Promise((resolve) => {
    // Off Windows, run in a new process group so a timeout can kill npx's whole tree (tauri CLI,
    // app, dev server); killing just npx would orphan them, and they'd keep our pipes open.
    const child = spawn(cmd, args, {
      ...opts,
      shell: windows,
      detached: !windows,
      env: { ...process.env, REACTIVE_SELF_TEST: '1' },
    });
    let out = '';
    const collect = (stream, target) =>
      stream.on('data', (chunk) => {
        out += chunk;
        target.write(chunk);
      });
    collect(child.stdout, process.stdout);
    collect(child.stderr, process.stderr);
    // Kill the whole tree (with shell: true on Windows, child.kill() would only end cmd.exe; off
    // Windows the child leads its own process group). Otherwise the app and the dev server on
    // port 1420 keep running and break the next run.
    const killTree = () => {
      if (windows) spawnSync('taskkill', ['/pid', String(child.pid), '/T', '/F'], { stdio: 'ignore' });
      else {
        try {
          process.kill(-child.pid, 'SIGKILL');
        } catch {
          child.kill('SIGKILL');
        }
      }
    };
    const timer = setTimeout(() => {
      timedOut = true;
      killTree();
    }, RUN_TIMEOUT_MS);
    // Ctrl+C / CI cancellation: the detached group doesn't receive the terminal's signal.
    const onSignal = (signal) => {
      killTree();
      process.exit(signal === 'SIGINT' ? 130 : 143);
    };
    process.once('SIGINT', onSignal);
    process.once('SIGTERM', onSignal);
    child.on('close', (code) => {
      clearTimeout(timer);
      process.off('SIGINT', onSignal);
      process.off('SIGTERM', onSignal);
      resolve({ out, code });
    });
  });
}

// Compile first, so the run timeout below never includes a cold build.
if (dev) {
  runStep('cargo', ['build', '-p', pkg], { cwd: srcTauri });
} else {
  runStep('npm', ['run', 'build'], { cwd: exampleDir });
  runStep('cargo', ['build', '-p', pkg, '--features', 'tauri/custom-protocol'], { cwd: srcTauri });
}

const { out, code } = dev
  ? await runApp('npx', ['tauri', 'dev'], { cwd: exampleDir })
  : await runApp('cargo', ['run', '-p', pkg, '--features', 'tauri/custom-protocol'], { cwd: srcTauri });

// Both lines can appear if the watchdog fired just as the run finished: any FAILED wins.
let ok = /SELF-TEST PASSED/.test(out) && !/SELF-TEST FAILED/.test(out) && !timedOut;
// Embedded mode runs the app directly, so its exit code counts too (e.g. a crash during shutdown
// after printing PASSED). Under `tauri dev` the CLI's exit code isn't the app's.
if (ok && !dev && code !== 0) {
  console.error(`\nFAIL: the app exited with code ${code} after reporting`);
  ok = false;
}
if (timedOut) console.error(`\nFAIL: timed out after ${RUN_TIMEOUT_MS / 1000}s (process tree killed)`);
else if (!/SELF-TEST (PASSED|FAILED)/.test(out)) console.error('\nFAIL: the app exited without reporting a self-test result');
if (dev) {
  // A rebuild mid-run means the dev watcher restarted the app (see /.taurignore).
  const restarts = (out.match(/Rebuilding application/g) ?? []).length;
  if (restarts > 0) {
    console.error(`\nFAIL: tauri dev restarted the app ${restarts} time(s) during the run`);
    ok = false;
  }
}
process.exit(ok ? 0 : 1);
