// OpenCode 2 plugin: publish to Redpanda as soon as an answer is finished.
//
// The plugin API exposes events but no way to list a session's messages, so
// instead of re-implementing the extraction it runs opencode-analytics.sh
// (produce.py: SQLite watermark -> Avro -> Redpanda) after each execution.
// Install: ln -s "$PWD/opencode-plugin/observatory.js" ~/.config/opencode/plugins/
import { spawn } from "node:child_process";
import { appendFileSync, openSync, realpathSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

// realpath: the plugin is loaded through a symlink, the script lives in the repo.
const ROOT = dirname(dirname(realpathSync(fileURLToPath(import.meta.url))));
const SCRIPT = join(ROOT, "opencode-analytics.sh");
const LOG = join(ROOT, "opencode-analytics.log");
const DEBOUNCE_MS = 2000;
const TRIGGERS = new Set([
  "session.execution.succeeded",
  "session.execution.failed",
  "session.execution.interrupted",
]);

// Module-level state: setup() runs once per project location, but every
// location must share a single producer run (it reads one global database).
let timer = null;
let running = false;
let pending = false;

function log(message) {
  try {
    appendFileSync(LOG, `${new Date().toISOString()} [plugin] ${message}\n`);
  } catch {}
}

function run() {
  if (running) {
    pending = true; // rerun once the current publication ends
    return;
  }
  running = true;
  pending = false;
  let child;
  try {
    const out = openSync(LOG, "a");
    child = spawn(SCRIPT, [], { cwd: ROOT, stdio: ["ignore", out, out] });
  } catch (error) {
    log(`cannot start ${SCRIPT}: ${error.message}`);
    running = false;
    return;
  }
  const done = () => {
    running = false;
    if (pending) run();
  };
  child.once("error", (error) => {
    log(`producer failed to start: ${error.message}`);
    done();
  });
  child.once("exit", (code) => {
    if (code) log(`producer exited with code ${code}`);
    done();
  });
}

// Coalesce bursts (subagents finishing together, several locations) into one run.
function schedule() {
  clearTimeout(timer);
  timer = setTimeout(run, DEBOUNCE_MS);
}

export default {
  id: "opencode-observatory",
  setup: async (ctx) => {
    const controller = new AbortController();
    const consuming = (async () => {
      for await (const event of ctx.event.subscribe({ signal: controller.signal })) {
        if (TRIGGERS.has(event.type)) schedule();
      }
    })().catch((error) => {
      if (!controller.signal.aborted) log(`event subscription failed: ${error.message}`);
    });
    log(`ready for ${ctx.location?.directory ?? "unknown location"}`);
    return async () => {
      controller.abort();
      await consuming;
    };
  },
};
