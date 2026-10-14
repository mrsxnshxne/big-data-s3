// OpenCode Observatory producer: OpenCode SQLite -> Avro events -> Redpanda.
//
// Runs in two modes, with the same code:
//   - OpenCode 2 plugin: publishes new rows when an answer ends, and once at
//     startup to catch up on anything missed while Redpanda was unreachable;
//   - CLI: node opencode-plugin/observatory.js [--full] [--dry-run]
//
// No dependencies: SQLite comes from the runtime (bun:sqlite inside OpenCode,
// node:sqlite from the CLI), Avro is encoded here, and events go through the
// Schema Registry and the HTTP Proxy of Redpanda with fetch().
// See opencode-plugin/README.md for installation and behaviour.
import { appendFileSync, existsSync, mkdirSync, readFileSync, realpathSync, renameSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";

// realpath: the plugin is loaded through a symlink, the project lives next to it.
const FILE = realpathSync(fileURLToPath(import.meta.url));
const ROOT = dirname(dirname(FILE));
const DATASETS = ["sessions", "messages", "parts", "tools"];
const KEYS = { sessions: "session_id", messages: "message_id", parts: "part_id", tools: "part_id" };
const TRIGGERS = new Set([
  "session.execution.succeeded",
  "session.execution.failed",
  "session.execution.interrupted",
]);
const DEBOUNCE_MS = 2000;
const BATCH_BYTES = 512 * 1024;

// ---------------------------------------------------------------- config

// OpenCode does not inherit the project's environment: read .env, and let
// real environment variables win (used by the CLI and the tests).
export function loadConfig() {
  const values = {};
  const envFile = join(ROOT, ".env");
  if (existsSync(envFile)) {
    for (const line of readFileSync(envFile, "utf8").split(/\r?\n/)) {
      const match = line.match(/^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$/);
      if (!match) continue;
      values[match[1]] = match[2].replace(/^(['"])(.*)\1$/, "$2").replace(/\$\{?HOME\}?/g, homedir());
    }
  }
  const env = { ...values, ...process.env };
  const fromRoot = (path) => (isAbsolute(path) ? path : join(ROOT, path));
  return {
    database: env.OPENCODE_DB || join(homedir(), ".local/share/opencode/opencode.db"),
    stateFile: fromRoot(env.PRODUCER_STATE_FILE || "data/state/producer-state.json"),
    logFile: fromRoot(env.OBSERVATORY_LOG || "opencode-analytics.log"),
    topicPrefix: env.KAFKA_TOPIC_PREFIX || "opencode",
    registryUrl: (env.SCHEMA_REGISTRY_URL || "http://localhost:8081").replace(/\/$/, ""),
    proxyUrl: (env.KAFKA_HTTP_PROXY_URL || "http://localhost:8082").replace(/\/$/, ""),
    // Used by claude-code-plugin/observatory.js, which shares this .env.
    claudeProjects: env.CLAUDE_PROJECTS_DIR || join(homedir(), ".claude/projects"),
    claudeStateFile: fromRoot(env.CLAUDE_STATE_FILE || "data/state/claude-code-state.json"),
  };
}

const schemas = Object.fromEntries(
  DATASETS.map((name) => [name, readFileSync(join(ROOT, "schemas", `${name}.avsc`), "utf8")]),
);

// ---------------------------------------------------------------- extraction

async function openDatabase(path) {
  // Read-only connection next to OpenCode's own writer (SQLite WAL mode).
  if (typeof Bun !== "undefined") {
    const { Database } = await import("bun:sqlite");
    const db = new Database(path, { readonly: true });
    return { all: (sql, ...params) => db.query(sql).all(...params), close: () => db.close() };
  }
  const { DatabaseSync } = await import("node:sqlite");
  const db = new DatabaseSync(path, { readOnly: true });
  return { all: (sql, ...params) => db.prepare(sql).all(...params), close: () => db.close() };
}

function obj(value) {
  if (value && typeof value === "object" && !Array.isArray(value)) return value;
  try {
    const parsed = JSON.parse(value || "{}");
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function text(value) {
  if (typeof value === "string") return value;
  if (Array.isArray(value)) return value.map(text).join("\n");
  if (value && typeof value === "object") {
    const inner = value.text ?? value.content ?? "";
    return typeof inner === "string" ? inner : JSON.stringify(inner);
  }
  return value == null ? "" : String(value);
}

// SQLite stores epoch milliseconds; the Avro schemas use timestamp-micros.
const micros = (ms) => (ms == null ? null : Number(ms) * 1000);

/**
 * Read every row updated after the stored watermark of each dataset.
 *
 * OpenCode 2 keeps sessions in session_v2 and messages in session_message;
 * the parts of a message (text, reasoning, tool calls) are embedded in the
 * JSON content of its data column, so parts and tools follow message updates.
 * Every record carries its source time_updated as _watermark.
 */
export async function extract(database, watermarks) {
  const db = await openDatabase(database);
  let sessions, messages;
  try {
    sessions = db.all(
      "select * from session_v2 where time_updated > ? order by time_updated",
      watermarks.sessions ?? 0,
    );
    messages = db.all(
      "select * from session_message where type in ('user', 'assistant') and time_updated > ? order by time_updated",
      Math.min(...["messages", "parts", "tools"].map((name) => watermarks[name] ?? 0)),
    );
  } finally {
    db.close();
  }

  const out = { sessions: [], messages: [], parts: [], tools: [] };
  for (const row of sessions) {
    const model = obj(row.model);
    out.sessions.push({
      session_id: row.id, title: row.title, directory: row.directory, agent: row.agent || "",
      model: model.id ?? (row.model || ""), provider: model.providerID ?? "", cost: row.cost || 0,
      tokens_input: row.tokens_input || 0, tokens_output: row.tokens_output || 0,
      tokens_reasoning: row.tokens_reasoning || 0, tokens_cache_read: row.tokens_cache_read || 0,
      tokens_cache_write: row.tokens_cache_write || 0,
      created_at: micros(row.time_created), updated_at: micros(row.time_updated),
      source: "opencode", _watermark: row.time_updated,
    });
  }

  for (const row of messages) {
    const data = obj(row.data);
    const tokens = obj(data.tokens);
    const updated = row.time_updated;
    if (updated > (watermarks.messages ?? 0)) {
      out.messages.push({
        message_id: row.id, session_id: row.session_id, role: row.type,
        agent: data.agent || (data.agents?.length ? data.agents[0] : ""), model: obj(data.model).id ?? "",
        finish: data.finish ?? "", cost: data.cost || 0,
        tokens_input: tokens.input || 0, tokens_output: tokens.output || 0,
        tokens_reasoning: tokens.reasoning || 0,
        created_at: micros(row.time_created), updated_at: micros(updated), source: "opencode", _watermark: updated,
      });
    }

    let parts = data.content;
    if (row.type === "user" && !Array.isArray(parts)) parts = [{ type: "text", text: data.text ?? "" }];
    (Array.isArray(parts) ? parts : []).forEach((raw, index) => {
      const part = obj(raw);
      const kind = part.type ?? "unknown";
      const partId = String(part.id || `${row.id}:${index}`);
      const time = obj(part.time);
      if (updated > (watermarks.parts ?? 0)) {
        out.parts.push({
          part_id: partId, message_id: row.id, session_id: row.session_id, type: kind,
          text: text(part.text ?? ""), created_at: micros(time.created ?? row.time_created),
          updated_at: micros(updated), source: "opencode", _watermark: updated,
        });
      }
      if (kind === "tool" && updated > (watermarks.tools ?? 0)) {
        const state = obj(part.state);
        out.tools.push({
          part_id: partId, message_id: row.id, session_id: row.session_id,
          tool: part.name ?? "unknown", call_id: String(part.id ?? ""), status: state.status ?? "",
          input: JSON.stringify(state.input ?? {}), output: text(state.content ?? state.error ?? ""),
          started_at: micros(time.ran ?? time.created ?? row.time_created),
          created_at: micros(time.created ?? row.time_created),
          updated_at: micros(updated), source: "opencode", _watermark: updated,
        });
      }
    });
  }
  return out;
}

// ---------------------------------------------------------------- Avro

// Minimal Avro binary encoder for the shapes used in schemas/*.avsc:
// string, long, double, null and unions of them (timestamp-micros is a long).
function encodeLong(value, bytes) {
  let n = BigInt(Math.round(value));
  n = n >= 0n ? n << 1n : (-n << 1n) - 1n; // zigzag
  do {
    let byte = Number(n & 0x7fn);
    n >>= 7n;
    if (n > 0n) byte |= 0x80;
    bytes.push(byte);
  } while (n > 0n);
}

function encodeValue(type, value, bytes, path) {
  if (Array.isArray(type)) {
    const branch = value == null ? type.indexOf("null") : type.findIndex((t) => t !== "null");
    if (branch < 0) throw new TypeError(`${path}: null is not allowed`);
    encodeLong(branch, bytes);
    return encodeValue(type[branch], value, bytes, path);
  }
  const name = typeof type === "object" ? type.type : type;
  switch (name) {
    case "null":
      return;
    case "string": {
      if (typeof value !== "string") throw new TypeError(`${path}: expected string, got ${typeof value}`);
      const encoded = new TextEncoder().encode(value);
      encodeLong(encoded.length, bytes);
      for (const byte of encoded) bytes.push(byte);
      return;
    }
    case "long":
      if (!Number.isFinite(Number(value))) throw new TypeError(`${path}: expected long, got ${value}`);
      return encodeLong(Number(value), bytes);
    case "double": {
      if (!Number.isFinite(Number(value))) throw new TypeError(`${path}: expected double, got ${value}`);
      const buffer = new DataView(new ArrayBuffer(8));
      buffer.setFloat64(0, Number(value), true);
      for (let i = 0; i < 8; i++) bytes.push(buffer.getUint8(i));
      return;
    }
    default:
      throw new TypeError(`${path}: unsupported Avro type ${name}`);
  }
}

// Confluent wire format: magic byte 0, schema id (4 bytes big-endian), payload.
function encodeRecord(schemaId, schema, record) {
  const bytes = [0, (schemaId >>> 24) & 255, (schemaId >>> 16) & 255, (schemaId >>> 8) & 255, schemaId & 255];
  for (const field of schema.fields) {
    encodeValue(field.type, record[field.name], bytes, `${schema.name}.${field.name}`);
  }
  return Buffer.from(bytes);
}

// ---------------------------------------------------------------- Redpanda

async function request(url, body, contentType, accept) {
  let response;
  try {
    response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": contentType, Accept: accept },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(30_000),
    });
  } catch (error) {
    throw new Error(`${new URL(url).origin} unreachable: ${error.cause?.message ?? error.message}`);
  }
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(`${url} -> HTTP ${response.status} ${JSON.stringify(payload)}`);
  return payload;
}

// Registers the schema under "<topic>-value" (same subject naming as the
// Python clients); the registry returns the existing id when it is unchanged.
async function registerSchema(config, topic, schema) {
  const { id } = await request(
    `${config.registryUrl}/subjects/${encodeURIComponent(`${topic}-value`)}/versions`,
    { schema },
    "application/vnd.schemaregistry.v1+json",
    "application/vnd.schemaregistry.v1+json",
  );
  return id;
}

async function sendBatch(config, topic, records) {
  const { offsets = [] } = await request(
    `${config.proxyUrl}/topics/${encodeURIComponent(topic)}`,
    { records },
    "application/vnd.kafka.binary.v2+json",
    "application/vnd.kafka.v2+json", // the proxy answers 406 to anything else
  );
  // Redpanda answers one offset per written batch, not per record.
  const failed = offsets.filter((offset) => offset.error_code);
  if (failed.length || !offsets.length) {
    throw new Error(`${topic}: batch of ${records.length} record(s) rejected ${JSON.stringify(failed[0] ?? offsets)}`);
  }
}

export async function publish(config, name, rows) {
  const topic = `${config.topicPrefix}.${name}`;
  const schema = JSON.parse(schemas[name]);
  // Encode everything first: a record that breaks the contract fails before any send.
  const records = rows.map((row) => ({
    key: Buffer.from(String(row[KEYS[name]])).toString("base64"),
    value: encodeRecord(0, schema, row),
  }));
  if (!records.length) return 0;
  const id = await registerSchema(config, topic, schemas[name]);
  let batch = [];
  let size = 0;
  for (const record of records) {
    record.value.writeUInt32BE(id, 1);
    record.value = record.value.toString("base64");
    batch.push(record);
    size += record.value.length;
    if (size >= BATCH_BYTES) {
      await sendBatch(config, topic, batch);
      batch = [];
      size = 0;
    }
  }
  if (batch.length) await sendBatch(config, topic, batch);
  return records.length;
}

// ---------------------------------------------------------------- watermark

function loadState(path) {
  try {
    const raw = JSON.parse(readFileSync(path, "utf8"));
    return Object.fromEntries(DATASETS.map((name) => [name, Number(raw[name] ?? 0)]));
  } catch {
    return {};
  }
}

function saveState(path, extracted, previous) {
  const state = { ...previous };
  for (const name of DATASETS) {
    for (const row of extracted[name]) state[name] = Math.max(state[name] ?? 0, row._watermark);
  }
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(`${path}.tmp`, JSON.stringify(state, Object.keys(state).sort(), 2) + "\n");
  renameSync(`${path}.tmp`, path); // atomic: a crash never leaves a half-written state
}

/**
 * One synchronisation: extract rows newer than the watermarks, publish them,
 * then advance the watermarks. They only move once Redpanda has acknowledged
 * every event, so a failure means the same rows are retried next time
 * (at-least-once; readers keep the latest version of each id).
 */
export async function sync({ full = false, dryRun = false } = {}) {
  const config = loadConfig();
  if (!existsSync(config.database)) throw new Error(`OpenCode database not found: ${config.database}`);
  const watermarks = full ? {} : loadState(config.stateFile);
  const extracted = await extract(config.database, watermarks);
  const summary = DATASETS.map((name) => `${extracted[name].length} ${name}`).join(", ");
  if (dryRun) return `dry run: pending events -> ${summary}`;
  for (const name of DATASETS) await publish(config, name, extracted[name]);
  saveState(config.stateFile, extracted, watermarks);
  return `produced to ${config.topicPrefix}.* -> ${summary}`;
}

// ---------------------------------------------------------------- plugin

function log(message) {
  try {
    appendFileSync(loadConfig().logFile, `${new Date().toISOString()} [observatory] ${message}\n`);
  } catch {}
}

// Module-level state: setup() runs once per project location, but every
// location reads the same global database and must share one sync at a time.
let timer = null;
let running = false;
let pending = false;

async function runSync() {
  if (running) {
    pending = true; // rerun once the current synchronisation ends
    return;
  }
  running = true;
  pending = false;
  try {
    log(await sync());
  } catch (error) {
    log(`sync failed, will retry on the next answer: ${error.message}`);
  } finally {
    running = false;
    if (pending) runSync();
  }
}

// Coalesce bursts (subagents finishing together, several locations) into one sync.
function schedule() {
  clearTimeout(timer);
  timer = setTimeout(runSync, DEBOUNCE_MS);
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
    schedule(); // catch up on answers given while Redpanda was unreachable
    return async () => {
      controller.abort();
      await consuming;
    };
  },
};

// ---------------------------------------------------------------- CLI

// Inside OpenCode, argv[1] is its virtual /$bunfs binary: only a real path to
// this file (node or bun) means "run as a command".
function isCommandLine() {
  if (typeof Bun !== "undefined") return import.meta.main === true;
  try {
    return realpathSync(process.argv[1]) === FILE;
  } catch {
    return false;
  }
}

if (isCommandLine()) {
  const args = new Set(process.argv.slice(2));
  if (args.has("--help") || args.has("-h")) {
    console.log("Usage: node opencode-plugin/observatory.js [--full] [--dry-run]");
  } else {
    sync({ full: args.has("--full"), dryRun: args.has("--dry-run") })
      .then((summary) => console.log(summary))
      .catch((error) => {
        console.error(error.message);
        process.exitCode = 1;
      });
  }
}
