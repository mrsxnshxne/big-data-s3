// Claude Code Observatory producer: Claude Code transcripts -> Avro events -> Redpanda.
//
// Claude Code writes every session as JSON lines under ~/.claude/projects:
//   <project>/<session>.jsonl                      main conversation
//   <project>/<session>/subagents/agent-*.jsonl    subagents, same sessionId
// This script turns them into the same sessions/messages/parts/tools events
// as the OpenCode plugin, tagged source = "claude-code", and publishes them
// with the OpenCode plugin's Avro encoder and Redpanda client.
//
// Usage: node plugins/claude/observatory.js [--full] [--dry-run] [--log]
// Run it from a Claude Code Stop hook to publish after every answer
// (see plugins/claude/README.md).
import { appendFileSync, existsSync, mkdirSync, readdirSync, readFileSync, realpathSync, renameSync, statSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { loadConfig, publish } from "../opencode/observatory.js";

const FILE = realpathSync(fileURLToPath(import.meta.url));
const DATASETS = ["sessions", "messages", "parts", "tools"];
const SOURCE = "claude-code";

// ---------------------------------------------------------------- cost

// $ per million tokens: [input, output, cache read]. Cache writes cost 1.25x
// the input price for the 5-minute TTL and 2x for the 1-hour TTL. The longest
// matching prefix wins, so dated ids (claude-sonnet-4-5-20250929) resolve too.
// Estimate only: Pro/Max subscriptions are not billed per token.
const PRICES = {
  "claude-fable-5-1": [10, 50, 0.25],
  "claude-fable-5": [10, 50, 1],
  "claude-mythos-5-1": [10, 50, 0.25],
  "claude-mythos-5": [10, 50, 1],
  "claude-opus-5-5": [4, 20, 0.2],
  "claude-opus-5": [5, 25, 0.5],
  "claude-opus-4-8": [5, 25, 0.5],
  "claude-opus-4-7": [5, 25, 0.5],
  "claude-opus-4-6": [5, 25, 0.5],
  "claude-opus-4-5": [5, 25, 0.5],
  "claude-opus-4": [15, 75, 1.5],
  "claude-sonnet-5-5": [2, 10, 0.2],
  "claude-sonnet-5": [2, 10, 0.2],
  "claude-sonnet-4": [3, 15, 0.3],
  "claude-3-7-sonnet": [3, 15, 0.3],
  "claude-haiku-5-5": [0.1, 0.5, 0.01],
  "claude-haiku-4-5": [1, 5, 0.1],
  "claude-3-5-haiku": [0.8, 4, 0.08],
};
const PRICE_KEYS = Object.keys(PRICES).sort((a, b) => b.length - a.length);

export function estimateCost(model, usage) {
  const key = PRICE_KEYS.find((prefix) => String(model || "").startsWith(prefix));
  if (!key) return 0;
  const [input, output, read] = PRICES[key];
  const creation = usage.cache_creation ?? {};
  const write5m = creation.ephemeral_5m_input_tokens ?? 0;
  const write1h = creation.ephemeral_1h_input_tokens ?? 0;
  // Older transcripts only carry the total cache write, billed at the 5m rate.
  const writeOther = Math.max(0, (usage.cache_creation_input_tokens ?? 0) - write5m - write1h);
  return (
    (usage.input_tokens ?? 0) * input +
    (usage.output_tokens ?? 0) * output +
    (usage.cache_read_input_tokens ?? 0) * read +
    (write5m + writeOther) * input * 1.25 +
    write1h * input * 2
  ) / 1e6;
}

// ---------------------------------------------------------------- extraction

function text(value) {
  if (typeof value === "string") return value;
  if (Array.isArray(value)) return value.map(text).filter(Boolean).join("\n");
  if (value && typeof value === "object") {
    if (value.type === "image") return "[image]";
    const inner = value.text ?? value.content ?? "";
    return typeof inner === "string" ? inner : text(inner);
  }
  return value == null ? "" : String(value);
}

const micros = (iso) => {
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? ms * 1000 : null;
};

function readLines(path) {
  const entries = [];
  for (const line of readFileSync(path, "utf8").split("\n")) {
    if (!line.trim()) continue;
    try {
      entries.push(JSON.parse(line));
    } catch {} // a line being written while we read: picked up next time
  }
  return entries;
}

/** Group transcript files by session: the main file plus its subagents. */
export function discover(root) {
  const units = new Map();
  if (!existsSync(root)) return units;
  for (const project of readdirSync(root, { withFileTypes: true })) {
    if (!project.isDirectory()) continue;
    const dir = join(root, project.name);
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      if (!entry.isFile() || !entry.name.endsWith(".jsonl")) continue;
      const id = entry.name.slice(0, -".jsonl".length);
      const files = [join(dir, entry.name)];
      const subagents = join(dir, id, "subagents");
      if (existsSync(subagents)) {
        for (const name of readdirSync(subagents)) {
          if (name.endsWith(".jsonl")) files.push(join(subagents, name));
        }
      }
      const mtime = Math.max(...files.map((path) => statSync(path).mtimeMs));
      units.set(`${project.name}/${id}`, { files, mtime });
    }
  }
  return units;
}

/**
 * Turn the transcript files of one session into records.
 *
 * An assistant message is spread over several lines (one per content block)
 * that share message.id and repeat its usage; tool results come back in later
 * user lines, keyed by tool_use_id. Every record gets updated_at = time of its
 * latest line, so readers keep the most complete version of each id.
 */
export function parseSession(files) {
  const out = { sessions: [], messages: [], parts: [], tools: [] };
  const messages = new Map();
  const tools = new Map();
  let sessionId = null;
  let title = "";
  let firstPrompt = "";
  let directory = "";
  let model = "";
  let first = null;
  let last = null;

  const touch = (time) => {
    if (time == null) return;
    first = first == null ? time : Math.min(first, time);
    last = last == null ? time : Math.max(last, time);
  };

  for (const path of files) {
    for (const entry of readLines(path)) {
      if (entry.type === "ai-title" && entry.aiTitle) title = entry.aiTitle;
      if (entry.type === "summary" && entry.summary && !title) title = entry.summary;
      if (entry.type !== "user" && entry.type !== "assistant") continue;
      if (entry.isMeta || !entry.message) continue;
      sessionId ??= entry.sessionId;
      const time = micros(entry.timestamp);
      touch(time);
      if (!entry.isSidechain && entry.cwd) directory = entry.cwd;
      const agent = entry.agentId ? `subagent:${entry.agentId}` : "main";
      const content = entry.message.content;
      const blocks = Array.isArray(content) ? content : [{ type: "text", text: text(content) }];

      if (entry.type === "user") {
        for (const block of blocks) {
          if (block.type !== "tool_result") continue;
          const call = tools.get(block.tool_use_id);
          if (!call) continue;
          call.status = block.is_error ? "error" : "completed";
          call.output = text(block.content);
          call.updated_at = time;
        }
        const prompt = blocks.filter((block) => block.type !== "tool_result");
        if (!prompt.length) continue; // only tool results: not a prompt
        if (!entry.isSidechain && !firstPrompt) firstPrompt = text(prompt);
        out.messages.push({
          message_id: entry.uuid, session_id: entry.sessionId, role: "user", agent, model: "",
          finish: "", cost: 0, tokens_input: 0, tokens_output: 0, tokens_reasoning: 0,
          created_at: time, updated_at: time,
        });
        prompt.forEach((block, index) => {
          out.parts.push({
            part_id: `${entry.uuid}:${index}`, message_id: entry.uuid, session_id: entry.sessionId,
            type: "text", text: text(block), created_at: time, updated_at: time,
          });
        });
        continue;
      }

      const id = entry.message.id || entry.uuid;
      const usage = entry.message.usage ?? {};
      const msgModel = entry.message.model === "<synthetic>" ? "" : entry.message.model || "";
      if (msgModel) model = msgModel;
      const message = messages.get(id) ?? {
        message_id: id, session_id: entry.sessionId, role: "assistant", agent, model: msgModel,
        created_at: time, blocks: 0,
      };
      // The usage repeated on the last line of a message is the final one.
      Object.assign(message, {
        finish: entry.message.stop_reason ?? message.finish ?? "",
        cost: estimateCost(msgModel, usage),
        tokens_input: usage.input_tokens ?? 0,
        tokens_output: usage.output_tokens ?? 0,
        // output_tokens already includes thinking: do not count it twice.
        tokens_reasoning: 0,
        cache_read: usage.cache_read_input_tokens ?? 0,
        cache_write: usage.cache_creation_input_tokens ?? 0,
        updated_at: time,
      });
      messages.set(id, message);

      for (const block of blocks) {
        const index = message.blocks++;
        const kind = { thinking: "reasoning", tool_use: "tool" }[block.type] ?? block.type;
        const partId = block.type === "tool_use" ? block.id : `${id}:${index}`;
        out.parts.push({
          part_id: partId, message_id: id, session_id: entry.sessionId, type: kind,
          text: block.type === "thinking" ? block.thinking ?? "" : block.type === "text" ? block.text ?? "" : "",
          created_at: time, updated_at: time,
        });
        if (block.type === "tool_use") {
          tools.set(block.id, {
            part_id: block.id, message_id: id, session_id: entry.sessionId, tool: block.name ?? "unknown",
            call_id: block.id, status: "running", input: JSON.stringify(block.input ?? {}), output: "",
            started_at: time, created_at: time, updated_at: time,
          });
        }
      }
    }
  }
  if (!sessionId) return out;

  let cost = 0;
  const tokens = { input: 0, output: 0, cache_read: 0, cache_write: 0 };
  for (const { blocks, cache_read, cache_write, ...message } of messages.values()) {
    out.messages.push(message);
    cost += message.cost;
    tokens.input += message.tokens_input;
    tokens.output += message.tokens_output;
    tokens.cache_read += cache_read;
    tokens.cache_write += cache_write;
  }
  out.tools.push(...tools.values());
  out.sessions.push({
    session_id: sessionId, title: title || firstPrompt.split("\n")[0].slice(0, 120) || sessionId,
    directory, agent: "", model, provider: "anthropic", cost,
    tokens_input: tokens.input, tokens_output: tokens.output, tokens_reasoning: 0,
    tokens_cache_read: tokens.cache_read, tokens_cache_write: tokens.cache_write,
    created_at: first, updated_at: last,
  });
  for (const name of DATASETS) for (const row of out[name]) row.source = SOURCE;
  return out;
}

/**
 * Records of every session changed since the stored state. A session is
 * reparsed whole when one of its files changed, but only records newer than
 * its watermark are published (plus its session row, which aggregates all).
 */
export function extract(root, state) {
  const out = { sessions: [], messages: [], parts: [], tools: [] };
  const next = { ...state };
  for (const [key, { files, mtime }] of discover(root)) {
    const previous = state[key];
    if (previous && previous.mtime >= mtime) continue;
    const records = parseSession(files);
    const watermark = previous?.watermark ?? 0;
    let latest = watermark;
    for (const name of DATASETS) {
      for (const row of records[name]) {
        if (name !== "sessions" && !(row.updated_at > watermark)) continue;
        out[name].push(row);
        latest = Math.max(latest, row.updated_at ?? 0);
      }
    }
    next[key] = { mtime, watermark: latest };
  }
  return { records: out, state: next };
}

// ---------------------------------------------------------------- state

function loadState(path) {
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return {};
  }
}

function saveState(path, state) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(`${path}.tmp`, JSON.stringify(state, null, 2) + "\n");
  renameSync(`${path}.tmp`, path); // atomic: a crash never leaves a half-written state
}

/**
 * One synchronisation. The state only moves once Redpanda has acknowledged
 * every event, so a failure means the same records are retried next time
 * (at-least-once; readers keep the latest version of each id).
 */
export async function sync({ full = false, dryRun = false } = {}) {
  const config = loadConfig();
  if (!existsSync(config.claudeProjects)) throw new Error(`Claude Code projects not found: ${config.claudeProjects}`);
  const { records, state } = extract(config.claudeProjects, full ? {} : loadState(config.claudeStateFile));
  const summary = DATASETS.map((name) => `${records[name].length} ${name}`).join(", ");
  if (dryRun) return `dry run: pending events -> ${summary}`;
  for (const name of DATASETS) await publish(config, name, records[name]);
  saveState(config.claudeStateFile, state);
  return `produced to ${config.topicPrefix}.* -> ${summary}`;
}

// ---------------------------------------------------------------- CLI

function isCommandLine() {
  try {
    return realpathSync(process.argv[1]) === FILE;
  } catch {
    return false;
  }
}

if (isCommandLine()) {
  const args = new Set(process.argv.slice(2));
  // --log: the Stop hook discards stdout, so write the outcome to the shared log.
  const report = (message) => {
    if (!args.has("--log")) return console.log(message);
    try {
      appendFileSync(loadConfig().logFile, `${new Date().toISOString()} [claude-code] ${message}\n`);
    } catch {}
  };
  if (args.has("--help") || args.has("-h")) {
    console.log("Usage: node plugins/claude/observatory.js [--full] [--dry-run] [--log]");
  } else {
    sync({ full: args.has("--full"), dryRun: args.has("--dry-run") })
      .then(report)
      .catch((error) => {
        report(`sync failed, will retry on the next answer: ${error.message}`);
        process.exitCode = 1;
      });
  }
}
