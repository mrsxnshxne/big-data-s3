-- Live path: Redpanda opencode.* (Avro) -> flat JSON events on live.events, read by the SSE bridge
-- (live/server.py), plus 10 s activity windows on live.activity.
-- Prompts, tool inputs/outputs and part text are deliberately NOT forwarded.
-- Each topic is read from the beginning so the panel can rebuild its totals after a restart.
--
-- Arroyo 0.15 reads Avro timestamp-micros longs as if they were nanoseconds (dates land in 1970):
-- the raw number is the real value in microseconds, so it is converted back with to_timestamp_micros().
CREATE TABLE messages_in (
  message_id TEXT NOT NULL,
  session_id TEXT,
  role TEXT,
  agent TEXT,
  model TEXT,
  finish TEXT,
  cost DOUBLE,
  tokens_input BIGINT,
  tokens_output BIGINT,
  tokens_reasoning BIGINT,
  tokens_cache_read BIGINT,
  tokens_cache_write BIGINT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  source TEXT
) WITH (
  connector = 'kafka',
  type = 'source',
  bootstrap_servers = 'redpanda:29092',
  topic = 'opencode.messages',
  format = 'avro',
  'avro.confluent_schema_registry' = 'true',
  'schema_registry.endpoint' = 'http://redpanda:8081',
  'source.offset' = 'earliest',
  'source.group_id' = 'arroyo-live-messages'
);

CREATE TABLE tools_in (
  part_id TEXT NOT NULL,
  message_id TEXT,
  session_id TEXT,
  tool TEXT,
  call_id TEXT,
  status TEXT,
  started_at TIMESTAMP,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  source TEXT
) WITH (
  connector = 'kafka',
  type = 'source',
  bootstrap_servers = 'redpanda:29092',
  topic = 'opencode.tools',
  format = 'avro',
  'avro.confluent_schema_registry' = 'true',
  'schema_registry.endpoint' = 'http://redpanda:8081',
  'source.offset' = 'earliest',
  'source.group_id' = 'arroyo-live-tools'
);

CREATE TABLE sessions_in (
  session_id TEXT NOT NULL,
  title TEXT,
  agent TEXT,
  model TEXT,
  cost DOUBLE,
  tokens_input BIGINT,
  tokens_output BIGINT,
  tokens_cache_read BIGINT,
  tokens_cache_write BIGINT,
  updated_at TIMESTAMP,
  source TEXT
) WITH (
  connector = 'kafka',
  type = 'source',
  bootstrap_servers = 'redpanda:29092',
  topic = 'opencode.sessions',
  format = 'avro',
  'avro.confluent_schema_registry' = 'true',
  'schema_registry.endpoint' = 'http://redpanda:8081',
  'source.offset' = 'earliest',
  'source.group_id' = 'arroyo-live-sessions'
);

CREATE TABLE live_events (
  kind TEXT,
  id TEXT,
  session_id TEXT,
  title TEXT,
  role TEXT,
  agent TEXT,
  model TEXT,
  tool TEXT,
  status TEXT,
  cost DOUBLE,
  tokens_input BIGINT,
  tokens_output BIGINT,
  tokens_cache_read BIGINT,
  tokens_cache_write BIGINT,
  ts TIMESTAMP
) WITH (
  connector = 'kafka',
  type = 'sink',
  bootstrap_servers = 'redpanda:29092',
  topic = 'live.events',
  format = 'json',
  'sink.key_field' = 'id'
);

INSERT INTO live_events
SELECT 'message' AS kind,
       message_id AS id,
       session_id AS session_id,
       CAST(NULL AS TEXT) AS title,
       role AS role,
       agent AS agent,
       model AS model,
       CAST(NULL AS TEXT) AS tool,
       finish AS status,
       cost AS cost,
       tokens_input AS tokens_input,
       tokens_output AS tokens_output,
       tokens_cache_read AS tokens_cache_read,
       tokens_cache_write AS tokens_cache_write,
       to_timestamp_micros(CAST(updated_at AS BIGINT)) AS ts
FROM messages_in;

INSERT INTO live_events
SELECT 'tool' AS kind,
       part_id AS id,
       session_id AS session_id,
       CAST(NULL AS TEXT) AS title,
       CAST(NULL AS TEXT) AS role,
       CAST(NULL AS TEXT) AS agent,
       CAST(NULL AS TEXT) AS model,
       tool AS tool,
       status AS status,
       CAST(NULL AS DOUBLE) AS cost,
       CAST(NULL AS BIGINT) AS tokens_input,
       CAST(NULL AS BIGINT) AS tokens_output,
       CAST(NULL AS BIGINT) AS tokens_cache_read,
       CAST(NULL AS BIGINT) AS tokens_cache_write,
       to_timestamp_micros(CAST(coalesce(updated_at, created_at) AS BIGINT)) AS ts
FROM tools_in;

INSERT INTO live_events
SELECT 'session' AS kind,
       session_id AS id,
       session_id AS session_id,
       title AS title,
       CAST(NULL AS TEXT) AS role,
       agent AS agent,
       model AS model,
       CAST(NULL AS TEXT) AS tool,
       CAST(NULL AS TEXT) AS status,
       cost AS cost,
       tokens_input AS tokens_input,
       tokens_output AS tokens_output,
       tokens_cache_read AS tokens_cache_read,
       tokens_cache_write AS tokens_cache_write,
       to_timestamp_micros(CAST(updated_at AS BIGINT)) AS ts
FROM sessions_in;

CREATE TABLE live_activity (
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  model TEXT,
  messages BIGINT,
  cost DOUBLE,
  tokens_input BIGINT,
  tokens_output BIGINT
) WITH (
  connector = 'kafka',
  type = 'sink',
  bootstrap_servers = 'redpanda:29092',
  topic = 'live.activity',
  format = 'json'
);

INSERT INTO live_activity
SELECT window.start, window.end, model, messages, cost, tokens_input, tokens_output
FROM (
  SELECT tumble(interval '10 seconds') AS window, model,
         count(*) AS messages, sum(cost) AS cost,
         sum(tokens_input) AS tokens_input, sum(tokens_output) AS tokens_output
  FROM messages_in
  WHERE role = 'assistant'
  GROUP BY window, model
);
