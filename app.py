from __future__ import annotations

import json
import os

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pyarrow as pa
import streamlit as st
from pyiceberg.catalog import load_catalog

st.set_page_config(page_title="Observatory", page_icon="◌", layout="wide")

ICEBERG_CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
ICEBERG_NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "analytics")
TABLES = ("sessions", "messages", "parts", "tools")
PRIMARY_KEYS = {"sessions": "session_id", "messages": "message_id", "parts": "part_id", "tools": "part_id"}
# Rows written before the source column existed come from OpenCode.
SOURCES = {"opencode": "◌ OpenCode", "claude-code": "✳ Claude Code"}
DEFAULT_SOURCE = "opencode"

TOOL_ICONS = {
    "read": "📖", "write": "✍️", "edit": "🖊️", "multi_edit": "🖊️", "multiedit": "🖊️", "notebookedit": "🖊️",
    "bash": "💻", "shell": "💻", "exec": "💻",
    "grep": "🔎", "glob": "📂", "ls": "🗂️", "search": "🔎",
    "task": "🤖", "subtask": "🤖", "agent": "🤖",
    "webfetch": "🌐", "websearch": "🌐", "fetch": "🌐",
    "todowrite": "📋", "question": "❓", "apply_patch": "🩹",
    "skill": "🎯", "memory": "🧠",
}
STATUS_BADGES = {
    "completed": "✅",
    "error": "❌",
    "failed": "❌",
    "running": "🔄",
    "streaming": "🔄",
    "pending": "⏳",
}
PLOTLY_MARGIN = dict(l=40, r=20, t=10, b=30)
DOW_FR = ["Dim", "Lun", "Mar", "Mer", "Jeu", "Ven", "Sam"]
# Keys that best summarise what a tool call was about, in order of preference.
COMPARE_COLORS = ("#3b82f6", "#f97316")  # blue, orange
COMPARE_MARKS = ("🔵", "🟠")
SUMMARY_KEYS = ("path", "file_path", "filePath", "command", "cmd", "pattern", "query", "url", "description", "prompt", "subject", "skill", "id")


@st.cache_data(ttl=30, show_spinner="Chargement des tables Iceberg…")
def load_tables(catalog_uri: str) -> dict[str, pa.Table]:
    """Scan the Iceberg tables appended by the streaming sink."""
    catalog = load_catalog(
        "opencode",
        type="rest",
        uri=catalog_uri,
        warehouse=os.getenv("ICEBERG_WAREHOUSE", "s3://iceberg-warehouse"),
        **{
            "s3.endpoint": os.getenv("ICEBERG_S3_ENDPOINT", os.getenv("S3_ENDPOINT", "http://localhost:9000")),
            "s3.access-key-id": os.getenv("S3_ACCESS_KEY", "rustfsadmin"),
            "s3.secret-access-key": os.getenv("S3_SECRET_KEY", "rustfsadmin"),
            "s3.region": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            "s3.path-style-access": os.getenv("ICEBERG_S3_PATH_STYLE_ACCESS", "true"),
        },
    )
    tables = {}
    for name in TABLES:
        try:
            table = catalog.load_table(f"{ICEBERG_NAMESPACE}.{name}").scan().to_arrow()
        except Exception:
            continue
        if "source" not in table.schema.names:
            table = table.append_column("source", pa.nulls(len(table), pa.string()))
        tables[name] = table
    return tables


def dedup_view_sql(name: str, sources: list[str]) -> str:
    """The sink appends events; keep only the latest version of each row of the chosen sources."""
    key = PRIMARY_KEYS[name]
    allowed = ", ".join(f"'{source}'" for source in sources if source in SOURCES)
    return f"""
        create or replace view {name} as
        select * exclude (_version) from (
            select * replace (coalesce(source, '{DEFAULT_SOURCE}') as source),
                   row_number() over (partition by {key} order by updated_at desc) as _version
            from raw_{name}
        )
        where _version = 1 and source in ({allowed or "null"})
    """


def connect(tables: dict[str, pa.Table], sources: list[str]) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    for name, table in tables.items():
        connection.register(f"raw_{name}", table)
        connection.execute(dedup_view_sql(name, sources))
    return connection


def chart(fig: go.Figure, key: str) -> None:
    """Render a Plotly figure with a consistent layout."""
    fig.update_layout(margin=PLOTLY_MARGIN)
    st.plotly_chart(fig, use_container_width=True, key=key)


def fmt_count(value: object) -> str:
    """Human-friendly compact numbers: 12 300 -> 12,3 k."""
    try:
        number = float(value or 0)
    except (TypeError, ValueError):
        return "—"
    for threshold, suffix in ((1_000_000, "M"), (1_000, "k")):
        if abs(number) >= threshold:
            return f"{number / threshold:.1f} {suffix}"
    return f"{int(number):,}"


def one_line(value: object, limit: int) -> str:
    text = " ".join(str(value or "").split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def parse_json(raw: object) -> object:
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return None


def tool_summary(tool_input: object) -> str:
    """Short human-readable hint of what the tool call targeted."""
    data = parse_json(tool_input) if isinstance(tool_input, str) else tool_input
    if isinstance(data, dict):
        for key in SUMMARY_KEYS:
            value = data.get(key)
            if value:
                return one_line(value, 64)
    elif isinstance(data, list) and data:
        return one_line(data[0], 64)
    return one_line(tool_input, 64) if tool_input else ""


def output_preview(output: object, limit: int = 100) -> str:
    """First meaningful line of a tool result, for the collapsed row."""
    if not output:
        return ""
    data = parse_json(output) if isinstance(output, str) else output
    if isinstance(data, dict):
        for key in ("output", "text", "content", "result", "message", "error"):
            if data.get(key):
                return one_line(data[key], limit)
        return ""
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                for key in ("text", "content", "output"):
                    if item.get(key):
                        return one_line(item[key], limit)
        return one_line(data[0], limit) if data else ""
    return one_line(output, limit)


def render_tool(part: object) -> None:
    """One tool call: compact titled expander with structured input/output."""
    tool = str(part.tool or "outil")
    status = str(part.status or "")
    icon = TOOL_ICONS.get(tool.lower(), "🔧")
    badge = STATUS_BADGES.get(status, "❔")
    summary = tool_summary(part.input)
    title = f"{badge} {icon} **{tool}**"
    if summary:
        title += f" · `{one_line(summary, 52)}`"
    with st.expander(title, expanded=False):
        left, right = st.columns(2)
        with left:
            st.markdown("##### ⚙️ Entrée")
            data = parse_json(part.input) if isinstance(part.input, str) else part.input
            if data is not None:
                st.json(data, expanded=False)
            else:
                st.code(one_line(part.input, 4000) or "—", language=None)
        with right:
            st.markdown("##### 📤 Sortie")
            if part.output:
                data = parse_json(part.output) if isinstance(part.output, str) else part.output
                if data is not None:
                    st.json(data, expanded=False)
                else:
                    st.code(one_line(part.output, 4000), language=None)
            else:
                st.caption(f"En attente de résultat ({status or 'statut inconnu'})")
        preview = output_preview(part.output)
        if preview:
            st.caption(f"👁️ Aperçu : {preview}")


def render_message(message: object, parts: "pd_DataFrame") -> None:  # type: ignore[name-defined]
    """Render one conversation message as a chat bubble."""
    is_user = message.role == "user"
    avatar = "🧑" if is_user else "🤖"
    with st.chat_message("user" if is_user else "assistant", avatar=avatar):
        stats = []
        if not is_user:
            if message.model:
                stats.append(f"🧩 {message.model}")
            tokens = int(message.tokens_output or 0) + int(message.tokens_reasoning or 0)
            if tokens:
                stats.append(f"🔤 {fmt_count(tokens)} tok")
            if float(message.cost or 0):
                stats.append(f"💰 ${float(message.cost):.4f}")
        stats.insert(0, f"🕐 {message.created_at:%d %b %H:%M:%S}")
        st.caption(" · ".join(stats))

        tools = parts[parts.type == "tool"]
        texts = parts[parts.type == "text"]
        if len(tools) or len(texts) > 1:
            chips = []
            if len(texts):
                chips.append(f"💬 {len(texts)} message{'s' if len(texts) > 1 else ''}")
            if len(tools):
                errors = int((tools.status == "error").sum())
                chips.append(f"🔧 {len(tools)} outil{'s' if len(tools) > 1 else ''}")
                if errors:
                    chips.append(f"❌ {errors} erreur{'s' if errors > 1 else ''}")
            st.caption(" | ".join(chips))

        for _, part in parts.iterrows():
            if part.type == "tool":
                render_tool(part)
            elif part.type == "reasoning" and part.text:
                with st.expander("💭 Réflexion", expanded=False):
                    st.markdown(f"<div style='color:gray;font-style:italic'>{part.text}</div>", unsafe_allow_html=True)
            elif part.type == "text" and part.text:
                st.markdown(part.text)


def render_transcript(con: duckdb.DuckDBPyConnection, messages: pd.DataFrame) -> None:
    for _, message in messages.iterrows():
        parts = con.execute("""
            select p.type, p.text, t.tool, t.status, t.input, t.output
            from parts p
            left join tools t on t.part_id = p.part_id
            where p.message_id = ?
            order by p.created_at
        """, [message.message_id]).df()
        render_message(message, parts)


def known(value: object) -> bool:
    return value is not None and value == value  # NaN != NaN


def fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds >= 3600:
        return f"{seconds // 3600} h {seconds % 3600 // 60:02d}"
    if seconds >= 60:
        return f"{seconds // 60} min {seconds % 60:02d}"
    return f"{seconds} s"


def fmt_dollars(value: float) -> str:
    return f"${float(value):.4f}"


def fmt_pct(value: float) -> str:
    return f"{float(value):.0%}"


# (key, label, which value wins: "low", "high" or None when it is not a quality signal, format)
COMPARE_METRICS = (
    ("duration_s", "⏱️ Durée totale", "low", fmt_duration),
    ("turn_median_s", "⏳ Temps médian par tour", "low", fmt_duration),
    ("prompts", "🧑 Prompts utilisateur", "low", fmt_count),
    ("replies", "🤖 Messages agent", None, fmt_count),
    ("tokens_generated", "🔤 Tokens générés", "low", fmt_count),
    ("tokens_per_prompt", "🔤 Tokens générés / prompt", "low", fmt_count),
    ("context", "📥 Tokens de contexte (entrée + cache)", "low", fmt_count),
    ("cache_ratio", "♻️ Part du contexte lue en cache", "high", fmt_pct),
    ("peak_context", "📈 Pic de contexte (un appel)", "low", fmt_count),
    ("mean_context", "📊 Contexte moyen par appel", "low", fmt_count),
    ("compactions", "🗜️ Compactions (chute du contexte ≥ 30 %)", None, fmt_count),
    ("cost", "💰 Coût", "low", fmt_dollars),
    ("cost_per_prompt", "💰 Coût / prompt", "low", fmt_dollars),
    ("tool_calls", "🔧 Appels outils", None, fmt_count),
    ("tool_errors", "❌ Erreurs outils", "low", fmt_count),
    ("error_rate", "❌ Taux d'erreur outils", "low", fmt_pct),
    ("distinct_tools", "🧰 Outils distincts", None, fmt_count),
    ("tool_calls_per_prompt", "🔧 Appels outils / prompt", "low", fmt_count),
    ("repeated_calls", "🔁 Appels outils identiques répétés", "low", fmt_count),
    ("tool_output_chars", "📦 Caractères renvoyés par les outils", "low", fmt_count),
)


def turn_durations(con: duckdb.DuckDBPyConnection, session_id: str) -> pd.Series:
    """Seconds from each user prompt to the last agent activity before the next one."""
    messages = con.execute("""
        select role, agent, created_at, coalesce(updated_at, created_at) as updated_at
        from messages where session_id = ? order by created_at
    """, [session_id]).df()
    # Subagent prompts are written by the main agent, not by the user.
    prompt = (messages.role == "user") & ~messages.agent.fillna("").str.startswith("subagent:")
    messages["turn"] = prompt.cumsum()
    turns = messages[messages.turn > 0].groupby("turn").agg(start=("created_at", "min"), end=("updated_at", "max"))
    return (turns.end - turns.start).dt.total_seconds()


def context_series(con: duckdb.DuckDBPyConnection, session_id: str) -> pd.DataFrame:
    """Prompt size of each model call of the main agent: fresh input plus cache reads and writes."""
    series = con.execute("""
        select created_at,
               coalesce(tokens_input, 0) + coalesce(tokens_cache_read, 0)
                 + coalesce(tokens_cache_write, 0)                  as context,
               coalesce(tokens_cache_read, 0)                       as cache_read
        from messages
        where session_id = ? and role = 'assistant' and coalesce(agent, '') not like 'subagent:%'
        order by created_at
    """, [session_id]).df()
    series.insert(0, "call", range(1, len(series) + 1))
    series["cache_share"] = (series.cache_read / series.context).where(series.context > 0)
    return series


def session_profile(con: duckdb.DuckDBPyConnection, session_id: str) -> dict:
    """The figures of one conversation listed in COMPARE_METRICS."""
    profile = con.execute("""
        select s.source, s.model, s.title,
               coalesce(s.tokens_input, 0) + coalesce(s.tokens_cache_read, 0)
                 + coalesce(s.tokens_cache_write, 0)                           as context,
               coalesce(s.tokens_cache_read, 0)                                as cache_read,
               coalesce(s.tokens_output, 0) + coalesce(s.tokens_reasoning, 0)  as tokens_generated,
               coalesce(s.cost, 0)                                             as cost,
               (select count(*) from messages m where m.session_id = s.session_id and m.role = 'user'
                  and coalesce(m.agent, '') not like 'subagent:%')             as prompts,
               (select count(*) from messages m
                 where m.session_id = s.session_id and m.role = 'assistant')   as replies,
               (select count(*) from tools t where t.session_id = s.session_id) as tool_calls,
               (select count(*) from tools t
                 where t.session_id = s.session_id and t.status = 'error')     as tool_errors,
               (select count(distinct tool) from tools t
                 where t.session_id = s.session_id)                            as distinct_tools,
               (select count(*) - count(distinct (tool, input)) from tools t
                 where t.session_id = s.session_id)                            as repeated_calls,
               (select coalesce(sum(length(output)), 0) from tools t
                 where t.session_id = s.session_id)                            as tool_output_chars,
               (select epoch(max(coalesce(m.updated_at, m.created_at)) - min(m.created_at))
                  from messages m where m.session_id = s.session_id)           as duration_s
        from sessions s where s.session_id = ?
    """, [session_id]).df().iloc[0].to_dict()
    turns = turn_durations(con, session_id)
    prompts = profile["prompts"]
    profile["turn_median_s"] = turns.median() if len(turns) else None
    profile["tokens_per_prompt"] = profile["tokens_generated"] / prompts if prompts else None
    profile["cost_per_prompt"] = profile["cost"] / prompts if prompts else None
    profile["cache_ratio"] = profile["cache_read"] / profile["context"] if profile["context"] else None
    context = context_series(con, session_id)
    # Rows published before per-message cache counts have none: context then only holds fresh input.
    has_context = len(context) and context.context.max() > 0
    profile["peak_context"] = context.context.max() if has_context else None
    profile["mean_context"] = context.context.mean() if has_context else None
    profile["compactions"] = int((context.context < context.context.shift() * 0.7).sum()) if has_context else None
    profile["tool_calls_per_prompt"] = profile["tool_calls"] / prompts if prompts else None
    profile["error_rate"] = profile["tool_errors"] / profile["tool_calls"] if profile["tool_calls"] else None
    return profile


def winner(a: object, b: object, better: str | None) -> str:
    if not better or not known(a) or not known(b):
        return ""
    if a == b:
        return "="
    return "A" if (a < b) == (better == "low") else "B"


def compare_frame(con: duckdb.DuckDBPyConnection, query: str, ids: list[str], names: list[str]) -> pd.DataFrame:
    """Run the same per-session query for both conversations, tagged by name."""
    frames = [con.execute(query, [sid]).df().assign(conversation=name) for sid, name in zip(ids, names)]
    return pd.concat(frames, ignore_index=True)


source_choice = st.sidebar.radio(
    "Source", ["all", *SOURCES], horizontal=True,
    format_func=lambda value: "Tous" if value == "all" else SOURCES[value],
)
selected_sources = list(SOURCES) if source_choice == "all" else [source_choice]
# Claude Code transcripts carry no price: its costs come from a price table.
COST_SUFFIX = " (estimé)" if "claude-code" in selected_sources else ""

tables = load_tables(ICEBERG_CATALOG_URI)
connection = connect(tables, selected_sources)
available_tables = {row[0] for row in connection.execute("show tables").fetchall()}
if "sessions" not in available_tables:
    st.error(
        "Aucune donnée dans Iceberg. Démarrez Redpanda et le sink "
        "(`docker-compose up -d redpanda sink`), puis lancez `node plugins/opencode/observatory.js` "
        "ou `node plugins/claude/observatory.js`."
    )
    st.stop()

st.title("◌ Observatory")

sessions = connection.sql("select * from sessions order by updated_at desc").df()
if sessions.empty:
    st.info("Aucune conversation pour cette source.")
    st.stop()
session_id = st.sidebar.selectbox(
    "Conversation",
    sessions.session_id,
    format_func=lambda value: (
        SOURCES[sessions.loc[sessions.session_id == value, "source"].iloc[0]].split()[0] + " "
        + one_line(sessions.loc[sessions.session_id == value, "title"].iloc[0], 60)
    ),
)
selected = sessions[sessions.session_id == session_id].iloc[0]

overview_tab, conversation_tab, stats_tab, compare_tab, raw_tab = st.tabs(
    ["🌐 Vue d'ensemble", "💬 Conversation", "📊 Statistiques", "⚖️ Comparer", "🧾 Appels outil (brut)"]
)

with overview_tab:
    totals = connection.execute("""
        select
            (select count(*) from sessions)                                as sessions,
            (select count(*) from messages)                                as messages,
            (select count(*) from tools)                                   as tools,
            (select coalesce(sum(tokens_output + tokens_reasoning), 0)
               from messages where role = 'assistant')                     as tokens,
            (select coalesce(sum(cost), 0) from sessions)                  as cost
    """).fetchone()
    top = st.columns(5)
    top[0].metric("🗂️ Sessions", fmt_count(totals[0]))
    top[1].metric("💬 Messages", fmt_count(totals[1]))
    top[2].metric("🔧 Appels outils", fmt_count(totals[2]))
    top[3].metric("🔤 Tokens (sortie + raisonnement)", fmt_count(totals[3]))
    top[4].metric(f"💰 Coût total{COST_SUFFIX}", f"${float(totals[4]):.4f}")

    daily = connection.execute("""
        select date_trunc('day', created_at)::date as day,
               count(*)                            as messages,
               coalesce(sum(cost), 0)              as cost,
               coalesce(sum(tokens_output + tokens_reasoning), 0) as tokens
        from messages
        group by 1 order by 1
    """).df()
    if len(daily):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Activité quotidienne")
            fig = go.Figure()
            fig.add_bar(x=daily.day, y=daily.messages, name="Messages", opacity=0.45)
            fig.add_scatter(x=daily.day, y=daily.tokens, name="Tokens", yaxis="y2",
                            mode="lines+markers", line=dict(color="#f97316"))
            fig.update_layout(
                legend=dict(orientation="h", y=1.15),
                yaxis=dict(title="Messages"),
                yaxis2=dict(title="Tokens", overlaying="y", side="right", showgrid=False),
            )
            chart(fig, "daily_activity")
        with col2:
            st.markdown("#### Coût cumulé")
            fig = px.area(daily, x="day", y="cost", labels={"cost": "Coût ($)", "day": ""})
            chart(fig, "daily_cost")

        if len(selected_sources) > 1:
            by_source = connection.execute("""
                select date_trunc('day', created_at)::date as day, source, count(*) as messages
                from messages group by 1, 2 order by 1
            """).df()
            st.markdown("#### Activité par source")
            fig = px.bar(by_source, x="day", y="messages", color="source",
                         labels={"messages": "Messages", "day": "", "source": "Source"})
            fig.for_each_trace(lambda trace: trace.update(name=SOURCES.get(trace.name, trace.name)))
            fig.update_layout(barmode="stack", legend=dict(orientation="h", y=1.15))
            chart(fig, "daily_by_source")

        heat = connection.execute("""
            select extract(dow from created_at)::int as dow,
                   extract(hour from created_at)::int as hour,
                   count(*) as n
            from parts
            group by 1, 2
        """).df()
        if len(heat):
            st.markdown("#### Carte d'activité (jour × heure)")
            pivot = heat.pivot_table(index="dow", columns="hour", values="n").reindex(
                index=range(7), columns=range(24), fill_value=0
            )
            fig = go.Figure(go.Heatmap(
                z=pivot.values,
                x=[f"{h:02d}h" for h in pivot.columns],
                y=[DOW_FR[d] for d in pivot.index],
                colorscale="Blues",
                hovertemplate="%{y} %{x} → %{z} événements<extra></extra>",
            ))
            chart(fig, "activity_heatmap")

    usage = connection.execute("""
        select coalesce(nullif(model, ''), 'inconnu') as model,
               count(*) as sessions,
               coalesce(sum(cost), 0) as cost
        from sessions group by 1 order by cost desc
    """).df()
    if len(usage):
        col3, col4 = st.columns(2)
        with col3:
            st.markdown("#### Sessions par modèle")
            fig = px.pie(usage, names="model", values="sessions", hole=0.55)
            fig.update_traces(textinfo="percent+label")
            chart(fig, "model_sessions")
        with col4:
            st.markdown("#### Coût par modèle")
            fig = px.bar(usage, x="model", y="cost", labels={"cost": "Coût ($)", "model": ""})
            chart(fig, "model_cost")

with conversation_tab:
    meta = [SOURCES[selected.source]]
    if selected.model:
        meta.append(f"🧩 {selected.model}")
    if selected.agent:
        meta.append(f"🎭 {selected.agent}")
    if selected.directory:
        meta.append(f"📁 {selected.directory}")
    if meta:
        st.caption(" · ".join(meta))

    top = st.columns(5)
    top[0].metric("💬 Messages", int(connection.execute("select count(*) from messages where session_id = ?", [session_id]).fetchone()[0]))
    top[1].metric("🔧 Outils", int(connection.execute("select count(*) from tools where session_id = ?", [session_id]).fetchone()[0]))
    top[2].metric("🔤 Tokens sortie", fmt_count(selected.tokens_output))
    top[3].metric("🧠 Raisonnement", fmt_count(selected.tokens_reasoning))
    cost_label = "💰 Coût (estimé)" if selected.source == "claude-code" else "💰 Coût"
    top[4].metric(cost_label, f"${float(selected.cost):.4f}")

    st.subheader(selected.title)
    messages = connection.execute("select * from messages where session_id = ? order by created_at", [session_id]).df()
    render_transcript(connection, messages)

with stats_tab:
    left, right = st.columns(2)
    with left:
        st.markdown("#### Répartition des parties")
        activity = connection.execute("""
            select type, count(*) as count from parts where session_id = ? group by type order by count desc
        """, [session_id]).df()
        if len(activity):
            fig = px.pie(activity, names="type", values="count", hole=0.55)
            fig.update_traces(textinfo="value+percent")
            chart(fig, "session_parts")
    with right:
        st.markdown("#### Outils utilisés")
        tools = connection.execute("""
            select tool, count(*) as count,
                   sum(case when status = 'error' then 1 else 0 end) as erreurs
            from tools where session_id = ? group by tool order by count desc
        """, [session_id]).df()
        if len(tools):
            fig = go.Figure()
            fig.add_bar(y=tools["tool"], x=tools["count"], name="Appels", orientation="h", opacity=0.8)
            fig.add_bar(y=tools["tool"], x=tools["erreurs"], name="Erreurs", orientation="h",
                        marker_color="#ef4444")
            fig.update_layout(barmode="overlay", legend=dict(orientation="h", y=1.15),
                              yaxis=dict(categoryorder="total ascending"))
            chart(fig, "session_tools")

    st.markdown("#### Tokens et coût cumulés (messages agent)")
    agent = messages[messages.role == "assistant"].copy() if len(messages) else messages
    if len(agent):
        agent["cum_tokens"] = agent.tokens_output.fillna(0).cumsum() + agent.tokens_reasoning.fillna(0).cumsum()
        agent["cum_cost"] = agent.cost.fillna(0).cumsum()
        col5, col6 = st.columns(2)
        with col5:
            fig = px.area(agent, x="created_at", y="cum_tokens",
                          labels={"cum_tokens": "Tokens cumulés", "created_at": ""})
            chart(fig, "session_tokens")
        with col6:
            fig = px.line(agent, x="created_at", y="cum_cost", markers=True,
                          labels={"cum_cost": "Coût cumulé ($)", "created_at": ""})
            chart(fig, "session_cost")

    st.markdown("#### Chronologie des appels outils")
    timeline = connection.execute("""
        select tool, status, started_at,
               coalesce(updated_at, started_at) as finished_at,
               epoch(coalesce(updated_at, started_at) - started_at) as duration_s
        from tools
        where session_id = ? and started_at is not null
        order by started_at
    """, [session_id]).df()
    if len(timeline):
        fig = px.timeline(
            timeline, x_start="started_at", x_end="finished_at", y="tool",
            color="status", opacity=0.85, hover_data=["duration_s"],
            labels={"tool": "", "status": "Statut", "duration_s": "Durée (s)"},
        )
        fig.update_yaxes(autorange="reversed", categoryorder="total ascending")
        chart(fig, "tool_timeline")

        stats = timeline.groupby("tool").duration_s.agg(["mean", "max", "count"]).reset_index()
        stats = stats.sort_values("mean", ascending=False)
        colA, colB = st.columns([2, 1])
        with colA:
            fig = px.bar(stats, x="tool", y="mean", labels={"mean": "Durée moyenne (s)", "tool": ""})
            chart(fig, "tool_avg_duration")
        with colB:
            st.markdown("**Détail des durées**")
            st.dataframe(
                stats.rename(columns={"mean": "moyenne (s)", "max": "max (s)", "count": "appels"})
                .round(2),
                hide_index=True, use_container_width=True,
            )
    else:
        st.caption("Aucun appel outil horodaté dans cette session.")

with compare_tab:
    # Comparisons often cross sources (same model in OpenCode and Claude Code): ignore the source filter.
    compare_connection = connection if len(selected_sources) == len(SOURCES) else connect(tables, list(SOURCES))
    candidates = compare_connection.sql("select * from sessions order by updated_at desc").df().set_index("session_id")
    if len(candidates) < 2:
        st.info("Il faut au moins deux conversations pour comparer.")
    else:
        def candidate_label(value: str) -> str:
            row = candidates.loc[value]
            model = f" · {row.model}" if row.model else ""
            return f"{SOURCES[row.source].split()[0]} {one_line(row.title, 50)}{model}"

        ids = list(candidates.index)
        first = ids.index(session_id) if session_id in ids else 0
        pick_a, pick_b = st.columns(2)
        id_a = pick_a.selectbox(f"{COMPARE_MARKS[0]} Conversation bleue", ids, index=first, format_func=candidate_label, key="compare_a")
        id_b = pick_b.selectbox(f"{COMPARE_MARKS[1]} Conversation orange", ids, index=1 if first == 0 else 0,
                                format_func=candidate_label, key="compare_b")
        if id_a == id_b:
            st.warning("Choisissez deux conversations différentes.")
        else:
            ids = [id_a, id_b]
            a, b = (session_profile(compare_connection, sid) for sid in ids)
            names = [
                f"{mark} {SOURCES[p['source']].split()[0]} {p['model'] or 'modèle inconnu'}"
                for mark, p in zip(COMPARE_MARKS, (a, b))
            ]
            colors = dict(zip(names, COMPARE_COLORS))

            rows, wins = [], {"A": [], "B": []}
            for key, label, better, fmt in COMPARE_METRICS:
                va, vb = a[key], b[key]
                won = winner(va, vb, better)
                if won in wins:
                    wins[won].append(label)
                rows.append({
                    "Métrique": label,
                    COMPARE_MARKS[0]: fmt(va) if known(va) else "—",
                    COMPARE_MARKS[1]: fmt(vb) if known(vb) else "—",
                    f"Écart {COMPARE_MARKS[1]} / {COMPARE_MARKS[0]}": f"{(vb - va) / va:+.0%}" if known(va) and known(vb) and va else "",
                    "Avantage": {"A": COMPARE_MARKS[0], "B": COMPARE_MARKS[1], "=": "="}.get(won, ""),
                })

            score = st.columns(2)
            for column, letter, name in zip(score, "AB", names):
                won = len(wins[letter])
                column.metric(name, f"{won} point{'s' if won > 1 else ''} gagné{'s' if won > 1 else ''}")
                if wins[letter]:
                    column.caption(", ".join(label.split(" ", 1)[1] for label in wins[letter]))
            st.dataframe(rows, hide_index=True, use_container_width=True)

            notes = ["Les écarts n'ont de sens qu'à tâche égale : même prompt, même dépôt."]
            if "claude-code" in (a["source"], b["source"]):
                notes.append("Claude Code : coût estimé depuis une table de prix, réflexion comptée dans les tokens générés.")
            st.caption(" ".join(notes))

            progress = compare_frame(compare_connection, """
                select minutes, tokens, cost from (
                    select role,
                           epoch(created_at - min(created_at) over ()) / 60          as minutes,
                           coalesce(tokens_output, 0) + coalesce(tokens_reasoning, 0) as tokens,
                           coalesce(cost, 0)                                          as cost
                    from messages where session_id = ?
                ) where role = 'assistant' order by minutes
            """, ids, names)
            if len(progress):
                progress["tokens"] = progress.groupby("conversation").tokens.cumsum()
                progress["cost"] = progress.groupby("conversation").cost.cumsum()
                left, right = st.columns(2)
                with left:
                    st.markdown("#### Tokens générés cumulés")
                    fig = px.line(progress, x="minutes", y="tokens", color="conversation", color_discrete_map=colors,
                                  labels={"minutes": "Minutes depuis le début", "tokens": "Tokens", "conversation": ""})
                    fig.update_layout(legend=dict(orientation="h", y=1.15))
                    chart(fig, "compare_tokens")
                with right:
                    st.markdown("#### Coût cumulé")
                    fig = px.line(progress, x="minutes", y="cost", color="conversation", color_discrete_map=colors,
                                  labels={"minutes": "Minutes depuis le début", "cost": "Coût ($)", "conversation": ""})
                    fig.update_layout(legend=dict(orientation="h", y=1.15))
                    chart(fig, "compare_cost")

            context = pd.concat([
                context_series(compare_connection, sid).assign(conversation=name) for sid, name in zip(ids, names)
            ], ignore_index=True)
            if len(context) and context.context.max() > 0:
                st.markdown("#### Taille du contexte à chaque appel")
                fig = px.line(context, x="call", y="context", color="conversation", color_discrete_map=colors,
                              hover_data={"cache_share": ":.0%"},
                              labels={"call": "Appel n° (agent principal)", "context": "Tokens de contexte",
                                      "cache_share": "Part en cache", "conversation": ""})
                fig.update_layout(legend=dict(orientation="h", y=1.15))
                chart(fig, "compare_context")
                st.caption("Une chute marque une compaction ou un nouveau contexte. "
                           "Les conversations publiées avant l'ajout du cache par message n'affichent pas de courbe : "
                           "republiez-les avec `--full`.")

            turns = pd.concat([
                turn_durations(compare_connection, sid).rename("seconds").reset_index().assign(conversation=name)
                for sid, name in zip(ids, names)
            ], ignore_index=True)
            if len(turns):
                st.markdown("#### Durée de chaque tour")
                fig = px.bar(turns, x="turn", y="seconds", color="conversation", barmode="group",
                             color_discrete_map=colors,
                             labels={"turn": "Tour (prompt n°)", "seconds": "Durée (s)", "conversation": ""})
                fig.update_layout(legend=dict(orientation="h", y=1.15))
                chart(fig, "compare_turns")

            left, right = st.columns(2)
            with left:
                st.markdown("#### Outils utilisés")
                tool_use = compare_frame(compare_connection, """
                    select tool, count(*) as calls,
                           sum(case when status = 'error' then 1 else 0 end) as errors,
                           coalesce(sum(length(output)), 0) as output_chars
                    from tools where session_id = ? group by tool
                """, ids, names)
                if len(tool_use):
                    fig = px.bar(tool_use, y="tool", x="calls", color="conversation", barmode="group",
                                 orientation="h", color_discrete_map=colors, hover_data=["errors", "output_chars"],
                                 labels={"tool": "", "calls": "Appels", "errors": "Erreurs",
                                         "output_chars": "Caractères renvoyés", "conversation": ""})
                    fig.update_layout(legend=dict(orientation="h", y=1.15), yaxis=dict(categoryorder="total ascending"))
                    chart(fig, "compare_tools")
                else:
                    st.caption("Aucun appel outil.")
            with right:
                st.markdown("#### Répartition des parties")
                part_mix = compare_frame(compare_connection, """
                    select type, count(*) as count from parts where session_id = ? group by type
                """, ids, names)
                if len(part_mix):
                    fig = px.bar(part_mix, x="type", y="count", color="conversation", barmode="group",
                                 color_discrete_map=colors, labels={"type": "", "count": "Parties", "conversation": ""})
                    fig.update_layout(legend=dict(orientation="h", y=1.15))
                    chart(fig, "compare_parts")

            if st.toggle("Afficher les deux transcripts", key="compare_transcripts"):
                for transcript_tab, sid in zip(st.tabs(names), ids):
                    with transcript_tab:
                        st.subheader(candidates.loc[sid, "title"])
                        render_transcript(compare_connection, compare_connection.execute(
                            "select * from messages where session_id = ? order by created_at", [sid]
                        ).df())

with raw_tab:
    st.dataframe(
        connection.execute("""
            select created_at as "Date", tool as "Outil", status as "Statut",
                   input as "Entrée", output as "Sortie"
            from tools where session_id = ? order by created_at
        """, [session_id]).df(),
        hide_index=True,
        column_config={
            "Entrée": st.column_config.TextColumn(width="medium"),
            "Sortie": st.column_config.TextColumn(width="large"),
        },
    )
