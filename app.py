from __future__ import annotations

import json
import os

import duckdb
import pyarrow as pa
import streamlit as st
from pyiceberg.catalog import load_catalog

st.set_page_config(page_title="OpenCode Observatory", page_icon="◌", layout="wide")

ICEBERG_CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
ICEBERG_NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "analytics")
TABLES = ("sessions", "messages", "parts", "tools")
PRIMARY_KEYS = {"sessions": "session_id", "messages": "message_id", "parts": "part_id", "tools": "part_id"}

TOOL_ICONS = {
    "read": "📖", "write": "✍️", "edit": "🖊️", "multi_edit": "🖊️",
    "bash": "💻", "shell": "💻", "exec": "💻",
    "grep": "🔎", "glob": "📂", "ls": "🗂️", "search": "🔎",
    "task": "🤖", "subtask": "🤖",
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
# Keys that best summarise what a tool call was about, in order of preference.
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
            tables[name] = catalog.load_table(f"{ICEBERG_NAMESPACE}.{name}").scan().to_arrow()
        except Exception:
            pass
    return tables


def dedup_view_sql(name: str) -> str:
    """The sink appends events; keep only the latest version of each row."""
    key = PRIMARY_KEYS[name]
    return f"""
        create or replace view {name} as
        select * exclude (_version) from (
            select *, row_number() over (
                partition by {key} order by updated_at desc
            ) as _version
            from raw_{name}
        )
        where _version = 1
    """


def connect(tables: dict[str, pa.Table]) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    for name, table in tables.items():
        connection.register(f"raw_{name}", table)
        connection.execute(dedup_view_sql(name))
    return connection


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
    icon = TOOL_ICONS.get(tool, "🔧")
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


tables = load_tables(ICEBERG_CATALOG_URI)
connection = connect(tables)
available_tables = {row[0] for row in connection.execute("show tables").fetchall()}
if "sessions" not in available_tables:
    st.error(
        "Aucune donnée dans Iceberg. Démarrez Redpanda et le sink "
        "(`docker-compose up -d redpanda sink`), puis lancez `./opencode-analytics.sh sync`."
    )
    st.stop()

st.title("◌ OpenCode Observatory")

sessions = connection.sql("select * from sessions order by updated_at desc").df()
session_id = st.sidebar.selectbox(
    "Conversation",
    sessions.session_id,
    format_func=lambda value: one_line(sessions.loc[sessions.session_id == value, "title"].iloc[0], 60),
)
selected = sessions[sessions.session_id == session_id].iloc[0]

meta = []
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
top[4].metric("💰 Coût", f"${float(selected.cost):.4f}")

conversation_tab, stats_tab, raw_tab = st.tabs(["💬 Conversation", "📊 Statistiques", "🧾 Appels outil (brut)"])

with conversation_tab:
    st.subheader(selected.title)
    messages = connection.execute("select * from messages where session_id = ? order by created_at", [session_id]).df()
    for _, message in messages.iterrows():
        parts = connection.execute("""
            select p.type, p.text, t.tool, t.status, t.input, t.output
            from parts p
            left join tools t on t.part_id = p.part_id
            where p.message_id = ?
            order by p.created_at
        """, [message.message_id]).df()
        render_message(message, parts)

with stats_tab:
    left, right = st.columns(2)
    with left:
        st.markdown("#### Répartition des parties")
        activity = connection.execute("""
            select type, count(*) as count from parts where session_id = ? group by type order by count desc
        """, [session_id]).df()
        st.bar_chart(activity.set_index("type"), height=320)
    with right:
        st.markdown("#### Outils utilisés")
        tools = connection.execute("""
            select tool, count(*) as count,
                   sum(case when status = 'error' then 1 else 0 end) as erreurs
            from tools where session_id = ? group by tool order by count desc
        """, [session_id]).df()
        tools["outil"] = tools.tool.map(lambda t: f"{TOOL_ICONS.get(t, '🔧')} {t}")
        st.bar_chart(tools.set_index("outil")[["count", "erreurs"]], height=320)

    st.markdown("#### Activité dans le temps")
    timeline = connection.execute("""
        select date_trunc('minute', created_at) as minute, role, count(*) as messages
        from messages where session_id = ? group by minute, role order by minute
    """, [session_id]).df()
    if len(timeline):
        st.area_chart(timeline.pivot(index="minute", columns="role", values="messages").fillna(0), height=260)

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
