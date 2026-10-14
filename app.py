from __future__ import annotations

import json
import os

import duckdb
import plotly.express as px
import plotly.graph_objects as go
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
PLOTLY_MARGIN = dict(l=40, r=20, t=10, b=30)
DOW_FR = ["Dim", "Lun", "Mar", "Mer", "Jeu", "Ven", "Sam"]
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
        "(`docker-compose up -d redpanda sink`), puis lancez `node opencode-plugin/observatory.js`."
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

overview_tab, conversation_tab, stats_tab, raw_tab = st.tabs(
    ["🌐 Vue d'ensemble", "💬 Conversation", "📊 Statistiques", "🧾 Appels outil (brut)"]
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
    top[4].metric("💰 Coût total", f"${float(totals[4]):.4f}")

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
