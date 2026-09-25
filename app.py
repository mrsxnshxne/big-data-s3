from __future__ import annotations

import json
import os

import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
import streamlit as st

st.set_page_config(page_title="OpenCode Observatory", page_icon="◌", layout="wide")

ICEBERG_CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
ICEBERG_NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "analytics")
TABLES = ("sessions", "messages", "parts", "tools")
PRIMARY_KEYS = {"sessions": "session_id", "messages": "message_id", "parts": "part_id", "tools": "part_id"}


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


def render_tool(tool: object, status: object, tool_input: object, output: object) -> None:
    """Render a tool call without hiding its structured input or result."""
    title = f"{tool} · {status or 'statut inconnu'}"
    with st.expander(title, expanded=False):
        st.markdown("**Entrée**")
        try:
            st.json(json.loads(tool_input or "{}"))
        except (TypeError, json.JSONDecodeError):
            st.code(str(tool_input or ""), language=None)
        st.markdown("**Sortie**")
        if output:
            try:
                st.json(json.loads(output))
            except (TypeError, json.JSONDecodeError):
                st.code(str(output), language=None)
        else:
            st.caption("Aucune sortie enregistrée")


tables = load_tables(ICEBERG_CATALOG_URI)
connection = connect(tables)
available_tables = {row[0] for row in connection.execute("show tables").fetchall()}
if "sessions" not in available_tables:
    st.error(
        "Aucune donnée dans Iceberg. Démarrez Redpanda et le sink "
        "(`docker-compose up -d redpanda sink`), puis lancez `./opencode-analytics.sh sync`."
    )
    st.stop()

st.title("OpenCode Observatory")

sessions = connection.sql("select * from sessions order by updated_at desc").df()
session_id = st.sidebar.selectbox("Conversation", sessions.session_id, format_func=lambda value: sessions.loc[sessions.session_id == value, "title"].iloc[0])
selected = sessions[sessions.session_id == session_id].iloc[0]

top = st.columns(5)
top[0].metric("Messages", int(connection.execute("select count(*) from messages where session_id = ?", [session_id]).fetchone()[0]))
top[1].metric("Outils", int(connection.execute("select count(*) from tools where session_id = ?", [session_id]).fetchone()[0]))
top[2].metric("Tokens sortie", f"{int(selected.tokens_output):,}")
top[3].metric("Raisonnement", f"{int(selected.tokens_reasoning):,}")
top[4].metric("Coût", f"${float(selected.cost):.4f}")

st.subheader(selected.title)

left, right = st.columns([1, 1])
with left:
    st.markdown("#### Activité")
    activity = connection.execute("""
        select type, count(*) as count from parts where session_id = ? group by type order by count desc
    """, [session_id]).df()
    st.bar_chart(activity.set_index("type"))
with right:
    st.markdown("#### Outils utilisés")
    tools = connection.execute("select tool, count(*) as count from tools where session_id = ? group by tool order by count desc", [session_id]).df()
    st.dataframe(tools, hide_index=True, use_container_width=True)

st.markdown("#### Fil de la conversation")
messages = connection.execute("select * from messages where session_id = ? order by created_at", [session_id]).df()
for _, message in messages.iterrows():
    role = "Utilisateur" if message.role == "user" else "Agent"
    with st.expander(f"{role} · {message.created_at:%Y-%m-%d %H:%M:%S} · {message.message_id}", expanded=False):
        parts = connection.execute("""
            select p.type, p.text, t.tool, t.status, t.input, t.output
            from parts p
            left join tools t on t.part_id = p.part_id
            where p.message_id = ?
            order by p.created_at
        """, [message.message_id]).df()
        for _, part in parts.iterrows():
            if part.type == "tool":
                render_tool(part.tool, part.status, part.input, part.output)
            elif part.text:
                label = {"reasoning": "Réflexion", "text": "Message"}.get(part.type, part.type)
                st.markdown(f"**{label}**")
                st.code(part.text, language=None)

st.markdown("#### Détails des appels outil")
st.dataframe(connection.execute("select tool, status, input, output, created_at from tools where session_id = ? order by created_at", [session_id]).df(), hide_index=True, use_container_width=True)
