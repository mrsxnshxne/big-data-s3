from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import boto3
import duckdb
import streamlit as st

st.set_page_config(page_title="OpenCode Observatory", page_icon="◌", layout="wide")

DATA_DIR = Path(os.getenv("DATA_DIR", "data/parquet"))
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_PREFIX = os.getenv("S3_PREFIX", "parquet").strip("/")


@st.cache_data(ttl=30)
def data_dir() -> Path:
    """Use local Parquet in development, or refresh a small S3 cache remotely."""
    if not S3_BUCKET:
        return DATA_DIR
    target = Path(tempfile.gettempdir()) / "opencode-analytics-parquet"
    target.mkdir(parents=True, exist_ok=True)
    client = boto3.client(
        "s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "rustfsadmin"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "rustfsadmin"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )
    response = client.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"{S3_PREFIX}/")
    for item in response.get("Contents", []):
        name = Path(item["Key"]).name
        if name.endswith(".parquet"):
            client.download_file(S3_BUCKET, item["Key"], str(target / name))
    return target


def parquet_signature(directory: Path) -> tuple[tuple[str, int, int] | tuple[str, None, None], ...]:
    return tuple(
        (
            name,
            path.stat().st_mtime_ns,
            path.stat().st_size,
        )
        if path.exists()
        else (name, None, None)
        for name in ("sessions", "messages", "parts", "tools")
        for path in [directory / f"{name}.parquet"]
    )


@st.cache_resource
def db(directory: Path, signature: tuple[tuple[str, int, int] | tuple[str, None, None], ...]) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    for name in ("sessions", "messages", "parts", "tools"):
        path = directory / f"{name}.parquet"
        if path.exists():
            escaped_path = str(path).replace("'", "''")
            connection.execute(f"create or replace view {name} as select * from read_parquet('{escaped_path}')")
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


directory = data_dir()
connection = db(directory, parquet_signature(directory))
if not (directory / "sessions.parquet").exists():
    st.error("Aucune donnée Parquet. Lancez `python ingest.py --upload` puis rechargez la page.")
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
