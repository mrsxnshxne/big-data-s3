"""Register the Arroyo pipelines found next to this script (``arroyo/*.sql``).

Each file becomes one pipeline named after the file. A pipeline that already
exists is left running, unless ``--replace`` is given (it is then stopped,
deleted and created again from the current SQL).

    python arroyo/deploy.py [--replace]
"""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

API = os.getenv("ARROYO_URL", "http://localhost:5115").rstrip("/") + "/api/v1"
CHECKPOINT_INTERVAL_MICROS = int(float(os.getenv("ARROYO_CHECKPOINT_SECONDS", "10")) * 1_000_000)


def call(method: str, path: str, body: dict | None = None) -> dict:
    request = urllib.request.Request(
        API + path,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = response.read()
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"{method} {path} -> HTTP {error.code}: {error.read().decode()[:500]}") from None
    return json.loads(payload) if payload.strip().startswith((b"{", b"[")) else {}


def wait_for_arroyo(timeout: float = 120) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            call("GET", "/ping")
            return
        except (RuntimeError, OSError):
            if time.monotonic() > deadline:
                raise SystemExit(f"Arroyo unreachable at {API} after {timeout:.0f}s")
            time.sleep(2)


def existing_pipelines() -> dict[str, str]:
    return {p["name"]: p["id"] for p in call("GET", "/pipelines")["data"]}


def remove(pipeline_id: str) -> None:
    call("PATCH", f"/pipelines/{pipeline_id}", {"stop": "immediate"})
    for _ in range(30):
        try:
            call("DELETE", f"/pipelines/{pipeline_id}")
            return
        except RuntimeError:  # still stopping
            time.sleep(1)
    raise RuntimeError(f"pipeline {pipeline_id} did not stop")


def deploy(replace: bool) -> None:
    wait_for_arroyo()
    present = existing_pipelines()
    for path in sorted(Path(__file__).resolve().parent.glob("*.sql")):
        query = path.read_text()
        name = path.stem
        checked = call("POST", "/pipelines/validate_query", {"query": query, "udfs": []})
        if checked.get("errors"):
            raise SystemExit(f"{path.name}: {'; '.join(checked['errors'])}")
        if name in present:
            if not replace:
                print(f"{name}: already deployed ({present[name]}), use --replace to redeploy", flush=True)
                continue
            remove(present[name])
        created = call("POST", "/pipelines", {
            "name": name,
            "query": query,
            "udfs": [],
            "parallelism": 1,
            "checkpointIntervalMicros": CHECKPOINT_INTERVAL_MICROS,
        })
        print(f"{name}: deployed as {created['id']}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--replace", action="store_true", help="redeploy pipelines that already exist")
    deploy(parser.parse_args().replace)
