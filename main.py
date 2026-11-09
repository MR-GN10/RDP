"""HITEK / ICMR Data Gateway.

FastAPI + DuckDB over the public HuggingFace bucket:
    CutehackX/icrm-hitek-full-db-mixed-bucket

The bucket contains prebuilt Parquet indexes sorted by phoneNumber and
aadharNumber.  The app works in two modes:

remote (default when no local files are present)
    Streams the sorted index parts straight from HuggingFace.  No 227 GB
    download is required, but the first lookup that touches a new row group
    is network-bound (roughly 30-90 seconds).  Results are cached.

local
    If idx_phone*.parquet / idx_aadhar*.parquet / part*.parquet exist in
    ICMR_DATA_DIR (or next to this file), DuckDB queries those files instead.

Endpoints
---------
GET  /                       landing page + search box
GET  /FetchData?Number=...   original phone lookup endpoint
GET  /search?q=...           unified search (phone / aadhar / text)
POST /search/parallel        batch search
GET  /health                 status
GET  /docs                   Swagger UI
"""

from __future__ import annotations

import glob
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import duckdb
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR = os.environ.get("ICMR_DATA_DIR") or (
    os.path.join(BASE, "data") if os.path.isdir(os.path.join(BASE, "data")) else BASE
)

BUCKET = os.environ.get("ICMR_BUCKET", "CutehackX/icrm-hitek-full-db-mixed-bucket")
BUCKET_BASE = f"https://huggingface.co/buckets/{BUCKET}/resolve"

PARALLELISM = int(os.environ.get("ICMR_PARALLEL", "8"))
THREADS_PER_CONN = int(os.environ.get("ICMR_THREADS_PER_CONN", "8"))
DUPLICATE_CAP = 2  # max 2 copies of the same person in results
CACHE_TTL = int(os.environ.get("ICMR_CACHE_TTL", "600"))  # seconds
# DuckDB's httpfs `http_timeout` is in SECONDS (not milliseconds).  Values
# above ~2,147,483 overflow its internal millisecond conversion, which makes
# every request fail immediately with "Timeout was reached error for HTTP
# HEAD".  Keep it in a sane range (1 hour by default).
HTTP_TIMEOUT_S = int(os.environ.get("ICMR_HTTP_TIMEOUT_S", "3600"))
HTTP_TIMEOUT_S = max(1, min(HTTP_TIMEOUT_S, 2147483))
HTTP_RETRIES = int(os.environ.get("ICMR_HTTP_RETRIES", "3"))
ALLOW_REMOTE_TEXT_SCAN = os.environ.get("ICMR_ALLOW_REMOTE_TEXT_SCAN", "0") == "1"
ALLOW_REMOTE_OTHER_SCAN = os.environ.get("ICMR_ALLOW_REMOTE_OTHER_SCAN", "0") == "1"

SEARCH_FIELDS = [
    "name", "fathersName", "phoneNumber", "aadharNumber", "otherNumber",
    "address", "district", "pincode", "state", "town", "source",
]
NUMBER_FIELDS = ["phoneNumber", "aadharNumber", "otherNumber"]
TEXT_FIELDS = [f for f in SEARCH_FIELDS if f not in NUMBER_FIELDS]

PHONE_PARTS = 7
AADHAR_PARTS = 7

# min/max phoneNumber per sorted part, read from Parquet row-group statistics.
# None means "unbounded on that side".
_PHONE_RANGES: list[tuple[int | None, int | None]] = [
    (None, 7310463827),
    (7310463827, 8077394733),
    (8077394734, 8780538380),
    (8780538381, 9313336069),
    (9313336069, 9703053944),
    (9703053945, 9990701360),
    (9990701361, None),
]

# aadharNumber ranges for the clean parts (0-3).  Parts 4-6 contain some
# malformed values in the published index, so values above the part-3 max are
# routed to parts 4-6 directly.
_AADHAR_RANGES: list[tuple[int | None, int | None]] = [
    (None, 351104219878),
    (351104219878, 549490723501),
    (549490723501, 746293615015),
    (746293615015, 944410175347),
]
_AADHAR_TAIL_START = 944410175348  # values >= this go to parts 4,5,6


def _bucket_url(name: str) -> str:
    return f"{BUCKET_BASE}/{name}?download=true"


_REMOTE_PHONE = [_bucket_url(f"idx_phone.{i}.parquet") for i in range(PHONE_PARTS)]
_REMOTE_AADHAR = [_bucket_url(f"idx_aadhar.{i}.parquet") for i in range(AADHAR_PARTS)]


# ---------------------------------------------------------------------------
# Local / remote discovery
# ---------------------------------------------------------------------------

def _local_parts(prefix: str) -> list[str]:
    """Return local index parts for `prefix` (e.g. idx_phone.0.parquet)."""
    single = os.path.join(_DATA_DIR, f"{prefix}.parquet")
    parts = sorted(glob.glob(os.path.join(_DATA_DIR, f"{prefix}.*.parquet")))
    parts = [p for p in parts if os.path.isfile(p) and os.path.getsize(p) > 0]
    if parts:
        return parts
    if os.path.isfile(single) and os.path.getsize(single) > 0:
        return [single]
    return []


def _local_raw() -> list[str]:
    names = ["part1.parquet", "part2a.parquet", "part2b_new.parquet"]
    out = []
    for n in names:
        p = os.path.join(_DATA_DIR, n)
        if os.path.isfile(p) and os.path.getsize(p) > 0:
            out.append(p)
    return out


_LOCAL_PHONE = _local_parts("idx_phone")
_LOCAL_AADHAR = _local_parts("idx_aadhar")
_LOCAL_RAW = _local_raw()

# A full-width local scan source (raw parquet if present, otherwise the phone
# index, which still has all 11 columns).
_LOCAL_SCAN_FILES = _LOCAL_RAW or _LOCAL_PHONE
MODE = "local" if (_LOCAL_PHONE or _LOCAL_AADHAR or _LOCAL_RAW) else "remote"

# ---------------------------------------------------------------------------
# DuckDB connection pool (one connection per worker thread)
# ---------------------------------------------------------------------------

_thread_local = threading.local()
_conns: list[duckdb.DuckDBPyConnection] = []
_conns_lock = threading.Lock()


def _new_conn() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()  # in-memory catalog
    con.execute(f"SET threads = {THREADS_PER_CONN}")
    con.execute("SET enable_progress_bar = false")
    if MODE == "remote":
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"SET http_timeout = {HTTP_TIMEOUT_S}")
        con.execute(f"SET http_retries = {HTTP_RETRIES}")
    if _LOCAL_SCAN_FILES:
        files = ", ".join(_sql_literal(p) for p in _LOCAL_SCAN_FILES)
        con.execute(f"CREATE VIEW people AS SELECT * FROM read_parquet([{files}])")
    return con


def _get_conn() -> duckdb.DuckDBPyConnection:
    ident = getattr(_thread_local, "id", None)
    if ident is None:
        with _conns_lock:
            ident = len(_conns)
            _thread_local.id = ident
    with _conns_lock:
        while len(_conns) <= ident:
            _conns.append(_new_conn())
    return _conns[ident]


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _parquet_expr(paths: list[str]) -> str:
    if not paths:
        raise ValueError("No Parquet files are available")
    return "read_parquet([" + ", ".join(_sql_literal(p) for p in paths) + "])"


def _source_value(source: str | None) -> str | None:
    if source == "hitek":
        return "inddata"
    if source in ("icmr", "inddata"):
        return source
    return None


def _source_clause(source: str | None) -> str:
    src = _source_value(source)
    if src:
        return f" AND source = {_sql_literal(src)}"
    return ""


def _run(sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    con = _get_conn()
    cur = con.execute(sql, params or [])
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _person_key(row: dict[str, Any]) -> tuple[str, ...]:
    ph = (row.get("phoneNumber") or "").strip()
    ad = (row.get("aadharNumber") or "").strip()
    if ph or ad:
        return (ph, ad)
    return (row.get("name") or "").strip(), (row.get("fathersName") or "").strip()


def _cap_duplicates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep at most DUPLICATE_CAP rows per person (same phone+aadhar identity)."""
    seen: dict[tuple[str, ...], int] = {}
    out: list[dict[str, Any]] = []
    for r in rows:
        k = _person_key(r)
        n = seen.get(k, 0)
        if n < DUPLICATE_CAP:
            seen[k] = n + 1
            out.append(r)
    return out


def _pretty(data: dict[str, Any], pretty: bool) -> Response:
    body = json.dumps(data, indent=2 if pretty else None, ensure_ascii=False, default=str)
    return Response(content=body, media_type="application/json")


# ---------------------------------------------------------------------------
# Result cache
# ---------------------------------------------------------------------------

_cache: dict[tuple[Any, ...], tuple[float, dict[str, Any]]] = {}
_cache_lock = threading.Lock()


def _cache_get(key: tuple[Any, ...]) -> dict[str, Any] | None:
    if CACHE_TTL <= 0:
        return None
    now = time.time()
    with _cache_lock:
        item = _cache.get(key)
        if item and now - item[0] < CACHE_TTL:
            return item[1]
        if item:
            _cache.pop(key, None)
    return None


def _cache_set(key: tuple[Any, ...], value: dict[str, Any]) -> None:
    if CACHE_TTL <= 0:
        return
    with _cache_lock:
        # keep the cache bounded
        if len(_cache) > 512:
            oldest = sorted(_cache.items(), key=lambda kv: kv[1][0])[:128]
            for k, _ in oldest:
                _cache.pop(k, None)
        _cache[key] = (time.time(), value)


# ---------------------------------------------------------------------------
# Index routing
# ---------------------------------------------------------------------------

def _phone_candidates(value: str) -> list[int]:
    if not value.isdigit():
        return list(range(PHONE_PARTS))
    n = int(value)
    out = []
    for i, (lo, hi) in enumerate(_PHONE_RANGES):
        if (lo is None or n >= lo) and (hi is None or n <= hi):
            out.append(i)
    return out or list(range(PHONE_PARTS))


def _aadhar_candidates(value: str) -> list[int]:
    if not value.isdigit():
        return list(range(AADHAR_PARTS))
    n = int(value)
    if n >= _AADHAR_TAIL_START:
        return [4, 5, 6]
    for i, (lo, hi) in enumerate(_AADHAR_RANGES):
        if (lo is None or n >= lo) and n <= hi:
            return [i]
    return [0, 1, 2, 3]


def _index_paths(kind: str, value: str) -> list[str]:
    if kind == "phone":
        if _LOCAL_PHONE:
            return _LOCAL_PHONE
        return [_REMOTE_PHONE[i] for i in _phone_candidates(value)]
    if kind == "aadhar":
        if _LOCAL_AADHAR:
            return _LOCAL_AADHAR
        return [_REMOTE_AADHAR[i] for i in _aadhar_candidates(value)]
    raise ValueError(f"Unknown index: {kind}")


def _exact(field: str, value: str, paths: list[str], limit: int,
           source: str | None = None) -> list[dict[str, Any]]:
    # Values are inlined (not bound as ? parameters) so DuckDB can push the
    # predicate into the Parquet scan and use row-group zone maps.  With a
    # prepared-statement parameter DuckDB scans every row group.
    sql = (f"SELECT * FROM {_parquet_expr(paths)} "
           f"WHERE {field} = {_sql_literal(value)}")
    sql += _source_clause(source)
    sql += f" LIMIT {int(limit)}"
    return _run(sql)


def _query_index(kind: str, value: str, limit: int,
                 source: str | None = None) -> dict[str, Any]:
    """Exact lookup on the phone/aadhar sorted index."""
    field = "phoneNumber" if kind == "phone" else "aadharNumber"
    paths = _index_paths(kind, value)
    fetch_limit = limit * DUPLICATE_CAP + 20
    try:
        rows = _exact(field, value, paths, fetch_limit, source)
    except Exception as exc:
        # Only retry the remaining parts when the selected part is actually
        # missing.  Do not retry on timeouts: that would multiply the wait.
        msg = str(exc).lower()
        missing_part = any(s in msg for s in ("404", "not found", "no files"))
        all_remote = _REMOTE_PHONE if kind == "phone" else _REMOTE_AADHAR
        if MODE == "remote" and missing_part and len(paths) < len(all_remote):
            rows = _exact(field, value, all_remote, fetch_limit, source)
            paths = all_remote
        else:
            raise
    capped = _cap_duplicates(rows)[:limit]
    return {
        "results": capped,
        "raw_count": len(rows),
        "parts_queried": len(paths),
        "index": f"idx_{kind}",
    }


def _query_other(value: str, limit: int,
                 source: str | None = None) -> dict[str, Any]:
    """otherNumber has no prebuilt index.  Only scan locally (or opt-in remote)."""
    if _LOCAL_SCAN_FILES:
        sql = (f"SELECT * FROM {_parquet_expr(_LOCAL_SCAN_FILES)} "
               f"WHERE otherNumber = {_sql_literal(value)}")
        sql += _source_clause(source)
        sql += f" LIMIT {int(limit * DUPLICATE_CAP + 20)}"
        rows = _run(sql)
        return {
            "results": _cap_duplicates(rows)[:limit],
            "raw_count": len(rows),
            "parts_queried": len(_LOCAL_SCAN_FILES),
            "index": "local-scan",
        }
    if ALLOW_REMOTE_OTHER_SCAN:
        rows = _exact("otherNumber", value, _REMOTE_PHONE,
                      limit * DUPLICATE_CAP + 20, source)
        return {
            "results": _cap_duplicates(rows)[:limit],
            "raw_count": len(rows),
            "parts_queried": len(_REMOTE_PHONE),
            "index": "remote-scan",
        }
    return {"results": [], "raw_count": 0, "parts_queried": 0,
            "index": None, "skipped": "otherNumber has no index in remote mode"}


def _query_text(value: str, limit: int, source: str | None = None,
                field: str | None = None, mode: str = "contains") -> dict[str, Any]:
    """Text search over local files.  Remote full scans are not viable."""
    if not _LOCAL_SCAN_FILES:
        raise HTTPException(
            503,
            "Text search needs local data. Run setup.bat/setup.sh (or point "
            "ICMR_DATA_DIR at the downloaded index parts). Phone/aadhar lookups "
            "work in remote mode.",
        )
    target = field or "name"
    if target not in SEARCH_FIELDS:
        raise HTTPException(400, f"Unknown field: {target}")
    if mode == "exact":
        sql = (f"SELECT * FROM {_parquet_expr(_LOCAL_SCAN_FILES)} "
               f"WHERE {target} = {_sql_literal(value)}")
    else:
        escaped = value.replace("\\", "\\\\").replace("%", r"\%").replace("_", r"\_")
        sql = (f"SELECT * FROM {_parquet_expr(_LOCAL_SCAN_FILES)} "
               f"WHERE {target} ILIKE {_sql_literal('%' + escaped + '%')} ESCAPE '\\'")
    sql += _source_clause(source)
    sql += f" LIMIT {int(limit * DUPLICATE_CAP + 20)}"
    rows = _run(sql)
    return {
        "results": _cap_duplicates(rows)[:limit],
        "raw_count": len(rows),
        "parts_queried": len(_LOCAL_SCAN_FILES),
        "index": "local-scan",
    }


# ---------------------------------------------------------------------------
# Unified search
# ---------------------------------------------------------------------------

def _phone_variants(value: str) -> list[str]:
    variants = [value]
    if len(value) == 12 and value.startswith("91"):
        variants.append(value[2:])
    if len(value) == 11 and value.startswith("0"):
        variants.append(value[1:])
    return list(dict.fromkeys(variants))


def _search_one(q: str, field: str | None = None, mode: str = "contains",
                limit: int = 10, source: str | None = None) -> dict[str, Any]:
    q = (q or "").strip()
    if not q:
        raise HTTPException(400, "q must not be empty")

    if field:
        if field not in SEARCH_FIELDS:
            raise HTTPException(400, f"Unknown field: {field}")
        if mode == "exact" and field in ("phoneNumber", "aadharNumber"):
            kind = "phone" if field == "phoneNumber" else "aadhar"
            data = _query_index(kind, q, limit, source)
        elif mode == "exact" and field == "otherNumber":
            data = _query_other(q, limit, source)
        else:
            data = _query_text(q, limit, source, field=field, mode=mode)
        return {
            "query": q, "field": field, "mode": mode, "dedup": DUPLICATE_CAP,
            "count": len(data["results"]), "results": data["results"],
            "raw_count": data.get("raw_count", len(data["results"])),
        }

    # Numeric input -> indexed exact lookups.  Accept +91, spaces, dashes,
    # dots and parentheses around a phone number.
    digits = re.sub(r"\D", "", q)
    numeric_like = bool(re.fullmatch(r"[\d\s+\-().]+", q))
    if digits and len(digits) >= 8 and numeric_like:
        searched: list[str] = []
        phone_data = {"results": [], "raw_count": 0}
        for v in _phone_variants(digits):
            phone_data = _query_index("phone", v, limit, source)
            if phone_data["results"]:
                break
        if phone_data["results"]:
            searched.append("phoneNumber")

        aadhar_data = {"results": [], "raw_count": 0}
        if len(digits) >= 12:
            # Do not strip a country-code prefix from a 12-digit Aadhar.
            aadhar_data = _query_index("aadhar", digits, limit, source)
            if aadhar_data["results"]:
                searched.append("aadharNumber")

        other_data = {"results": [], "raw_count": 0}
        if not phone_data["results"]:
            other_data = _query_other(digits, limit, source)
            if other_data["results"]:
                searched.append("otherNumber")

        all_rows = _cap_duplicates(
            phone_data["results"] + aadhar_data["results"] + other_data["results"]
        )[:limit]
        return {
            "query": q, "field": None, "mode": "exact", "dedup": DUPLICATE_CAP,
            "count": len(all_rows), "results": all_rows,
            "searched_fields": searched or ["phoneNumber"],
            "raw_count": phone_data["raw_count"] + aadhar_data["raw_count"]
                         + other_data["raw_count"],
        }

    # Text input.
    data = _query_text(q, limit, source, field="name", mode="contains")
    rows = data["results"]
    if not rows and _LOCAL_SCAN_FILES:
        father = _query_text(q, limit, source, field="fathersName", mode="contains")
        rows = _cap_duplicates(rows + father["results"])[:limit]
    return {
        "query": q, "field": None, "mode": "contains", "dedup": DUPLICATE_CAP,
        "count": len(rows), "results": rows,
        "searched_fields": ["name", "fathersName"],
        "raw_count": data["raw_count"],
    }


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="HITEK + ICMR Data Gateway",
    description=(
        "DuckDB-backed lookup over the ICMR + HITEK full DB (2.5B rows, 11 columns). "
        "Phone and Aadhar lookups use the prebuilt sorted indexes. "
        "Duplicates are capped at 2 per person."
    ),
)


@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        return JSONResponse(
            status_code=404,
            content={
                "status": "rejected",
                "message": "Invalid endpoint. Use /FetchData?Number=XXXXXXXXXX or /search?q=...",
                "Developer": "cybern1lux",
            },
        )
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail, "Developer": "cybern1lux"},
    )


LANDING_PAGE_HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HITEK Data Gateway - LIVE</title>
    <style>
        body { margin: 0; overflow: hidden; background-color: #050505; color: #00ffcc; font-family: 'Courier New', Courier, monospace; }
        #canvas-container { position: fixed; inset: 0; z-index: -1; }
        .overlay {
            position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%);
            width: min(900px, 92vw); max-height: 90vh; overflow: auto;
            text-align: center; background: rgba(10, 10, 10, 0.9); padding: 32px;
            border: 1px solid #00ffcc; border-radius: 12px;
            box-shadow: 0 0 30px rgba(0, 255, 204, 0.3); backdrop-filter: blur(5px);
        }
        h1 { margin: 0 0 10px 0; font-size: 2.4em; text-transform: uppercase; letter-spacing: 4px; text-shadow: 0 0 15px #00ffcc; }
        p { font-size: 1em; margin: 6px 0; color: #ccc; }
        .highlight { color: #00ffcc; font-weight: bold; }
        .status-box {
            margin: 16px 0; font-weight: bold; padding: 10px; border-radius: 8px;
            background: rgba(0, 255, 204, 0.1); border: 1px solid rgba(0, 255, 204, 0.5);
            font-size: 0.95em;
        }
        .blinking { animation: blinker 1.5s linear infinite; display: inline-block; }
        @keyframes blinker { 50% { opacity: 0; } }
        form { display: flex; gap: 8px; justify-content: center; margin: 18px 0 8px; flex-wrap: wrap; }
        input {
            flex: 1 1 260px; min-width: 220px; padding: 12px 14px; border-radius: 8px;
            border: 1px solid #00ffcc; background: #07110f; color: #00ffcc;
            font: inherit; font-size: 1.05em; outline: none;
        }
        button {
            padding: 12px 22px; border-radius: 8px; border: 1px solid #00ffcc;
            background: #00ffcc; color: #04110e; font: inherit; font-weight: bold; cursor: pointer;
        }
        button:disabled { opacity: 0.5; cursor: wait; }
        pre {
            text-align: left; background: #07110f; border: 1px solid #123; border-radius: 8px;
            padding: 12px; max-height: 42vh; overflow: auto; color: #bfffe9;
            font-size: 0.82em; white-space: pre-wrap; word-break: break-word;
        }
        .hint { color: #888; font-size: 0.82em; }
        a { color: #00ffcc; }
    </style>
</head>
<body>
    <div id="canvas-container"></div>
    <div class="overlay">
        <h1>SYSTEM ONLINE</h1>
        <p>API Gateway is <span class="highlight">Active &amp; Secured</span></p>
        <p>Source: <span class="highlight" id="mode">checking...</span></p>
        <div class="status-box">
            <span class="blinking" style="color: #00ffcc;">●</span> HTTP 200 OK - LISTENING FOR QUERIES
        </div>
        <form id="search-form">
            <input id="number" name="number" inputmode="numeric" autocomplete="off"
                   placeholder="Enter phone number (e.g. 9732226783)" />
            <button id="go" type="submit">FETCH</button>
        </form>
        <p class="hint">
            Remote mode streams the sorted HuggingFace index. The first lookup that
            touches a new row group can take 30-90s; repeated queries are cached.
        </p>
        <pre id="out">Ready. Try 9732226783</pre>
    </div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
    <script>
        try {
            const scene = new THREE.Scene();
            const camera = new THREE.PerspectiveCamera(75, innerWidth / innerHeight, 0.1, 2000);
            const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
            renderer.setSize(innerWidth, innerHeight);
            document.getElementById('canvas-container').appendChild(renderer.domElement);

            const geometry = new THREE.BufferGeometry();
            const vertices = [];
            for (let i = 0; i < 8000; i++) {
                vertices.push(THREE.MathUtils.randFloatSpread(3000));
                vertices.push(THREE.MathUtils.randFloatSpread(3000));
                vertices.push(THREE.MathUtils.randFloatSpread(3000));
            }
            geometry.setAttribute('position', new THREE.Float32BufferAttribute(vertices, 3));
            const material = new THREE.PointsMaterial({ color: 0x00ffcc, size: 2.5, transparent: true, opacity: 0.8 });
            const points = new THREE.Points(geometry, material);
            scene.add(points);
            camera.position.z = 1200;

            (function animate() {
                requestAnimationFrame(animate);
                points.rotation.x += 0.0005;
                points.rotation.y += 0.001;
                renderer.render(scene, camera);
            })();

            addEventListener('resize', () => {
                camera.aspect = innerWidth / innerHeight;
                camera.updateProjectionMatrix();
                renderer.setSize(innerWidth, innerHeight);
            });
        } catch (e) {
            console.warn('three.js unavailable', e);
        }

        fetch('/health').then(r => r.json()).then(h => {
            document.getElementById('mode').textContent =
                (h.mode || 'unknown').toUpperCase() + (h.bucket ? ' · ' + h.bucket : '');
        }).catch(() => {});

        const form = document.getElementById('search-form');
        const out = document.getElementById('out');
        const go = document.getElementById('go');

        form.addEventListener('submit', async (ev) => {
            ev.preventDefault();
            const n = document.getElementById('number').value.trim();
            if (!n) return;
            go.disabled = true;
            out.textContent = 'Searching… the first remote lookup can take 30-90 seconds.';
            const t0 = performance.now();
            try {
                const r = await fetch('/FetchData?Number=' + encodeURIComponent(n));
                const j = await r.json();
                out.textContent = 'HTTP ' + r.status + ' · ' +
                    Math.round(performance.now() - t0) + ' ms\n\n' +
                    JSON.stringify(j, null, 2);
            } catch (e) {
                out.textContent = 'Request failed: ' + e;
            } finally {
                go.disabled = false;
            }
        });
    </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def root_landing_page():
    return HTMLResponse(content=LANDING_PAGE_HTML, status_code=200)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "mode": MODE,
        "bucket": BUCKET if MODE == "remote" else None,
        "data_dir": _DATA_DIR,
        "indexes": {
            "phone": bool(_LOCAL_PHONE) if MODE == "local" else True,
            "aadhar": bool(_LOCAL_AADHAR) if MODE == "local" else True,
            "name": bool(_LOCAL_SCAN_FILES),
            "other": bool(_LOCAL_SCAN_FILES) or ALLOW_REMOTE_OTHER_SCAN,
        },
        "local_parts": {
            "phone": len(_LOCAL_PHONE),
            "aadhar": len(_LOCAL_AADHAR),
            "raw": len(_LOCAL_RAW),
        },
        "dedup_cap": DUPLICATE_CAP,
        "cache_ttl_s": CACHE_TTL,
        "cache_entries": len(_cache),
        "columns": SEARCH_FIELDS,
    }


@app.get("/FetchData")
def fetch_data(Number: str = Query(None)):
    """Original endpoint: exact phone lookup.

    Main_Records = rows whose phoneNumber matches.
    Alt_Records  = rows whose otherNumber matches (local mode / opt-in only).
    """
    raw = (Number or "").strip()
    digits = re.sub(r"\D", "", raw)
    if not digits or len(digits) < 10 or len(digits) > 15:
        return JSONResponse(
            status_code=400,
            content={
                "status": "rejected",
                "message": "Invalid parameter. STRICTLY use /FetchData?Number=XXXXXXXXXX",
                "Developer": "cybern1lux",
            },
        )

    cache_key = ("fetch", digits)
    cached = _cache_get(cache_key)
    if cached is not None:
        payload = dict(cached)
        meta = dict(payload.get("meta", {}))
        meta["cached"] = True
        payload["meta"] = meta
        return payload

    t0 = time.time()
    try:
        main_data = {"results": [], "raw_count": 0}
        for v in _phone_variants(digits):
            main_data = _query_index("phone", v, 20, None)
            if main_data["results"]:
                break

        alt_data = _query_other(digits, 20, None)

        aadhar_data = {"results": [], "raw_count": 0}
        if not main_data["results"] and not alt_data["results"] and len(digits) >= 12:
            for v in _phone_variants(digits):
                aadhar_data = _query_index("aadhar", v, 20, None)
                if aadhar_data["results"]:
                    break

        if not main_data["results"] and not alt_data["results"] and not aadhar_data["results"]:
            return JSONResponse(
                status_code=404,
                content={
                    "status": "not_found",
                    "phone": digits,
                    "Developer": "cybern1lux",
                },
            )

        data: dict[str, Any] = {
            "Main_Records": main_data["results"],
            "Alt_Records": alt_data["results"],
        }
        if aadhar_data["results"]:
            data["Aadhar_Records"] = aadhar_data["results"]

        payload = {
            "status": "success",
            "phone": digits,
            "Data": data,
            "meta": {
                "mode": MODE,
                "index": main_data.get("index") or aadhar_data.get("index") or alt_data.get("index"),
                "parts_queried": main_data.get("parts_queried", 0),
                "elapsed_ms": round((time.time() - t0) * 1000, 1),
                "raw_matches": {
                    "phoneNumber": main_data["raw_count"],
                    "otherNumber": alt_data["raw_count"],
                    "aadharNumber": aadhar_data["raw_count"],
                },
                "dedup_cap": DUPLICATE_CAP,
                "cached": False,
                "alt_lookup": alt_data.get("index") or alt_data.get("skipped"),
            },
            "Developer": "cybern1lux",
        }
        _cache_set(cache_key, payload)
        return payload

    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Database processing error: {str(e)}",
                "Developer": "cybern1lux",
            },
        )


@app.get("/search")
def search(
    q: str = Query(..., description="Search term - phone, aadhar, name, address, etc."),
    field: str | None = Query(None, description=f"Restrict to one field: {SEARCH_FIELDS}"),
    mode: str = Query("contains", pattern="^(exact|contains)$"),
    limit: int = Query(10, ge=1, le=1000),
    source: str | None = Query(None, pattern="^(icmr|hitek|inddata)$",
                               description="Filter by source: icmr, hitek (=inddata), inddata"),
    pretty: bool = Query(True, description="Pretty-print JSON"),
):
    """Unified search across all 11 columns. Duplicates capped at 2 per person."""
    data = _search_one(q, field=field, mode=mode, limit=limit, source=source)
    return _pretty(data, pretty)


class BatchRequest(BaseModel):
    queries: list[dict[str, Any]]
    limit: int = 10


@app.post("/search/parallel")
def search_parallel(req: BatchRequest):
    """Run up to 50 searches in parallel (bounded by ICMR_PARALLEL)."""
    if not req.queries:
        raise HTTPException(400, "queries must not be empty")
    if len(req.queries) > 50:
        raise HTTPException(400, "max 50 queries per batch")

    jobs = [
        (item.get("value", ""), item.get("field"), item.get("mode", "contains"),
         int(item.get("limit", req.limit)), item.get("source"))
        for item in req.queries
    ]
    workers = max(1, min(PARALLELISM, len(jobs)))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="batch") as pool:
        futures = [pool.submit(_search_one, *job) for job in jobs]
        results = []
        for f in futures:
            try:
                results.append(f.result())
            except Exception as e:
                results.append({"error": str(e)})
    return _pretty({"searches": len(jobs), "parallelism": workers, "results": results}, True)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8001, reload=False)
