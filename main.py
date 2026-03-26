#!/usr/bin/env python3

# run as: uvicorn main:app --host 0.0.0.0 --port PORT

from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import psycopg
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, Response

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("owntracks_ingest")

PSQL_URL = os.getenv("PSQL_URL", "").strip()

if not PSQL_URL:
    raise RuntimeError("PSQL_URL is not set in the environment or .env")


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS owntracks_locations (
    id BIGSERIAL PRIMARY KEY,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    topic TEXT,
    username TEXT,
    device TEXT,
    tracker_id TEXT NOT NULL,

    payload_type TEXT NOT NULL,

    tst TIMESTAMPTZ NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    acc DOUBLE PRECISION,
    alt DOUBLE PRECISION,
    batt INTEGER,
    vel DOUBLE PRECISION,
    cog DOUBLE PRECISION,

    conn TEXT,
    inregions TEXT[],

    raw JSONB NOT NULL,

    CONSTRAINT owntracks_locations_dedupe_unique
        UNIQUE (tracker_id, tst, lat, lon)
);

CREATE INDEX IF NOT EXISTS owntracks_locations_received_at_idx
    ON owntracks_locations (received_at DESC);

CREATE INDEX IF NOT EXISTS owntracks_locations_tst_idx
    ON owntracks_locations (tst DESC);

CREATE INDEX IF NOT EXISTS owntracks_locations_tracker_id_idx
    ON owntracks_locations (tracker_id);

CREATE INDEX IF NOT EXISTS owntracks_locations_payload_type_idx
    ON owntracks_locations (payload_type);

CREATE INDEX IF NOT EXISTS owntracks_locations_raw_gin_idx
    ON owntracks_locations
    USING GIN (raw);
"""


def unix_to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None

    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def normalise_text_list(value: Any) -> list[str] | None:
    if value is None:
        return None

    if isinstance(value, list):
        return [str(item) for item in value]

    return None


def clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    return text or None


def get_text_header(request: Request, name: str) -> str | None:
    value = request.headers.get(name)
    if value is None:
        return None

    value = value.strip()
    return value or None


def extract_record(payload: dict[str, Any], request: Request) -> dict[str, Any]:
    tracker_id = clean_optional_text(payload.get("tid"))
    tst = unix_to_datetime(payload.get("tst"))

    if tracker_id is None:
        raise HTTPException(status_code=400, detail="Location payload missing tid")

    if tst is None:
        raise HTTPException(status_code=400, detail="Location payload missing/invalid tst")

    if "lat" not in payload or "lon" not in payload:
        raise HTTPException(status_code=400, detail="Location payload missing lat/lon")

    topic = (
        clean_optional_text(payload.get("topic"))
        or get_text_header(request, "X-OT-Topic")
        or get_text_header(request, "X-Limit-U")
    )

    username = (
        clean_optional_text(payload.get("username"))
        or get_text_header(request, "X-OT-Username")
    )

    device = (
        clean_optional_text(payload.get("device"))
        or get_text_header(request, "X-OT-Device")
    )

    return {
        "topic": topic,
        "username": username,
        "device": device,
        "tracker_id": tracker_id,
        "payload_type": str(payload.get("_type", "unknown")),
        "tst": tst,
        "lat": payload.get("lat"),
        "lon": payload.get("lon"),
        "acc": payload.get("acc"),
        "alt": payload.get("alt"),
        "batt": payload.get("batt"),
        "vel": payload.get("vel"),
        "cog": payload.get("cog"),
        "conn": clean_optional_text(payload.get("conn")),
        "inregions": normalise_text_list(payload.get("inregions")),
        "raw": json.dumps(payload, ensure_ascii=False),
    }


def insert_record(record: dict[str, Any]) -> bool:
    with psycopg.connect(PSQL_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO owntracks_locations (
                    topic,
                    username,
                    device,
                    tracker_id,
                    payload_type,
                    tst,
                    lat,
                    lon,
                    acc,
                    alt,
                    batt,
                    vel,
                    cog,
                    conn,
                    inregions,
                    raw
                )
                VALUES (
                    %(topic)s,
                    %(username)s,
                    %(device)s,
                    %(tracker_id)s,
                    %(payload_type)s,
                    %(tst)s,
                    %(lat)s,
                    %(lon)s,
                    %(acc)s,
                    %(alt)s,
                    %(batt)s,
                    %(vel)s,
                    %(cog)s,
                    %(conn)s,
                    %(inregions)s,
                    %(raw)s::jsonb
                )
                ON CONFLICT (tracker_id, tst, lat, lon) DO NOTHING
                """,
                record,
            )
            inserted = cur.rowcount == 1

        conn.commit()
        return inserted


def init_db() -> None:
    with psycopg.connect(PSQL_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initialising database")
    init_db()
    yield
    logger.info("Shutting down")


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/owntracks")
async def owntracks_ingest(request: Request) -> Response:
    body = await request.body()

    if not body:
        logger.info("Ignoring zero-length payload")
        return Response(content="[]", media_type="application/json", status_code=200)

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        logger.warning("Invalid JSON payload: %s", exc)
        raise HTTPException(status_code=400, detail="Invalid JSON payload") from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Expected JSON object")

    payload_type = payload.get("_type")
    if payload_type != "location":
        logger.info("Ignoring non-location payload with _type=%r", payload_type)
        return Response(content="[]", media_type="application/json", status_code=200)

    record = extract_record(payload, request)
    inserted = insert_record(record)

    if inserted:
        logger.info(
            "Stored location: tid=%r lat=%r lon=%r tst=%r",
            record["tracker_id"],
            record["lat"],
            record["lon"],
            record["tst"],
        )
    else:
        logger.info(
            "Skipped duplicate: tid=%r lat=%r lon=%r tst=%r",
            record["tracker_id"],
            record["lat"],
            record["lon"],
            record["tst"],
        )

    return Response(content="[]", media_type="application/json", status_code=200)