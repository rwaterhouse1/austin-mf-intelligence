"""
San Antonio Multifamily Building Permit Tracker
================================================
Data source: data.sanantonio.gov (CKAN — NOT Socrata)
Permit type: "Comm New Building Permit" with WORK TYPE = "New"
Unit estimate: AREA (SF) / 900, minimum 5 units

COMMANDS:
    python pipeline_sanantonio.py setup       # Create DB tables + load zip crosswalk
    python pipeline_sanantonio.py backfill    # Pull all history (current + historical resources)
    python pipeline_sanantonio.py incremental # Pull permits since last run
    python pipeline_sanantonio.py enrich      # Re-run submarket assignment only
    python pipeline_sanantonio.py schedule    # Start daily auto-refresh at 6am
    python pipeline_sanantonio.py status      # Show counts, match rate, top submarkets
"""

import os
import re
import sys
import time
import json
import logging
import argparse
from typing import Optional

import requests
import psycopg2
import psycopg2.extras
import schedule
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
DB_DSN = os.getenv("SANANTONIO_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/sanantonio_permits")

# CKAN datastore endpoints — data.sanantonio.gov runs CKAN, not Socrata
CKAN_BASE            = "https://data.sanantonio.gov/api/3/action"
CKAN_SEARCH_URL      = f"{CKAN_BASE}/datastore_search"
CKAN_SQL_URL         = f"{CKAN_BASE}/datastore_search_sql"

# Current permits resource (rolling / live)
RESOURCE_CURRENT     = "c21106f9-3ef5-4f3a-8604-f992b4db7512"
# Historical permits resource (2020-2024)
RESOURCE_HISTORICAL  = "c22b1ef2-dcf8-4d77-be1a-ee3638092aab"

# Multifamily filter criteria
MF_PERMIT_TYPE       = "Comm New Building Permit"
MF_WORK_TYPE         = "New"

# Unit estimation: no unit count field exists in SA data
AVG_UNIT_SF          = 900    # average multifamily unit size (sq ft)
MIN_UNITS            = 5      # minimum units to consider as multifamily
MAX_UNITS            = 2000   # sanity cap

PAGE_SIZE            = 100    # CKAN default max per page
BATCH_COMMIT_SIZE    = 200    # commit every N records to survive SSL drops

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("pipeline_sanantonio.log")],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# ZIP → COSTAR SUBMARKET CROSSWALK  (San Antonio MSA)
# ─────────────────────────────────────────────────────────────────────────────
ZIP_CROSSWALK = {
    # Downtown
    "78201": "Downtown",
    "78202": "Downtown",
    "78203": "Downtown",
    "78204": "Downtown",
    "78205": "Downtown",
    "78206": "Downtown",
    "78207": "Downtown",  # also listed under West, using Downtown per city core
    "78208": "Downtown",
    # North Central
    "78209": "North Central",
    "78212": "North Central",
    "78216": "North Central",
    # Northwest
    "78213": "Northwest",
    "78230": "Northwest",
    "78240": "Northwest",
    "78249": "Northwest",
    "78250": "Northwest",
    # Northeast
    "78217": "Northeast",
    "78218": "Northeast",
    "78219": "Northeast",
    "78220": "Northeast",
    "78233": "Northeast",
    "78239": "Northeast",
    "78244": "Northeast",
    # South
    "78210": "South",
    "78211": "South",
    "78214": "South",
    "78221": "South",
    # Southeast
    "78222": "Southeast",
    "78223": "Southeast",
    "78235": "Southeast",
    "78241": "Southeast",
    # Southwest
    "78224": "Southwest",
    "78225": "Southwest",
    "78226": "Southwest",
    "78227": "Southwest",
    "78245": "Southwest",
    "78251": "Southwest",
    "78252": "Southwest",
    # West
    "78228": "West",
    "78237": "West",
    "78238": "West",
    # Medical Center
    "78229": "Medical Center",
    # Stone Oak
    "78231": "Stone Oak",
    "78232": "Stone Oak",
    "78247": "Stone Oak",
    "78248": "Stone Oak",
    "78256": "Stone Oak",
    "78257": "Stone Oak",
    "78258": "Stone Oak",
    "78259": "Stone Oak",
    "78260": "Stone Oak",
    "78261": "Stone Oak",
    # Helotes / Leon Valley
    "78253": "Helotes/Leon Valley",
    "78254": "Helotes/Leon Valley",
    "78255": "Helotes/Leon Valley",
    # Schertz / Cibolo
    "78108": "Schertz/Cibolo",
    "78109": "Schertz/Cibolo",
    "78124": "Schertz/Cibolo",
    "78266": "Schertz/Cibolo",
    # New Braunfels
    "78130": "New Braunfels",
    "78132": "New Braunfels",
    "78133": "New Braunfels",
    "78163": "New Braunfels",
}

# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA  (mirrors Austin co_permits structure)
# ─────────────────────────────────────────────────────────────────────────────
SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS costar_submarkets (
    id              SERIAL PRIMARY KEY,
    submarket_id    VARCHAR(64) UNIQUE NOT NULL,
    submarket_name  VARCHAR(128) NOT NULL,
    geom            GEOMETRY(MULTIPOLYGON, 4326),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sa_submarkets_geom ON costar_submarkets USING GIST(geom);

CREATE TABLE IF NOT EXISTS zip_submarket_crosswalk (
    zip_code        VARCHAR(5) PRIMARY KEY,
    submarket_name  VARCHAR(128) NOT NULL
);

CREATE TABLE IF NOT EXISTS co_permits_raw (
    id              SERIAL PRIMARY KEY,
    permit_num      VARCHAR(64) UNIQUE NOT NULL,
    issue_date      DATE,
    submitted_date  DATE,
    address         TEXT,
    zip_code        VARCHAR(10),
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    area_sf         INTEGER,
    total_units     INTEGER,
    work_class      VARCHAR(64),
    project_name    TEXT,
    permit_type     VARCHAR(64),
    permit_status   VARCHAR(32),
    cd              VARCHAR(16),
    raw_json        JSONB,
    ingested_at     TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sa_raw_issue_date  ON co_permits_raw(issue_date);
CREATE INDEX IF NOT EXISTS idx_sa_raw_total_units ON co_permits_raw(total_units);

CREATE TABLE IF NOT EXISTS co_permits (
    id              SERIAL PRIMARY KEY,
    permit_num      VARCHAR(64) UNIQUE NOT NULL REFERENCES co_permits_raw(permit_num),
    issue_date      DATE NOT NULL,
    submitted_date  DATE,
    address         TEXT,
    zip_code        VARCHAR(10),
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    geom            GEOMETRY(POINT, 4326),
    area_sf         INTEGER,
    total_units     INTEGER,
    project_name    TEXT,
    work_class      VARCHAR(64),
    cd              VARCHAR(16),
    submarket_id    VARCHAR(64),
    submarket_name  VARCHAR(128),
    delivery_year   INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR    FROM issue_date)::INTEGER) STORED,
    delivery_quarter INTEGER GENERATED ALWAYS AS (EXTRACT(QUARTER FROM issue_date)::INTEGER) STORED,
    delivery_yyyyq  VARCHAR(7) GENERATED ALWAYS AS (
                        EXTRACT(YEAR FROM issue_date)::TEXT || '-Q' ||
                        EXTRACT(QUARTER FROM issue_date)::TEXT
                    ) STORED,
    enriched_at     TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sa_permits_geom      ON co_permits USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_sa_permits_date      ON co_permits(issue_date);
CREATE INDEX IF NOT EXISTS idx_sa_permits_submarket ON co_permits(submarket_name);
CREATE INDEX IF NOT EXISTS idx_sa_permits_year      ON co_permits(delivery_year);
CREATE INDEX IF NOT EXISTS idx_sa_permits_yyyyq     ON co_permits(delivery_yyyyq);

-- Deduplicated view: collapse per (address, total_units) to avoid duplicate entries
-- Minimum 5 units, maximum 2000 units (sanity cap for estimated units)
CREATE OR REPLACE VIEW co_projects AS
SELECT DISTINCT ON (address, total_units)
    id, permit_num, issue_date, submitted_date, address, zip_code,
    latitude, longitude, area_sf, total_units, project_name, work_class,
    cd, submarket_id, submarket_name,
    delivery_year, delivery_quarter, delivery_yyyyq
FROM co_permits
WHERE total_units >= 5
  AND total_units <= 2000
ORDER BY address, total_units, issue_date DESC;

CREATE OR REPLACE VIEW submarket_deliveries AS
SELECT
    COALESCE(submarket_name, 'Unknown') AS submarket_name,
    delivery_year,
    delivery_quarter,
    delivery_yyyyq,
    COUNT(*)                AS project_count,
    SUM(total_units)        AS total_units_delivered
FROM co_projects
WHERE total_units >= 5
GROUP BY COALESCE(submarket_name, 'Unknown'), delivery_year, delivery_quarter, delivery_yyyyq
ORDER BY delivery_yyyyq, total_units_delivered DESC;

CREATE TABLE IF NOT EXISTS pipeline_log (
    id               SERIAL PRIMARY KEY,
    run_at           TIMESTAMPTZ DEFAULT NOW(),
    run_type         VARCHAR(32),
    records_fetched  INTEGER DEFAULT 0,
    records_new      INTEGER DEFAULT 0,
    records_enriched INTEGER DEFAULT 0,
    errors           TEXT,
    duration_secs    DOUBLE PRECISION
);
"""

ENRICH_SQL = """
INSERT INTO co_permits (
    permit_num, issue_date, submitted_date, address, zip_code,
    latitude, longitude, geom, area_sf, total_units, project_name,
    work_class, cd, submarket_id, submarket_name
)
SELECT
    r.permit_num,
    r.issue_date,
    r.submitted_date,
    r.address,
    r.zip_code,
    r.latitude,
    r.longitude,
    CASE WHEN r.latitude IS NOT NULL AND r.longitude IS NOT NULL
         THEN ST_SetSRID(ST_MakePoint(r.longitude, r.latitude), 4326)
         ELSE NULL END,
    r.area_sf,
    r.total_units,
    r.project_name,
    r.work_class,
    r.cd,
    s.submarket_id,
    COALESCE(s.submarket_name, z.submarket_name) AS submarket_name
FROM co_permits_raw r
LEFT JOIN costar_submarkets s
    ON s.geom IS NOT NULL
    AND r.latitude IS NOT NULL
    AND ST_Contains(s.geom, ST_SetSRID(ST_MakePoint(r.longitude, r.latitude), 4326))
LEFT JOIN zip_submarket_crosswalk z
    ON z.zip_code = COALESCE(
        r.zip_code,
        SUBSTRING(r.address FROM '([0-9]{5})(?:[- ][0-9]{4})?(?:\\s|$)')
    )
WHERE r.total_units >= 5
  AND r.issue_date IS NOT NULL
ON CONFLICT (permit_num) DO UPDATE SET
    issue_date      = EXCLUDED.issue_date,
    submitted_date  = EXCLUDED.submitted_date,
    zip_code        = EXCLUDED.zip_code,
    latitude        = EXCLUDED.latitude,
    longitude       = EXCLUDED.longitude,
    geom            = EXCLUDED.geom,
    area_sf         = EXCLUDED.area_sf,
    total_units     = EXCLUDED.total_units,
    submarket_id    = EXCLUDED.submarket_id,
    submarket_name  = EXCLUDED.submarket_name,
    enriched_at     = NOW();
"""

UPSERT_RAW_SQL = """
INSERT INTO co_permits_raw (
    permit_num, issue_date, submitted_date, address, zip_code,
    latitude, longitude, area_sf, total_units, work_class,
    project_name, permit_type, permit_status, cd, raw_json
) VALUES (
    %(permit_num)s, %(issue_date)s, %(submitted_date)s, %(address)s, %(zip_code)s,
    %(latitude)s, %(longitude)s, %(area_sf)s, %(total_units)s, %(work_class)s,
    %(project_name)s, %(permit_type)s, %(permit_status)s, %(cd)s, %(raw_json)s
)
ON CONFLICT (permit_num) DO UPDATE SET
    issue_date     = EXCLUDED.issue_date,
    submitted_date = EXCLUDED.submitted_date,
    address        = EXCLUDED.address,
    zip_code       = EXCLUDED.zip_code,
    latitude       = EXCLUDED.latitude,
    longitude      = EXCLUDED.longitude,
    area_sf        = EXCLUDED.area_sf,
    total_units    = EXCLUDED.total_units,
    permit_status  = EXCLUDED.permit_status,
    raw_json       = EXCLUDED.raw_json,
    ingested_at    = NOW()
RETURNING (xmax = 0) AS inserted;
"""

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(DB_DSN)

def last_ingested_date(conn) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(issue_date) FROM co_permits_raw WHERE issue_date IS NOT NULL")
        result = cur.fetchone()[0]
        return str(result) if result else None

# ─────────────────────────────────────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────────────────────────────────────
def cmd_setup():
    print("\n━━━  SAN ANTONIO MF PERMIT TRACKER — SETUP  ━━━\n")
    print("① Applying schema...")
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()
    print("   ✓ Tables, indexes, and views created.")
    print(f"\n② Loading {len(ZIP_CROSSWALK)} zip → submarket mappings...")
    with conn.cursor() as cur:
        cur.execute("TRUNCATE zip_submarket_crosswalk")
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO zip_submarket_crosswalk (zip_code, submarket_name) VALUES %s ON CONFLICT DO NOTHING",
            list(ZIP_CROSSWALK.items()),
        )
    conn.commit()
    conn.close()
    print("   ✓ Zip crosswalk loaded.")
    print("\n━━━  SETUP COMPLETE  ━━━")
    print("\nNext → python pipeline_sanantonio.py backfill\n")

# ─────────────────────────────────────────────────────────────────────────────
# CKAN FETCH  (data.sanantonio.gov uses CKAN datastore API, not Socrata)
#
# CKAN datastore_search returns:
#   { "result": { "records": [...], "total": N, "offset": N } }
#
# We paginate using offset/limit and filter server-side using the `filters`
# parameter for exact-match fields, then do a Python-side check for safety.
# ─────────────────────────────────────────────────────────────────────────────
def fetch_ckan_page(resource_id: str, offset: int, since_date: Optional[str] = None) -> dict:
    """
    Fetch one page from CKAN datastore_search.
    Returns the raw CKAN result dict (keys: records, total, offset).
    """
    params = {
        "resource_id": resource_id,
        "limit": PAGE_SIZE,
        "offset": offset,
        # Filter server-side for the permit type (exact match dict)
        "filters": json.dumps({"PERMIT TYPE": MF_PERMIT_TYPE, "WORK TYPE": MF_WORK_TYPE}),
    }

    for attempt in range(5):
        try:
            resp = requests.get(CKAN_SEARCH_URL, params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            if not data.get("success"):
                raise ValueError(f"CKAN error: {data.get('error', data)}")
            return data["result"]
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            if attempt == 4:
                raise
            wait = 10 * (attempt + 1)
            log.warning(f"Request failed ({e}), retrying in {wait}s...")
            time.sleep(wait)

    return {"records": [], "total": 0}


def fetch_ckan_since(resource_id: str, since_date: str) -> list[dict]:
    """
    Use CKAN SQL endpoint to fetch permits submitted or issued after since_date.
    Falls back to paginated fetch_all if SQL endpoint fails.
    """
    sql = (
        f'SELECT * FROM "{resource_id}" '
        f'WHERE "PERMIT TYPE" = \'{MF_PERMIT_TYPE}\' '
        f'AND "WORK TYPE" = \'{MF_WORK_TYPE}\' '
        f'AND "DATE ISSUED" > \'{since_date}\''
    )
    try:
        resp = requests.get(CKAN_SQL_URL, params={"sql": sql}, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        if data.get("success"):
            return data["result"]["records"]
        log.warning(f"CKAN SQL failed: {data.get('error')} — falling back to paginated fetch")
    except Exception as e:
        log.warning(f"CKAN SQL endpoint error: {e} — falling back to paginated fetch")

    # Fallback: paginated fetch (will filter in Python)
    return fetch_ckan_all_pages(resource_id)


def fetch_ckan_all_pages(resource_id: str) -> list[dict]:
    """Paginate through all records in a CKAN resource, filtering for MF permits."""
    records = []
    offset = 0
    total = None
    page_num = 0

    while True:
        result = fetch_ckan_page(resource_id, offset)
        batch = result.get("records", [])
        if total is None:
            total = result.get("total", 0)

        # Python-side filter as a safety net
        filtered = [
            r for r in batch
            if (
                str(r.get("PERMIT TYPE", "")).strip() == MF_PERMIT_TYPE
                and str(r.get("WORK TYPE", "")).strip() == MF_WORK_TYPE
            )
        ]
        records.extend(filtered)
        page_num += 1
        log.info(
            f"  Resource {resource_id[:8]}… page {page_num}: "
            f"{len(batch)} fetched, {len(filtered)} matched ({len(records)} total)"
        )

        if not batch or len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(0.3)  # polite rate-limiting

    log.info(f"Resource {resource_id[:8]}…: {len(records):,} total MF permits found")
    return records


def fetch_all(since_date: Optional[str] = None) -> list[dict]:
    """
    Fetch all multifamily permits from San Antonio open data.
    - Backfill: pulls current resource + historical resource
    - Incremental: uses SQL endpoint for permits since last run date
    """
    log.info(f"Fetching SA permits | since={since_date or 'beginning'}")

    if since_date:
        # Incremental: query both resources for new records
        log.info(f"Incremental mode — fetching permits issued after {since_date}")
        records = fetch_ckan_since(RESOURCE_CURRENT, since_date)
        # Also check historical for any late-arriving data
        hist = fetch_ckan_since(RESOURCE_HISTORICAL, since_date)
        records.extend(hist)
    else:
        # Full backfill: both resources
        log.info("Backfill mode — fetching all records from current + historical resources")
        records = fetch_ckan_all_pages(RESOURCE_CURRENT)
        log.info(f"Fetching historical resource...")
        hist_records = fetch_ckan_all_pages(RESOURCE_HISTORICAL)
        records.extend(hist_records)

    # Deduplicate by permit number before returning
    seen = set()
    deduped = []
    for r in records:
        pnum = str(r.get("PERMIT #") or r.get("permit_num") or "").strip()
        if pnum and pnum not in seen:
            seen.add(pnum)
            deduped.append(r)

    log.info(f"Total unique MF permits after dedup: {len(deduped):,}")
    return deduped


# ─────────────────────────────────────────────────────────────────────────────
# PARSE
# ─────────────────────────────────────────────────────────────────────────────
_ZIP_RE = re.compile(r'\b(7[8-9]\d{3}|78\d{3})\b')  # TX zip codes

def _extract_zip(address: str) -> Optional[str]:
    """Extract 5-digit ZIP code from an address string."""
    if not address:
        return None
    m = _ZIP_RE.search(address)
    return m.group(1) if m else None


def _safe_int(val) -> int:
    try:
        return int(float(val)) if val not in (None, "", "None") else 0
    except (TypeError, ValueError):
        return 0


def _safe_float(val) -> Optional[float]:
    try:
        f = float(val)
        return f if f != 0.0 else None
    except (TypeError, ValueError):
        return None


def _parse_date(val: str) -> Optional[str]:
    """Return ISO date string (YYYY-MM-DD) or None."""
    if not val:
        return None
    # CKAN may return various date formats; take the first 10 chars if ISO-ish
    s = str(val).strip()
    if len(s) >= 10 and s[4] == "-":
        return s[:10]
    # Try MM/DD/YYYY
    parts = s.split("/")
    if len(parts) == 3:
        try:
            return f"{parts[2][:4]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
        except Exception:
            pass
    return None


def _estimate_units(area_sf: int) -> int:
    """
    Estimate multifamily unit count from gross building area.
    San Antonio permits have no unit count field.
    Formula: area_sf / AVG_UNIT_SF, minimum MIN_UNITS, maximum MAX_UNITS.
    """
    if area_sf <= 0:
        return 0
    estimated = max(MIN_UNITS, round(area_sf / AVG_UNIT_SF))
    return min(estimated, MAX_UNITS)


def parse_record(r: dict) -> Optional[dict]:
    """
    Parse a raw CKAN record into the normalized format for upsert.
    CKAN column names match the SA data dictionary (with spaces/caps).
    """
    try:
        permit_num = str(r.get("PERMIT #") or "").strip()
        if not permit_num:
            return None

        # Issue date is primary; fall back to submitted date
        issue_date     = _parse_date(r.get("DATE ISSUED") or "")
        submitted_date = _parse_date(r.get("DATE SUBMITTED") or "")

        # We require at least one date to be useful
        if not issue_date and not submitted_date:
            return None
        # If issue_date is absent, use submitted_date as a proxy
        effective_date = issue_date or submitted_date

        address = str(r.get("ADDRESS") or "").strip()

        # Coordinates: CKAN SA data has X_COORD / Y_COORD (may be TX State Plane or WGS84)
        # We try them as lat/lon; if they look like State Plane (large numbers), skip.
        lat = lon = None
        x = _safe_float(r.get("X_COORD"))
        y = _safe_float(r.get("Y_COORD"))
        if x and y:
            # WGS84 lat/lon range check for San Antonio area
            if -100.0 < x < -97.0 and 28.0 < y < 30.5:
                lon, lat = x, y
            elif -100.0 < y < -97.0 and 28.0 < x < 30.5:
                # Swapped
                lat, lon = y, x

        # Also try LOCATION field if coords not available
        if lat is None:
            loc = r.get("LOCATION") or ""
            if isinstance(loc, str) and loc:
                # CKAN sometimes encodes as "lat,lon" or GeoJSON-like
                coord_match = re.search(r'(-?\d+\.\d+)[,\s]+(-?\d+\.\d+)', loc)
                if coord_match:
                    a, b = float(coord_match.group(1)), float(coord_match.group(2))
                    if -100.0 < a < -97.0 and 28.0 < b < 30.5:
                        lon, lat = a, b
                    elif -100.0 < b < -97.0 and 28.0 < a < 30.5:
                        lat, lon = a, b

        # ZIP from address
        zip_code = _extract_zip(address)

        # Area (sq ft) → estimated units
        area_sf = _safe_int(r.get("AREA (SF)") or r.get("AREA_SF") or 0)
        total_units = _estimate_units(area_sf)

        # Skip if estimated units fall below threshold
        if total_units < MIN_UNITS:
            return None

        return {
            "permit_num":    permit_num,
            "issue_date":    effective_date,
            "submitted_date": submitted_date,
            "address":       address,
            "zip_code":      zip_code,
            "latitude":      lat,
            "longitude":     lon,
            "area_sf":       area_sf if area_sf > 0 else None,
            "total_units":   total_units,
            "work_class":    str(r.get("WORK TYPE") or "").strip().upper(),
            "project_name":  str(r.get("PROJECT NAME") or "").strip(),
            "permit_type":   str(r.get("PERMIT TYPE") or "").strip(),
            "permit_status": str(r.get("STATUS") or r.get("permit_status") or "").strip(),
            "cd":            str(r.get("CD") or "").strip() or None,
            "raw_json":      json.dumps(r),
        }
    except Exception as e:
        log.warning(f"Parse error on record {r.get('PERMIT #', '?')}: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# DB WRITES
# ─────────────────────────────────────────────────────────────────────────────
def upsert_raw(conn, records: list[dict]) -> tuple[int, int]:
    new_count = 0
    with conn.cursor() as cur:
        for rec in records:
            cur.execute(UPSERT_RAW_SQL, rec)
            if cur.fetchone()[0]:
                new_count += 1
    conn.commit()
    return len(records), new_count


def enrich_permits(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(ENRICH_SQL)
        count = cur.rowcount
    conn.commit()
    log.info(f"Enriched {count:,} permits.")
    return count


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE ORCHESTRATION
# ─────────────────────────────────────────────────────────────────────────────
def run_pipeline(mode: str = "incremental"):
    start  = time.time()
    conn   = get_conn()
    errors = None
    fetched = new_records = enriched = 0
    try:
        since = None if mode == "backfill" else last_ingested_date(conn)
        log.info(f"=== SA {'BACKFILL' if mode == 'backfill' else f'INCREMENTAL since {since}'} ===")

        raw    = fetch_all(since_date=since)
        parsed = [p for r in raw if (p := parse_record(r))]
        fetched = len(parsed)
        log.info(f"Parsed {fetched:,} qualifying MF permits (>= {MIN_UNITS} estimated units)")

        if parsed:
            new_records = 0
            for i in range(0, len(parsed), BATCH_COMMIT_SIZE):
                chunk = parsed[i:i + BATCH_COMMIT_SIZE]
                try:
                    _, chunk_new = upsert_raw(conn, chunk)
                    new_records += chunk_new
                    log.info(
                        f"  Committed batch {i//BATCH_COMMIT_SIZE + 1}: "
                        f"{len(chunk)} records ({chunk_new} new, {new_records} total new)"
                    )
                except Exception as batch_err:
                    log.error(f"  Batch {i//BATCH_COMMIT_SIZE + 1} failed: {batch_err} — reconnecting")
                    conn = get_conn()
                    _, chunk_new = upsert_raw(conn, chunk)
                    new_records += chunk_new
            log.info(f"Upserted {fetched:,} | {new_records:,} new")

        enriched = enrich_permits(conn)

    except Exception as e:
        errors = str(e)
        log.error(f"Pipeline error: {e}", exc_info=True)
    finally:
        duration = time.time() - start
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO pipeline_log "
                    "(run_type,records_fetched,records_new,records_enriched,errors,duration_secs) "
                    "VALUES (%s,%s,%s,%s,%s,%s)",
                    (mode, fetched, new_records, enriched, errors, round(duration, 2)),
                )
            conn.commit()
        except Exception:
            pass
        conn.close()
        log.info(f"Done in {duration:.1f}s — {fetched:,} fetched | {new_records:,} new | {enriched:,} enriched")


# ─────────────────────────────────────────────────────────────────────────────
# STATUS
# ─────────────────────────────────────────────────────────────────────────────
def cmd_status():
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT COUNT(*) AS n FROM co_permits_raw")
        raw_n = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) AS n FROM co_permits")
        enriched_n = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) AS n FROM co_permits WHERE submarket_name IS NOT NULL")
        matched_n = cur.fetchone()["n"]
        cur.execute("SELECT MIN(issue_date) mn, MAX(issue_date) mx FROM co_permits")
        dates = cur.fetchone()
        cur.execute("SELECT * FROM pipeline_log ORDER BY run_at DESC LIMIT 1")
        last = cur.fetchone()
        cur.execute("""
            SELECT submarket_name, SUM(total_units) AS units, COUNT(*) AS projects
            FROM co_permits WHERE submarket_name IS NOT NULL
            GROUP BY submarket_name ORDER BY units DESC LIMIT 13
        """)
        top = cur.fetchall()
        cur.execute("SELECT SUM(area_sf) AS total_sf, AVG(area_sf) AS avg_sf FROM co_permits_raw WHERE area_sf > 0")
        sf_stats = cur.fetchone()
    conn.close()

    pct = f"{matched_n/enriched_n*100:.0f}%" if enriched_n else "n/a"
    print("\n━━━  SAN ANTONIO MF PERMIT TRACKER — STATUS  ━━━\n")
    print(f"  Raw records       : {raw_n:,}")
    print(f"  Enriched permits  : {enriched_n:,}")
    print(f"  Submarket matched : {matched_n:,}  ({pct})")
    if dates and dates["mn"]:
        print(f"  Date range        : {dates['mn']} → {dates['mx']}")
    if sf_stats and sf_stats["total_sf"]:
        print(f"  Total area (SF)   : {int(sf_stats['total_sf']):,}")
        print(f"  Avg area (SF)     : {int(sf_stats['avg_sf']):,}")
    if last:
        print(f"\n  Last run   : {last['run_at']}  ({last['run_type']})")
        print(f"  Fetched / New : {last['records_fetched']:,} / {last['records_new']:,}")
        if last["errors"]:
            print(f"  ⚠ Error    : {last['errors']}")
    if top:
        print("\n  Top Submarkets (estimated units):")
        for r in top:
            bar = "█" * min(int(r["units"] / 100), 30)
            print(f"  {r['submarket_name']:<26} {r['units']:>6,}  {bar}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# SCHEDULER
# ─────────────────────────────────────────────────────────────────────────────
def start_scheduler():
    log.info("SA Scheduler started — incremental pull at 06:00 daily.")
    schedule.every().day.at("06:00").do(run_pipeline, mode="incremental")
    while True:
        schedule.run_pending()
        time.sleep(60)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="San Antonio Multifamily Permit Tracker")
    parser.add_argument(
        "command",
        choices=["setup", "backfill", "incremental", "enrich", "schedule", "status"],
    )
    args = parser.parse_args()
    dispatch = {
        "setup":       cmd_setup,
        "backfill":    lambda: run_pipeline("backfill"),
        "incremental": lambda: run_pipeline("incremental"),
        "enrich":      lambda: (lambda c: (enrich_permits(c), c.close()))(get_conn()),
        "schedule":    start_scheduler,
        "status":      cmd_status,
    }
    dispatch[args.command]()
