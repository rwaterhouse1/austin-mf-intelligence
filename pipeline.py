"""
Austin Multifamily Certificate of Occupancy Tracker
====================================================
Fetches permit classes: C-104, C-105, C-106
COMMANDS:
    python pipeline.py setup       # Create DB tables + load zip crosswalk
    python pipeline.py backfill    # Pull all history from Austin Open Data
    python pipeline.py incremental # Pull permits since last run
    python pipeline.py enrich      # Re-run submarket assignment only
    python pipeline.py schedule    # Start daily auto-refresh at 6am
    python pipeline.py status      # Show counts, match rate, top submarkets
"""

import os
import sys
import time
import json
import logging
import argparse
from typing import Optional

import requests
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
DB_DSN = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/austin_co")
SOCRATA_ENDPOINT = "https://data.austintexas.gov/resource/3syk-w9eu.json"
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")
PAGE_SIZE = 1000
PERMIT_CLASSES = [
    "C- 104 Three & Four Family Bldgs",
    "C- 105 Five or More Family Bldgs",
    "C- 106 Mixed Use",
]
BATCH_COMMIT_SIZE = 500  # commit every N records to survive SSL drops

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("pipeline.log")],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# ZIP → COSTAR SUBMARKET CROSSWALK
# ─────────────────────────────────────────────────────────────────────────────
ZIP_CROSSWALK = {
    "78701": "Downtown Austin",
    "78703": "West Austin",
    "78705": "Central Austin",
    "78751": "Central Austin",
    "78752": "Central Austin",
    "78756": "Central Austin",
    "78757": "Central Austin",
    "78722": "Midtown Austin",
    "78723": "East Austin",
    "78731": "Northwest Austin",
    "78702": "East Austin",
    "78721": "East Austin",
    "78724": "East Austin",
    "78725": "Southeast Austin",
    "78753": "North Austin",
    "78758": "North Austin",
    "78727": "North Austin",
    "78726": "Northwest Austin",
    "78750": "Northwest Austin",
    "78759": "Northwest Austin",
    "78717": "Far North Austin",
    "78728": "Far North Austin",
    "78729": "Far North Austin",
    "78733": "West Austin",
    "78736": "Southwest Austin",
    "78737": "Southwest Austin",
    "78730": "Northwest Austin",
    "78746": "West Austin",
    "78704": "South Central Austin",
    "78745": "South Austin",
    "78748": "South Austin",
    "78749": "Southwest Austin",
    "78741": "Riverside",
    "78742": "East Austin",
    "78744": "Southeast Austin",
    "78617": "Southeast Austin",
    "78719": "Southeast Austin",
    "78747": "Southeast Austin",
    "78735": "Southwest Austin",
    "78739": "Southwest Austin",
    "78653": "Northeast Austin",
    "78754": "Northeast Austin",
    "78660": "Pflugerville",
    "78691": "Pflugerville",
    "78664": "Round Rock",
    "78665": "Round Rock",
    "78680": "Round Rock",
    "78681": "Round Rock",
    "78682": "Round Rock",
    "78626": "Georgetown-Leander",
    "78627": "Georgetown-Leander",
    "78628": "Georgetown-Leander",
    "78633": "Georgetown-Leander",
    "78641": "Georgetown-Leander",
    "78642": "Georgetown-Leander",
    "78646": "Georgetown-Leander",
    "78613": "Cedar Park",
    "78630": "Cedar Park",
    "78620": "Lake Travis",
    "78669": "Lake Travis",
    "78734": "Lake Travis",
    "78738": "Lake Travis",
    "78645": "Lake Travis",
    "78610": "Buda-Kyle",
    "78640": "Buda-Kyle",
    "78666": "San Marcos",
    "78667": "San Marcos",
    "78028": "Hill Country",
    "78070": "Hill Country",
    "78132": "Hill Country",
    "78163": "Hill Country",
    "78130": "Comal County",
    "78131": "Comal County",
    "78006": "Kendall County",
    "78015": "Kendall County",
    "78027": "Kendall County",
    "78108": "Guadalupe County",
    "78124": "Guadalupe County",
    "78155": "Guadalupe County",
    "78602": "Bastrop County",
    "78612": "Bastrop County",
    "78621": "Bastrop County",
    "78650": "Bastrop County",
    "78659": "Bastrop County",
    "78644": "Caldwell County",
    "78648": "Caldwell County",
    "78656": "Caldwell County",
}

# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA
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
CREATE INDEX IF NOT EXISTS idx_submarkets_geom ON costar_submarkets USING GIST(geom);

CREATE TABLE IF NOT EXISTS zip_submarket_crosswalk (
    zip_code        VARCHAR(5) PRIMARY KEY,
    submarket_name  VARCHAR(128) NOT NULL
);

CREATE TABLE IF NOT EXISTS co_permits_raw (
    id              SERIAL PRIMARY KEY,
    permit_num      VARCHAR(64) UNIQUE NOT NULL,
    masterpermitnum VARCHAR(64),
    permit_class    VARCHAR(128),
    issue_date      DATE,
    address         TEXT,
    zip_code        VARCHAR(10),
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    total_units     INTEGER,
    work_class      VARCHAR(64),
    project_name    TEXT,
    permit_type     VARCHAR(64),
    permit_status   VARCHAR(32),
    raw_json        JSONB,
    ingested_at     TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_raw_masterpermit ON co_permits_raw(masterpermitnum);
CREATE INDEX IF NOT EXISTS idx_raw_issue_date  ON co_permits_raw(issue_date);
CREATE INDEX IF NOT EXISTS idx_raw_total_units ON co_permits_raw(total_units);

CREATE TABLE IF NOT EXISTS co_permits (
    id              SERIAL PRIMARY KEY,
    permit_num      VARCHAR(64) UNIQUE NOT NULL REFERENCES co_permits_raw(permit_num),
    masterpermitnum VARCHAR(64),
    permit_class    VARCHAR(128),
    issue_date      DATE NOT NULL,
    address         TEXT,
    zip_code        VARCHAR(10),
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    geom            GEOMETRY(POINT, 4326),
    total_units     INTEGER,
    project_name    TEXT,
    work_class      VARCHAR(64),
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
CREATE INDEX IF NOT EXISTS idx_permits_geom      ON co_permits USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_permits_date      ON co_permits(issue_date);
CREATE INDEX IF NOT EXISTS idx_permits_submarket ON co_permits(submarket_name);
CREATE INDEX IF NOT EXISTS idx_permits_year      ON co_permits(delivery_year);
CREATE INDEX IF NOT EXISTS idx_permits_yyyyq     ON co_permits(delivery_yyyyq);

-- Deduplicated view: collapse sub-permits per masterpermitnum,
-- filter to New work_class, 5-1000 units.
CREATE OR REPLACE VIEW co_projects AS
SELECT DISTINCT ON (COALESCE(masterpermitnum, permit_num))
    id, permit_num, masterpermitnum, permit_class, issue_date, address,
    zip_code, latitude, longitude, total_units, project_name, work_class,
    submarket_id, submarket_name,
    delivery_year, delivery_quarter, delivery_yyyyq
FROM co_permits
WHERE work_class = 'NEW'
  AND total_units BETWEEN 5 AND 1000
ORDER BY COALESCE(masterpermitnum, permit_num), issue_date DESC;

CREATE OR REPLACE VIEW submarket_deliveries AS
SELECT
    COALESCE(submarket_name, 'Unknown') AS submarket_name,
    delivery_year,
    delivery_quarter,
    delivery_yyyyq,
    COUNT(*)           AS project_count,
    SUM(total_units)     AS total_units_delivered
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
    permit_num, masterpermitnum, permit_class, issue_date, address, zip_code,
    latitude, longitude, geom, total_units, project_name, work_class,
    submarket_id, submarket_name
)
SELECT
    r.permit_num,
    r.masterpermitnum,
    r.permit_class,
    r.issue_date,
    r.address,
    r.zip_code,
    r.latitude,
    r.longitude,
    CASE WHEN r.latitude IS NOT NULL AND r.longitude IS NOT NULL
         THEN ST_SetSRID(ST_MakePoint(r.longitude, r.latitude), 4326)
         ELSE NULL END,
    r.total_units,
    r.project_name,
    r.work_class,
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
        SUBSTRING(r.address FROM '([0-9]{5})(?:[- ][0-9]{4})?$')
    )
WHERE r.issue_date IS NOT NULL
ON CONFLICT (permit_num) DO UPDATE SET
    masterpermitnum = EXCLUDED.masterpermitnum,
    permit_class   = EXCLUDED.permit_class,
    issue_date     = EXCLUDED.issue_date,
    zip_code       = EXCLUDED.zip_code,
    latitude       = EXCLUDED.latitude,
    longitude      = EXCLUDED.longitude,
    geom           = EXCLUDED.geom,
    total_units    = EXCLUDED.total_units,
    work_class     = EXCLUDED.work_class,
    submarket_id   = EXCLUDED.submarket_id,
    submarket_name = EXCLUDED.submarket_name,
    enriched_at    = NOW();
"""

UPSERT_RAW_SQL = """
INSERT INTO co_permits_raw (
    permit_num, masterpermitnum, permit_class, issue_date, address, zip_code,
    latitude, longitude, total_units, work_class, project_name, permit_type,
    permit_status, raw_json
) VALUES (
    %(permit_num)s, %(masterpermitnum)s, %(permit_class)s, %(issue_date)s,
    %(address)s, %(zip_code)s, %(latitude)s, %(longitude)s, %(total_units)s,
    %(work_class)s, %(project_name)s, %(permit_type)s, %(permit_status)s,
    %(raw_json)s
)
ON CONFLICT (permit_num) DO UPDATE SET
    masterpermitnum = EXCLUDED.masterpermitnum,
    permit_class  = EXCLUDED.permit_class,
    issue_date    = EXCLUDED.issue_date,
    address       = EXCLUDED.address,
    zip_code      = EXCLUDED.zip_code,
    latitude      = EXCLUDED.latitude,
    longitude     = EXCLUDED.longitude,
    total_units   = EXCLUDED.total_units,
    work_class    = EXCLUDED.work_class,
    permit_status = EXCLUDED.permit_status,
    raw_json      = EXCLUDED.raw_json,
    ingested_at   = NOW()
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
    print("\n━━━  AUSTIN CO TRACKER — SETUP  ━━━\n")
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
    print("\nNext → python pipeline.py backfill\n")

# ─────────────────────────────────────────────────────────────────────────────
# FETCH
# Key fix: no $where filter — we paginate all records and filter in Python
# This avoids the 400 errors from the Austin API rejecting SoQL operators
# ─────────────────────────────────────────────────────────────────────────────
def fetch_page(offset: int, since_date: Optional[str] = None) -> list[dict]:
    headers = {"X-App-Token": SOCRATA_APP_TOKEN} if SOCRATA_APP_TOKEN else {}
    params = {
        "$limit":  PAGE_SIZE,
        "$offset": offset,
    }
    # Fetch all 3 permit classes; no housing_units filter at raw ingest
    classes_sql = ", ".join(f"'{c}'" for c in PERMIT_CLASSES)
    where = f"permit_class in({classes_sql})"
    if since_date:
        where += f" AND issue_date > '{since_date}T00:00:00.000'"
    params["$where"] = where

    for _attempt in range(5):
        try:
            resp = requests.get(SOCRATA_ENDPOINT, params=params, headers=headers, timeout=120)
            resp.raise_for_status()
            break
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as _e:
            if _attempt == 4:
                raise
            import time as _t; _t.sleep(10 * (_attempt + 1))
    return resp.json()


def fetch_all(since_date: Optional[str] = None) -> list[dict]:
    log.info(f"Fetching permits | since={since_date or 'beginning'} | classes={PERMIT_CLASSES}")
    records, offset = [], 0
    while True:
        batch = fetch_page(offset, since_date)
        if not batch:
            break
        records.extend(batch)
        log.info(f"  Page {offset//PAGE_SIZE + 1}: {len(batch)} fetched ({len(records)} total)")
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(0.2)
    log.info(f"Total fetched: {len(records):,}")
    return records


def _safe_int(val) -> int:
    try:
        return int(float(val)) if val else 0
    except (TypeError, ValueError):
        return 0


def parse_record(r: dict) -> Optional[dict]:
    try:
        permit_num = (r.get("permit_number") or r.get("permitnum") or "").strip()
        if not permit_num:
            return None
        issue_date = r.get("issue_date", "")[:10] if r.get("issue_date") else None
        if not issue_date:
            return None
        lat = lon = None
        try:
            if r.get("latitude"):
                lat = float(r["latitude"])
                lon = float(r["longitude"])
            elif isinstance(r.get("location"), dict):
                lat = float(r["location"].get("latitude") or 0) or None
                lon = float(r["location"].get("longitude") or 0) or None
        except (TypeError, ValueError):
            pass
        zip_code = str(r.get("original_zip") or "").strip()[:5] or None
        return {
            "permit_num":       permit_num,
            "masterpermitnum":  (r.get("masterpermitnum") or "").strip() or None,
            "permit_class":     (r.get("permit_class") or "").strip(),
            "issue_date":       issue_date,
            "address":          (r.get("permit_location") or r.get("location_address") or "").strip(),
            "zip_code":         zip_code,
            "latitude":         lat,
            "longitude":        lon,
            "total_units":      _safe_int(r.get("housing_units")),
            "work_class":       r.get("work_class", "").strip().upper(),
            "project_name":     (r.get("description") or r.get("projectname") or "").strip(),
            "permit_type":      r.get("permit_type_desc", "").strip(),
            "permit_status":    (r.get("status_current") or r.get("permit_status") or "").strip(),
            "raw_json":         json.dumps(r),
        }
    except Exception as e:
        log.warning(f"Parse error: {e}")
        return None


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
# PIPELINE
# ─────────────────────────────────────────────────────────────────────────────
def run_pipeline(mode: str = "incremental"):
    start  = time.time()
    conn   = get_conn()
    errors = None
    fetched = new_records = enriched = 0
    try:
        since = None if mode == "backfill" else last_ingested_date(conn)
        log.info(f"=== {'BACKFILL' if mode == 'backfill' else f'INCREMENTAL since {since}'} ===")
        raw    = fetch_all(since_date=since)
        parsed = [p for r in raw if (p := parse_record(r))]
        fetched = len(parsed)
        if parsed:
            # Batch-commit every BATCH_COMMIT_SIZE records to survive SSL drops
            new_records = 0
            for i in range(0, len(parsed), BATCH_COMMIT_SIZE):
                chunk = parsed[i:i + BATCH_COMMIT_SIZE]
                try:
                    _, chunk_new = upsert_raw(conn, chunk)
                    new_records += chunk_new
                    log.info(f"  Committed batch {i//BATCH_COMMIT_SIZE + 1}: {len(chunk)} records ({chunk_new} new, {new_records} total new)")
                except Exception as batch_err:
                    log.error(f"  Batch {i//BATCH_COMMIT_SIZE + 1} failed: {batch_err} — reconnecting")
                    conn = get_conn()  # reconnect on SSL drop
                    _, chunk_new = upsert_raw(conn, chunk)
                    new_records += chunk_new
            log.info(f"Upserted {fetched:,} | {new_records:,} new")
        enriched = enrich_permits(conn)
    except Exception as e:
        errors = str(e)
        log.error(f"Pipeline error: {e}", exc_info=True)
    finally:
        duration = time.time() - start
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pipeline_log "
                "(run_type,records_fetched,records_new,records_enriched,errors,duration_secs) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (mode, fetched, new_records, enriched, errors, round(duration, 2)),
            )
        conn.commit()
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
            GROUP BY submarket_name ORDER BY units DESC LIMIT 10
        """)
        top = cur.fetchall()
    conn.close()
    pct = f"{matched_n/enriched_n*100:.0f}%" if enriched_n else "n/a"
    print("\n━━━  STATUS  ━━━\n")
    print(f"  Raw records      : {raw_n:,}")
    print(f"  Enriched permits : {enriched_n:,}")
    print(f"  Submarket matched: {matched_n:,}  ({pct})")
    if dates and dates["mn"]:
        print(f"  Date range       : {dates['mn']} → {dates['mx']}")
    if last:
        print(f"\n  Last run  : {last['run_at']}  ({last['run_type']})")
        print(f"  Fetched / New : {last['records_fetched']:,} / {last['records_new']:,}")
        if last["errors"]:
            print(f"  ⚠ Error   : {last['errors']}")
    if top:
        print("\n  Top Submarkets:")
        for r in top:
            bar = "█" * min(int(r["units"] / 300), 30)
            print(f"  {r['submarket_name']:<26} {r['units']:>6,}  {bar}")
    print()

# ─────────────────────────────────────────────────────────────────────────────
# SCHEDULER
# ─────────────────────────────────────────────────────────────────────────────
def start_scheduler():
    import schedule
    log.info("Scheduler started — incremental pull at 06:00 daily.")
    schedule.every().day.at("06:00").do(run_pipeline, mode="incremental")
    while True:
        schedule.run_pending()
        time.sleep(60)

# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Austin MF CO Tracker (C-104/C-105/C-106)")
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
