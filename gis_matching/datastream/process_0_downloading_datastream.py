import time
from pathlib import Path
from typing import List, Iterable, Set, Dict, Optional

import pandas as pd
from tqdm import tqdm

import requests
import urllib3

# =========================
# Config
# =========================
API_KEY = "hm9AHiWUHDm9dGKXRJFobsYonRg0zKpG"
API_BASE = "https://api.datastream.org/v1/odata/v4"

OUTPUT_DIR = r"E:\publications\clawave_1\data\datastream\process_0_downloaded"
OUTPUT_CSV = str(Path(OUTPUT_DIR, "observations_2000_2025_all.csv"))

# You can turn these off to broaden further
FILTER_BY_MONITORING_TYPE = True      # 'River/Stream'
FILTER_BY_SURFACE_WATER   = True      # 'Surface Water'

MAX_RETRIES = 6         # exponential backoff up to ~64s
PAGE_TOP    = 3000      # safer than 5000 to avoid 413
WRITE_BATCH = 5000      # write to disk in batches
REQ_SLEEP_S = 0.6       # keep < ~2 req/s

RECORDS_COLUMNS = [
    "Id", "DOI", "DatasetName",
    "MonitoringLocationID", "MonitoringLocationName",
    "MonitoringLocationLatitude", "MonitoringLocationLongitude",
    "MonitoringLocationHorizontalCoordinateReferenceSystem",
    "MonitoringLocationHorizontalAccuracyMeasure",
    "MonitoringLocationHorizontalAccuracyUnit",
    "MonitoringLocationVerticalMeasure", "MonitoringLocationVerticalUnit",
    "MonitoringLocationType",
    "ActivityType", "ActivityMediaName",
    "ActivityStartDate", "ActivityStartTime", "ActivityStartTimeZone",
    "ActivityEndDate", "ActivityEndTime", "ActivityEndTimeZone",
    "ActivityDepthHeightMeasure", "ActivityDepthHeightUnit",
    "SampleCollectionEquipmentName",
    "CharacteristicName", "MethodSpeciation",
    "ResultSampleFraction", "ResultValue", "ResultUnit", "ResultValueType",
    "ResultDetectionCondition",
    "ResultDetectionQuantitationLimitMeasure",
    "ResultDetectionQuantitationLimitUnit",
    "ResultDetectionQuantitationLimitType",
    "ResultStatusID", "ResultComment",
    "ResultAnalyticalMethodID", "ResultAnalyticalMethodContext",
    "ResultAnalyticalMethodName",
    "AnalysisStartDate", "AnalysisStartTime", "AnalysisStartTimeZone",
    "LaboratoryName", "LaboratorySampleID",
]
SELECT_COLS = ",".join(RECORDS_COLUMNS)

HEADERS = {"x-api-key": API_KEY}

# =========================
# Minimal local replacements
# =========================
def set_api_key(key: str):
    """Keeps your original call-site; sets header for requests."""
    global HEADERS
    HEADERS = {"x-api-key": key}

def _paged_get(endpoint: str, params: Optional[Dict] = None) -> Iterable[List[Dict]]:
    """GET with @odata.nextLink pagination; yields lists of rows ('value')."""
    next_url = endpoint
    first = True
    while next_url:
        resp = requests.get(
            next_url,
            headers=HEADERS,
            params=params if first else None,  # only on first request
            timeout=120,
        )
        # Basic handling for heavy pages
        if resp.status_code == 413:
            raise requests.exceptions.HTTPError("413 Payload Too Large")
        if resp.status_code >= 400:
            raise requests.exceptions.HTTPError(f"{resp.status_code}: {resp.text[:200]}")

        js = resp.json()
        rows = js.get("value", [])
        if rows:
            yield rows

        next_url = js.get("@odata.nextLink")
        params = None
        time.sleep(REQ_SLEEP_S)
        first = False

def locations(params: Dict) -> Iterable[Dict]:
    """Generator over /Locations rows."""
    endpoint = f"{API_BASE}/Locations"
    for page in _paged_get(endpoint, params=params):
        for row in page:
            yield row

def records(params: Dict) -> Iterable[Dict]:
    """Generator over /Records rows."""
    endpoint = f"{API_BASE}/Records"
    for page in _paged_get(endpoint, params=params):
        for row in page:
            yield row

# =========================
# Your original structure
# =========================
def year_windows(start=2000, end_inclusive=2025, step=5):
    y = start
    while y <= end_inclusive:
        yield y, min(y + step, end_inclusive + 1)  # [y, y_next)
        y += step

def build_records_filter(location_id: int, y0: int, y1: int) -> str:
    parts = [
        f"LocationId eq {location_id}",
        f"ActivityStartYear gte '{y0}' and ActivityStartYear lt '{y1}'",
    ]
    if FILTER_BY_SURFACE_WATER:
        parts.append("ActivityMediaName eq 'Surface Water'")
    if FILTER_BY_MONITORING_TYPE:
        parts.append("MonitoringLocationType eq 'River/Stream'")
    return " and ".join(parts)

def fetch_river_stream_location_ids() -> List[int]:
    params = {"$select": "Id", "$top": 10000}
    if FILTER_BY_MONITORING_TYPE:
        params["$filter"] = "MonitoringLocationType eq 'River/Stream'"
    ids: List[int] = []
    for row in locations(params):
        if "Id" in row and row["Id"] is not None:
            ids.append(int(row["Id"]))
    return ids

def iter_records_with_retry(params: dict, seen_ids: Set[int]) -> Iterable[dict]:
    """Yield Records with retry/backoff and intra-cycle dedupe by Id."""
    attempt = 0
    while True:
        try:
            for row in records(params):
                rid = row.get("Id")
                # dedupe within this (location, window) cycle
                if rid is None or rid not in seen_ids:
                    if rid is not None:
                        seen_ids.add(rid)
                    yield row
            return  # finished this params block successfully

        except (requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout,
                urllib3.exceptions.ProtocolError,
                requests.exceptions.ChunkedEncodingError,
                urllib3.exceptions.IncompleteRead,
                requests.exceptions.HTTPError) as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                print(f" !! Giving up after {MAX_RETRIES} retries for params: {params['$filter'][:120]}…")
                return
            wait = min(2 ** attempt, 64)
            print(f" !! Connection issue ({type(e).__name__}): retry {attempt}/{MAX_RETRIES} in {wait}s")
            # If payload too large, reduce page size and retry
            if isinstance(e, requests.exceptions.HTTPError) and "413" in str(e):
                params["$top"] = max(1000, int(params.get("$top", PAGE_TOP) * 0.6))
                print(f" .. Reducing $top to {params['$top']} due to 413")
            time.sleep(wait)

def write_rows(rows: List[dict], out_csv: str, header_written_flag: List[bool]) -> int:
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    # Keep schema stable even if a page omits some cols
    df = df.reindex(columns=RECORDS_COLUMNS)
    df.to_csv(out_csv, mode="a", index=False, header=not header_written_flag[0])
    header_written_flag[0] = True
    return len(rows)

def main():
    set_api_key(API_KEY)
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    header_written = [Path(OUTPUT_CSV).exists() and Path(OUTPUT_CSV).stat().st_size > 0]

    print("Collecting LocationIds …")
    loc_ids = fetch_river_stream_location_ids()
    print(f"Found {len(loc_ids):,} LocationIds")

    total_written = 0
    for y0, y1 in year_windows(2000, 2025, 5):
        print(f"\nFetching {y0}-{y1-1} for {len(loc_ids):,} locations …")
        pbar = tqdm(loc_ids, unit="loc", dynamic_ncols=True)

        for loc_id in pbar:
            params = {
                "$select": SELECT_COLS,
                "$filter": build_records_filter(loc_id, y0, y1),
                "$top": PAGE_TOP,
            }

            seen_ids_this_cycle: Set[int] = set()
            batch: List[dict] = []

            for row in iter_records_with_retry(params, seen_ids_this_cycle):
                batch.append(row)
                if len(batch) >= WRITE_BATCH:
                    total_written += write_rows(batch, OUTPUT_CSV, header_written)
                    batch.clear()
                    # stay comfortably below ~2 req/sec overall
                    time.sleep(0.5)

            if batch:
                total_written += write_rows(batch, OUTPUT_CSV, header_written)
                batch.clear()

        pbar.close()

    print(f"\nDone. Wrote {total_written:,} rows to: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
