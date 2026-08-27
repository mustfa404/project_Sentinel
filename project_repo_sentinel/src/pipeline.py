#!/usr/bin/env python3
"""
pipeline.py — Project Sentinel: Automated Near-Earth Object Triage

Pulls a live close-approach catalog from the NASA NeoWs feed endpoint (two
chained 7-day windows), reconciles it against a generated ground-station log,
scrapes NASA's running total of catalogued near-Earth asteroids, and flags
which objects deserve a human's attention before the rest get routine review.

Only requests, json, csv, and pathlib are used — no pandas/numpy, per the
brief. Every task is solved with loops, dicts, comprehensions, and try/except.

Runnable standalone:
    python src/pipeline.py

Importable:
    from pipeline import run_pipeline
    run_pipeline()
"""

from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

import requests

# Allow `python src/pipeline.py` to find generate_sentinel_log.py at the repo
# root regardless of the current working directory.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

NEOWS_FEED_URL = "https://api.nasa.gov/neo/rest/v1/feed"
NEO_TOTAL_URL = (
    "https://science.nasa.gov/science-research/planetary-science/planetary-defense/near-earth-asteroids/"
)

# NOTE: replace with your own personal key from api.nasa.gov before
# submitting — DEMO_KEY is shared across every user on your IP and can be
# exhausted in minutes on a campus network.
API_KEY = "DEMO_KEY"

# Two chained 7-day windows (the API's hard per-call limit), back to back, to
# comfortably clear the 100+ object requirement.
DEFAULT_DATE_WINDOWS = [
    ("2026-08-01", "2026-08-07"),
    ("2026-08-08", "2026-08-14"),
]

SIZE_THRESHOLD_KM = 0.14   # 140 m NASA survey-completeness benchmark
LUNAR_DISTANCE_KM = 384_400
CLOSE_LD_THRESHOLD = 10

RAW_DIR = _REPO_ROOT / "data" / "raw"
PROCESSED_DIR = _REPO_ROOT / "data" / "processed"
EXTRACTED_IDS_PATH = RAW_DIR / "extracted_ids.txt"
GROUND_STATION_LOG_PATH = RAW_DIR / "ground_station_log.csv"
CLEAN_DATA_PATH = PROCESSED_DIR / "clean_data.csv"

FALLBACK_TOTAL_KNOWN_NEOS = 38_000  # used only if the live scrape fails


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def safe_float(value, default=None):
    """Cast `value` to float, returning `default` instead of raising on failure."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# --------------------------------------------------------------------------
# Section 3: API & Data Acquisition
# --------------------------------------------------------------------------

def fetch_neos(date_windows: list[tuple[str, str]] = None, api_key: str = API_KEY) -> list[dict]:
    """
    Pull near-Earth objects across one or more 7-day windows (a hard NeoWs
    API limit), merging results from `payload["near_earth_objects"]` — a
    dict keyed by date string, not a flat list — into one running list.

    Returns:
        List of raw NEO object dicts, or an empty list if every window fails.
    """
    date_windows = date_windows or DEFAULT_DATE_WINDOWS
    all_records: list[dict] = []

    for start_date, end_date in date_windows:
        params = {"start_date": start_date, "end_date": end_date, "api_key": api_key}
        try:
            response = requests.get(NEOWS_FEED_URL, params=params, timeout=30)
            response.raise_for_status()
            payload = response.json()
        except requests.exceptions.RequestException as exc:
            print(f"[fetch_neos] Request failed for {start_date}..{end_date}: {exc}")
            continue
        except json.JSONDecodeError as exc:
            print(f"[fetch_neos] Could not decode JSON for {start_date}..{end_date}: {exc}")
            continue

        neos_by_date = payload.get("near_earth_objects", {})
        window_count = 0
        for _date_str, objects in neos_by_date.items():
            all_records.extend(objects)
            window_count += len(objects)
        print(f"[fetch_neos] {start_date}..{end_date}: {window_count} objects.")

    print(f"[fetch_neos] Total merged objects across all windows: {len(all_records)}.")
    return all_records


def extract_and_write_ids(records: list[dict]) -> list[str]:
    """
    Pull `neo_reference_id` from each object, de-duplicate (keep first-seen
    order), write one-per-line, and return the list.
    """
    neo_ids = [str(obj["neo_reference_id"]) for obj in records if obj.get("neo_reference_id")]
    neo_ids = list(dict.fromkeys(neo_ids))  # de-duplicate, keep first-seen order

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    EXTRACTED_IDS_PATH.write_text("\n".join(neo_ids), encoding="utf-8")
    print(f"[extract_and_write_ids] Extracted {len(neo_ids)} unique NEO ids -> {EXTRACTED_IDS_PATH}")
    return neo_ids


def scrape_total_known_neos(url: str = NEO_TOTAL_URL) -> int | None:
    """
    Fetch the NASA Planetary Defense page and pull the running total of
    discovered near-Earth asteroids out of "<N>: Total number of discovered
    near-Earth asteroids of all sizes."

    Strategy: find the anchor phrase "Total number of discovered near-Earth
    asteroids", look at a ~40-character window *before* it (the number sits
    immediately in front of the anchor, right before a colon), strip and
    split on whitespace to isolate the numeric token.

    Falls back to a rough known figure if the fetch or parse fails, so the
    rest of the pipeline can still run.
    """
    anchor = "Total number of discovered near-Earth asteroids"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        page_text = response.text
    except requests.exceptions.RequestException as exc:
        print(f"[scrape_total_known_neos] Request failed, using fallback {FALLBACK_TOTAL_KNOWN_NEOS}: {exc}")
        return FALLBACK_TOTAL_KNOWN_NEOS

    idx = page_text.find(anchor)
    if idx == -1:
        print(f"[scrape_total_known_neos] Anchor phrase not found, using fallback {FALLBACK_TOTAL_KNOWN_NEOS}.")
        return FALLBACK_TOTAL_KNOWN_NEOS

    window_start = max(0, idx - 40)
    window = page_text[window_start:idx]
    print(f"[scrape_total_known_neos] Window before anchor: {window!r}")

    tokens = window.strip().split()
    if not tokens:
        print(f"[scrape_total_known_neos] Empty window, using fallback {FALLBACK_TOTAL_KNOWN_NEOS}.")
        return FALLBACK_TOTAL_KNOWN_NEOS

    # The number sits right in front of a colon, e.g. "...38,394:" — take the
    # last whitespace-delimited token and keep only its digits.
    last_token = tokens[-1]
    digits = re.sub(r"[^\d]", "", last_token)

    if not digits:
        print(f"[scrape_total_known_neos] Could not isolate digits from {last_token!r}, using fallback {FALLBACK_TOTAL_KNOWN_NEOS}.")
        return FALLBACK_TOTAL_KNOWN_NEOS

    value = int(digits)
    print(f"[scrape_total_known_neos] Parsed total_known_neos = {value}")
    return value


# --------------------------------------------------------------------------
# Section 4: Phase 2 helpers — native EDA utilities
# --------------------------------------------------------------------------

def walk_leaf_types(obj, path: str = "root") -> None:
    """
    Recursively walk a record, printing the type of every leaf value
    alongside its dotted/indexed path. Dicts recurse into every value;
    lists recurse into their first item; anything else is a leaf.
    """
    if isinstance(obj, dict):
        for key, value in obj.items():
            walk_leaf_types(value, f"{path}.{key}")
    elif isinstance(obj, list):
        if obj:
            walk_leaf_types(obj[0], f"{path}[0]")
        else:
            print(f"{path}: list (empty)")
    else:
        print(f"{path}: {type(obj).__name__} = {obj!r}")


def _first_close_approach(record: dict) -> dict | None:
    """Return the first close_approach_data entry, or None if the list is empty."""
    approaches = record.get("close_approach_data") or []
    return approaches[0] if approaches else None


def numeric_field_min_max_mean(records: list[dict], extractor) -> tuple:
    """
    Single-pass min/max/mean over a numeric field pulled from each record via
    `extractor(record)`, casting through safe_float and skipping (counting)
    anything that fails to cast.
    """
    total = 0.0
    count = 0
    minimum = None
    maximum = None
    n_missing = 0

    for record in records:
        raw_value = extractor(record)
        value = safe_float(raw_value)
        if value is None:
            n_missing += 1
            continue

        count += 1
        total += value
        minimum = value if minimum is None else min(minimum, value)
        maximum = value if maximum is None else max(maximum, value)

    mean = (total / count) if count else None
    return minimum, maximum, mean, n_missing


def quality_verification(records: list[dict]) -> dict:
    """
    % of records missing/empty close_approach_data and absolute_magnitude_h,
    plus a check that is_potentially_hazardous_asteroid is present and
    boolean-typed on every record.
    """
    n = len(records)
    n_missing_approach = sum(1 for r in records if not r.get("close_approach_data"))
    n_missing_h = sum(1 for r in records if r.get("absolute_magnitude_h") is None)
    n_hazard_present_and_bool = sum(
        1 for r in records if isinstance(r.get("is_potentially_hazardous_asteroid"), bool)
    )

    return {
        "pct_missing_close_approach_data": (n_missing_approach / n * 100) if n else 0.0,
        "pct_missing_absolute_magnitude_h": (n_missing_h / n * 100) if n else 0.0,
        "n_hazard_flag_present_and_bool": n_hazard_present_and_bool,
        "n_total": n,
    }


# --------------------------------------------------------------------------
# Section 5: Phase 3 — Data Preparation
# --------------------------------------------------------------------------

def filter_cohort(records: list[dict]) -> list[dict]:
    """Drop any record with an empty close_approach_data list."""
    cohort = [r for r in records if r.get("close_approach_data")]
    print(f"[filter_cohort] {len(cohort)}/{len(records)} records retained (have a close approach).")
    return cohort


def _median(values: list[float]) -> float | None:
    """Simplified median (middle element of the sorted list; no averaging on even length)."""
    if not values:
        return None
    return sorted(values)[len(values) // 2]


def build_clean_records(cohort: list[dict], total_known_neos: int | None) -> list[dict]:
    """
    Clean, impute, and feature-engineer each NEO in the cohort into a flat
    dict ready to be joined with the ground-station log and written to CSV.
    """
    # First pass: collect non-null absolute_magnitude_h for median imputation.
    h_values = [
        safe_float(r.get("absolute_magnitude_h"))
        for r in cohort
        if safe_float(r.get("absolute_magnitude_h")) is not None
    ]
    h_median = _median(h_values)

    clean = []
    for record in cohort:
        approach = _first_close_approach(record)
        if approach is None:
            continue  # already filtered by filter_cohort, but stay defensive

        diameter = record.get("estimated_diameter", {}).get("kilometers", {})
        max_diameter_km = safe_float(diameter.get("estimated_diameter_max"))
        min_diameter_km = safe_float(diameter.get("estimated_diameter_min"))

        # NASA returns these as quoted strings, unlike estimated_diameter's
        # real numbers — every one needs an explicit safe_float cast.
        miss_distance_km = safe_float(approach.get("miss_distance", {}).get("kilometers"))
        miss_distance_lunar = safe_float(approach.get("miss_distance", {}).get("lunar"))
        relative_velocity_kph = safe_float(
            approach.get("relative_velocity", {}).get("kilometers_per_hour")
        )

        if max_diameter_km is None or miss_distance_lunar is None:
            continue  # can't score an object without size or distance

        absolute_magnitude_h = safe_float(record.get("absolute_magnitude_h"))
        if absolute_magnitude_h is None:
            absolute_magnitude_h = h_median  # rare-missing-field median imputation

        num_close_approaches_in_window = len(record.get("close_approach_data") or [])

        size_to_distance_ratio = (
            (max_diameter_km / miss_distance_lunar) if miss_distance_lunar else None
        )

        if miss_distance_lunar <= 5:
            approach_category = "very_close"
        elif miss_distance_lunar <= 20:
            approach_category = "close"
        elif miss_distance_lunar <= 60:
            approach_category = "moderate"
        else:
            approach_category = "distant"

        priority_watch = 1 if (max_diameter_km >= SIZE_THRESHOLD_KM and miss_distance_lunar <= CLOSE_LD_THRESHOLD) else 0

        clean.append({
            "neo_id": str(record.get("neo_reference_id", "")),
            "name": record.get("name"),
            "estimated_diameter_max_km": max_diameter_km,
            "estimated_diameter_min_km": min_diameter_km,
            "miss_distance_km": miss_distance_km,
            "miss_distance_lunar": miss_distance_lunar,
            "relative_velocity_kph": relative_velocity_kph,
            "absolute_magnitude_h": absolute_magnitude_h,
            "num_close_approaches_in_window": num_close_approaches_in_window,
            "is_potentially_hazardous_asteroid": record.get("is_potentially_hazardous_asteroid"),
            "size_to_distance_ratio": size_to_distance_ratio,
            "approach_category": approach_category,
            "priority_watch": priority_watch,
            "total_known_neos": total_known_neos,
        })

    return clean


def load_ground_station_log(path: Path = GROUND_STATION_LOG_PATH) -> dict[str, dict]:
    """Load ground_station_log.csv into a dict keyed by neo_id (string)."""
    log_by_id = {}
    try:
        with path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            log_by_id = {row["neo_id"]: row for row in reader}
    except FileNotFoundError:
        print(f"[load_ground_station_log] {path} not found — run generate_sentinel_log.py first.")
    return log_by_id


def join_with_ground_station_log(clean_records: list[dict], log_by_id: dict[str, dict]) -> list[dict]:
    """Attach whatever ground-station log fields exist for each record's neo_id."""
    matched = 0
    for record in clean_records:
        log_row = log_by_id.get(record["neo_id"])
        if log_row:
            matched += 1
        record["observatory_code"] = log_row.get("observatory_code") if log_row else None
        record["confidence_score"] = log_row.get("confidence_score") if log_row else None

    print(f"[join_with_ground_station_log] {matched}/{len(clean_records)} records matched a ground-station log row.")
    return clean_records


def min_max_scale(records: list[dict], field: str, new_field: str) -> list[dict]:
    """Min-max scale `field` into `new_field` across all records."""
    values = [r[field] for r in records if isinstance(r.get(field), (int, float))]
    if not values:
        for r in records:
            r[new_field] = None
        return records

    min_x, max_x = min(values), max(values)
    span = max_x - min_x

    for r in records:
        x = r.get(field)
        if not isinstance(x, (int, float)):
            r[new_field] = None
        elif span == 0:
            r[new_field] = 0.0
        else:
            r[new_field] = (x - min_x) / span

    return records


def validation_crosstab(records: list[dict]) -> dict[str, int]:
    """
    2x2 crosstab of `priority_watch` (bool-like 0/1) against NASA's own
    `is_potentially_hazardous_asteroid`, counting agreement/disagreement in
    each direction.
    """
    counts = {
        (True, True): 0, (True, False): 0,
        (False, True): 0, (False, False): 0,
    }
    for r in records:
        own_flag = bool(r.get("priority_watch"))
        nasa_flag = bool(r.get("is_potentially_hazardous_asteroid"))
        counts[(own_flag, nasa_flag)] += 1

    return {
        "both_flagged": counts[(True, True)],
        "priority_watch_only": counts[(True, False)],
        "nasa_hazardous_only": counts[(False, True)],
        "neither_flagged": counts[(False, False)],
    }


def write_clean_csv(records: list[dict], path: Path = CLEAN_DATA_PATH) -> None:
    """Write the cleaned, feature-engineered, joined, scaled records to CSV."""
    if not records:
        print("[write_clean_csv] No records to write.")
        return

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    fieldnames = list(records[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"[write_clean_csv] Wrote {len(records)} records -> {path}")


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def run_pipeline(date_windows: list[tuple[str, str]] = None, api_key: str = API_KEY) -> list[dict]:
    """Run the full Project Sentinel pipeline end-to-end."""
    from generate_sentinel_log import generate_sentinel_log  # local import, repo root

    records = fetch_neos(date_windows, api_key)
    if not records:
        print("[run_pipeline] No NEOs returned; aborting.")
        return []

    neo_ids = extract_and_write_ids(records)
    generate_sentinel_log(neo_ids, output_path=GROUND_STATION_LOG_PATH)

    total_known_neos = scrape_total_known_neos()

    cohort = filter_cohort(records)
    clean_records = build_clean_records(cohort, total_known_neos)

    log_by_id = load_ground_station_log(GROUND_STATION_LOG_PATH)
    clean_records = join_with_ground_station_log(clean_records, log_by_id)

    clean_records = min_max_scale(clean_records, "size_to_distance_ratio", "size_to_distance_ratio_scaled")

    crosstab = validation_crosstab(clean_records)
    print(f"[run_pipeline] Validation crosstab (priority_watch vs is_potentially_hazardous_asteroid): {crosstab}")

    n_flagged = sum(1 for r in clean_records if r["priority_watch"] == 1)
    n_total = len(clean_records)
    pct_workload_reduction = (1 - (n_flagged / n_total)) * 100 if n_total else 0.0
    print(
        f"[run_pipeline] {n_flagged}/{n_total} objects flagged priority_watch==1 "
        f"-> pct_workload_reduction = {pct_workload_reduction:.1f}%"
    )

    if total_known_neos:
        pct_of_catalogue = (n_total / total_known_neos) * 100
        print(f"[run_pipeline] This pull represents {pct_of_catalogue:.4f}% of all {total_known_neos} known NEOs.")

    write_clean_csv(clean_records, CLEAN_DATA_PATH)
    return clean_records


if __name__ == "__main__":
    run_pipeline()
