# Project Sentinel — Automated Near-Earth Object Triage

Aerospace / Planetary Defense. Mirrors the weekly triage workflow of a planetary-defense analyst: pull
the live catalog of newly-tracked close-approach objects, reconcile it against internal ground-station
records, and pre-flag which objects deserve a human's attention before the rest get routine review.

> **Run status:** this repo ships fully implemented, tested code (`src/pipeline.py`,
> `generate_sentinel_log.py`, `notebooks/exploration.ipynb`). The numbers marked `RUN_ME` below are
> filled in automatically the first time you run the pipeline with internet access — see **How to Run**.
> All parsing/join/scaling logic has been verified offline against synthetic data reproducing every
> messiness case in the brief (string-typed `miss_distance`/`relative_velocity`, empty
> `close_approach_data`, missing `absolute_magnitude_h`, ghost/dropped ids).

## 1. Objectives

Build an automated pre-triage layer for a planetary-defense close-approach review. Given this week's
tracked near-Earth objects (NEOs), flag the subset that combines meaningful size with a close pass, so an
analyst's limited review time goes to the objects that matter most — not the full list.

## 2. Resource Audit

| Resource | Detail |
|---|---|
| API access | Free NASA API key — instant signup at api.nasa.gov (name + email, no card) |
| Rate limit | `DEMO_KEY`: 30 req/hour, 50/day, shared across your IP — can exhaust fast on a shared network. A personal key raises this to 1,000/hour |
| Data sources | NASA NeoWs feed endpoint (2 chained calls) · `data/raw/ground_station_log.csv` (generated, never downloaded) · 1 web scrape (science.nasa.gov) |
| Estimated time | 4–6 hours across all three phases |

## 3. Target Definition

```
priority_watch = 1 if (max_estimated_diameter_km >= 0.14) AND (miss_distance_lunar <= 10)
priority_watch = 0 otherwise
```

0.14 km (140 m) is NASA's own NEO Observations Program survey-completeness benchmark for hazardous-size
objects. 10 lunar distances (1 LD ≈ 384,400 km) is a standard "notably close" cutoff in close-approach
reporting. This is deliberately a *different* rule than NASA's own `is_potentially_hazardous_asteroid`
flag (defined by minimum orbit intersection distance and absolute magnitude) — an independent classifier,
compared directly against NASA's own flag in the Phase 3 Validation Check.

## 4. Brainstormed Features

1. `estimated_diameter_max_km`
2. `estimated_diameter_min_km`
3. `miss_distance_km`
4. `miss_distance_lunar`
5. `relative_velocity_kph`
6. `absolute_magnitude_h`
7. `num_close_approaches_in_window`
8. `confidence_score` (from the ground-station log)

## 5. ROI Framework

```
pct_workload_reduction = (1 - (n_flagged / n_total)) * 100
```

`n_total` (every object in the cleaned pull) and `n_flagged` (`priority_watch == 1`) are computed directly
from the cleaned dataset.

> **RUN_ME:** after running `python src/pipeline.py`, paste the printed
> `n_flagged/n_total -> pct_workload_reduction` line here, e.g.:
> "An analyst who only manually reviews `priority_watch == 1` objects out of NN pulled this week cuts
> their weekly review set by NN.N%."

### Bonus catalogue-share insight

`total_known_neos` is scraped live from NASA's Planetary Defense page and attached as a constant column.

> **RUN_ME:** paste the printed `pct_of_catalogue` line here, e.g.: "This week's pull of NN objects
> represents NN.NNNN% of every NEO ever catalogued (out of NN total known NEOs as of this scrape)."

## 6. Data Quality Notes (from Phase 2 exploration)

- `near_earth_objects` in the API response is a **dict keyed by date string**, not a flat list —
  `fetch_neos()` loops over `.items()` for each of the two chained 7-day windows before ever reaching an
  actual object.
- `estimated_diameter` values are real JSON numbers, but `relative_velocity` and `miss_distance` (nested
  inside `close_approach_data`) are the same kind of numeric data returned as **quoted strings**. Every
  cast goes through a reusable `safe_float(value, default=None)` helper wrapped in try/except, so a bad
  cast never crashes the pipeline.
- `close_approach_data` is a list, usually length 1 in a short window — never indexed with `[0]`
  unguarded; an object with no close approach in the window returns an empty list, filtered out entirely
  in the Phase 3 cohort step.
- **Imputation:** the rare record missing `absolute_magnitude_h` is imputed with the cohort median (a
  simplified `sorted(values)[len(values) // 2]`, not averaging the two middle values on an even-length
  list) rather than dropped, since size/distance data is still usable without it.

## 7. Validation Check (Phase 3)

`validation_crosstab()` cross-tabulates `priority_watch` against NASA's own
`is_potentially_hazardous_asteroid` in a 2×2 count (`both_flagged`, `priority_watch_only`,
`nasa_hazardous_only`, `neither_flagged`).

> **RUN_ME:** paste the printed crosstab here and interpret it. The two flags measure different things on
> purpose: `priority_watch` is a pure size-and-distance rule, while NASA's hazardous flag is defined by
> minimum orbit intersection distance (MOID) and absolute magnitude — an object can be large and close in
> *this* week's window without having a historically hazardous MOID, and vice versa, so disagreement in
> both directions is expected rather than a bug in either rule.

## 8. How to Run

```bash
pip install requests
python src/pipeline.py
```

Before running, either accept the built-in `DEMO_KEY` (fine for a one-off run, subject to the 30/hour
shared limit) or set `API_KEY` in `src/pipeline.py` to your own free key from api.nasa.gov.

This will, in order:

1. Pull two chained 7-day NEO windows from the NeoWs feed and merge them (`fetch_neos`).
2. Write `data/raw/extracted_ids.txt` (de-duplicated `neo_reference_id`s) and generate the messy
   `data/raw/ground_station_log.csv` from those same ids (`generate_sentinel_log.py`).
3. Scrape the live total-known-NEOs figure from NASA's Planetary Defense page (falls back to a rough
   constant if the scrape or request fails, so the pipeline never hard-crashes on a network hiccup).
4. Filter the cohort to objects with a close approach, safely cast every string-typed numeric field,
   impute the rare missing `absolute_magnitude_h` with the cohort median, and engineer
   `size_to_distance_ratio` / `approach_category` / `priority_watch`.
5. Join against the ground-station log by `neo_id` (handles ids missing in either direction).
6. Min-max scale `size_to_distance_ratio` into `size_to_distance_ratio_scaled`.
7. Print the validation crosstab, ROI number, and catalogue-share insight, and write
   `data/processed/clean_data.csv`.

Then open `notebooks/exploration.ipynb` (same live API pull, plus the structural/EDA audits) and fill in
the `RUN_ME` numbers above. Adjust `DEFAULT_DATE_WINDOWS` in `src/pipeline.py` to a recent pair of 7-day
windows before running, if you want fresher data than the one shipped as the default.

## 9. Repository Layout

```
project_repo_sentinel/
├── data/
│   ├── raw/            # extracted_ids.txt + generated ground_station_log.csv
│   └── processed/      # clean_data.csv — pipeline output
├── notebooks/
│   └── exploration.ipynb
├── src/
│   └── pipeline.py     # importable AND runnable end-to-end
├── generate_sentinel_log.py
└── README.md
```
