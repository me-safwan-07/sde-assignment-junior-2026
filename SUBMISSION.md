# Submission

## Candidate
- **Name:** Amrin
- **Assignment chosen:** A (Data)
- **Time spent (honest estimate):** ~7h

## How to run
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
python -m solution.assignment_a ingest data/drop_1
python -m solution.assignment_a ingest data/drop_2
python -m solution.assignment_a report --out reports
python -m pytest -q
```

If you are using cmd.exe instead of PowerShell:
```cmd
python -m venv .venv
.venv\Scripts\activate.bat
pip install -r requirements.txt
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
python -m solution.assignment_a ingest data/drop_1
python -m solution.assignment_a ingest data/drop_2
python -m solution.assignment_a report --out reports
python -m pytest -q
```

## Design
I built a SQLite-backed ingestion pipeline for Assignment A. The database stores: one row per unique lead keyed by `(workflow_id, canonical_phone)`, one row per rejected lead row, and one row per valid event keyed by `event_id`. This keeps the pipeline idempotent and lets me generate the final reports from a consistent ledger instead of transient in-memory state.

For leads, each row is validated against the workflow schema in field order, normalised, and merged against existing identities so the first valid row wins and later duplicates overwrite only non-null fields. For events, I deduplicate by `event_id`, ignore malformed and unknown-disposition entries, and link remaining rows to valid leads by `(workflow_id, normalised_phone)` with `call_id` fallback for overrides. Because reports are generated from the final database state, events that were orphaned early become valid when their lead arrives in a later drop.

## Decisions & assumptions
| Decision | Why | Alternative considered |
|---|---|---|
| Rejected rows are stored in `rejections.csv` with local file row numbers | The contract requires `row_number` to count data rows within each file | Keeping a global row counter would be wrong for per-file outputs |
| Event linking is resolved at report time instead of at ingest time | This matches the requirement that events can arrive before their lead and still be counted once the lead appears later | Immediate orphan rejection would be incorrect under the final-state contract |
| I keep only the canonical phone in the lead key and not plaintext PII | The schema marks phone as PII, so the ledger stores a canonical phone for matching while the outputs mask it | Keeping raw phone values would violate the PII-at-rest requirement |

## Failure modes considered
- Bad lead rows: missing required values, invalid phones, malformed dates/currency, invalid enums, duplicate values.
- Re-runs: the ingest path is recorded in `ingested_drops`, so the same drop is skipped.
- Duplicate event delivery: dedupe by `event_id` across all ingested drops.
- Orphan events: they are counted in `data_quality.json` only when they remain unlinked in the final database state.
- Unknown dispositions: ignored and counted in the report, as required.
- Not fully handled: the code does not attempt multi-process coordination for high-volume ingestion; for the assignment's small/medium scale this is acceptable.

## Testing
- Phone normalisation and date/decimal parsing tests.
- End-to-end ingest + reporting test on the generated small dataset using `pytest`.
- I did not test the large-scale dataset because the time budget and the repo scope focused on correctness and contract compliance.

## Results
- The CLI successfully generates all five report files on the small seeded dataset.
- `data_quality.json` reflects final ingest totals, including malformed lines, duplicates, and orphan events.
- The generated reports match the expected structure and ordering semantics for batch summaries, daily rollups, lead reports, rejections, and quality counts.

## If I had another day
1. Add more exhaustive contract-level fixtures to compare generated outputs against expected CSV/JSON snapshots.
2. Validate the large `--scale large` dataset and benchmark ingestion/report runtime.
3. Refactor the report logic into tighter helper functions for readability and easier edge-case testing.
