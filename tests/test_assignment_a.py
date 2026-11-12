import json
import subprocess
import sys
from datetime import date
from decimal import Decimal

from solution.assignment_a import PipelineDB, parse_date, parse_decimal, parse_phone


def test_phone_and_date_parsing():
    assert parse_phone("91-77187-50036") == "+917718750036"
    assert parse_phone("0-9876543210") == "+919876543210"
    assert parse_phone("N/A") is None
    assert parse_date("01-Dec-2026") == date(2026, 12, 1)
    assert parse_date("31/02/2026") is None
    assert parse_decimal("Rs. 1,234.50") == Decimal("1234.50")
    assert parse_decimal("-1") is None


def test_end_to_end_ingest_and_report(tmp_path):
    data_dir = tmp_path / "data"
    subprocess.run(
        [
            sys.executable,
            "assignment-a-data/tools/generate_data.py",
            "--seed",
            "42",
            "--scale",
            "small",
            "--out",
            str(data_dir),
        ],
        check=True,
        cwd="C:/Users/amrin/Desktop/sde-assignment-junior-2026",
    )

    db_path = tmp_path / "pipeline.db"
    db = PipelineDB(str(db_path))
    db.ingest_drop(str(data_dir / "drop_1"))
    db.ingest_drop(str(data_dir / "drop_2"))
    out_dir = tmp_path / "reports"
    db.build_reports(str(out_dir))

    assert (out_dir / "batch_summary.csv").exists()
    assert (out_dir / "daily_overview.csv").exists()
    assert (out_dir / "lead_report.csv").exists()
    assert (out_dir / "rejections.csv").exists()
    assert (out_dir / "data_quality.json").exists()

    quality = json.loads((out_dir / "data_quality.json").read_text(encoding="utf-8"))
    assert quality["lead_rows_total"] > 0
    assert quality["events_total"] > 0
