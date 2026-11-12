#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sqlite3
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any

IST = timezone(timedelta(hours=5, minutes=30))


def normalize_header(value: str | None) -> str:
    if value is None:
        return ""
    v = value.strip().lower()
    v = re.sub(r"[_\s]+", " ", v)
    return v.strip()


def parse_phone(value: str | None) -> str | None:
    if value is None:
        return None
    digits = re.sub(r"\D+", "", value.strip())
    if not digits:
        return None
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if len(digits) != 10 or digits[0] not in "6789":
        return None
    return f"+91{digits}"


def parse_date(value: str | None) -> date | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    formats = ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d-%b-%Y", "%d-%B-%Y"]
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        return None


def parse_decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    text = re.sub(r"(?i)^\s*(?:₹|rs\.?|inr)\s*", "", text)
    text = text.replace(",", "")
    text = text.replace(" ", "")
    if text.startswith("(") and text.endswith(")"):
        text = "-" + text[1:-1]
    try:
        dec = Decimal(text)
    except InvalidOperation:
        return None
    if dec < 0:
        return None
    return dec.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def normalize_enum(value: str, choices: list[str]) -> str | None:
    if value is None:
        return None
    text = value.strip().lower()
    lookup = {choice.lower(): choice.lower() for choice in choices}
    return lookup.get(text)


def load_schema(workflow_id: str) -> dict[str, Any]:
    root = Path(__file__).resolve().parent.parent
    with open(root / "assignment-a-data" / "schemas" / f"{workflow_id}.json", "r", encoding="utf-8") as fh:
        return json.load(fh)


def dispositions_by_workflow() -> dict[str, dict[str, dict[str, Any]]]:
    result: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    root = Path(__file__).resolve().parent.parent
    with open(root / "assignment-a-data" / "schemas" / "dispositions.csv", "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            result[row["workflow_id"]][row["code"].lower()] = {
                "is_terminal": row["is_terminal"].strip().lower() == "true",
                "is_success": row["is_success"].strip().lower() == "true",
            }
    return result


def parse_utc_or_ist(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        if "T" in text:
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=IST)
            return dt.astimezone(IST)
        if " " in text:
            return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
        return datetime.fromisoformat(text).replace(tzinfo=IST)
    except ValueError:
        return None


def format_ist_iso(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.astimezone(IST).isoformat(timespec="seconds")


def mask_phone(phone: str | None) -> str:
    if not phone:
        return ""
    digits = re.sub(r"\D", "", phone)
    if len(digits) >= 4:
        return f"+91******{digits[-4:]}"
    return "+91******"


class PipelineDB:
    def __init__(self, db_path: str = "assignment_a.sqlite3") -> None:
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.ensure_schema()

    def ensure_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ingested_drops (
                drop_path TEXT PRIMARY KEY,
                ingested_at TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS leads (
                workflow_id TEXT NOT NULL,
                canonical_phone TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (workflow_id, canonical_phone)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS rejections (
                workflow_id TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                row_number INTEGER NOT NULL,
                reason TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_rows (
                event_id TEXT PRIMARY KEY,
                workflow_id TEXT,
                event_type TEXT,
                call_id TEXT,
                occurred_at TEXT,
                phone TEXT,
                payload_json TEXT,
                raw_json TEXT
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS quality_stats (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    def stat(self, key: str) -> int:
        row = self.conn.execute("SELECT value FROM quality_stats WHERE key = ?", (key,)).fetchone()
        if row is None:
            return 0
        try:
            return int(row["value"])
        except ValueError:
            return 0

    def set_stat(self, key: str, value: int) -> None:
        self.conn.execute(
            "INSERT INTO quality_stats(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(int(value))),
        )

    def inc_stat(self, key: str, delta: int = 1) -> None:
        self.set_stat(key, self.stat(key) + delta)

    def ingest_drop(self, drop_dir: str) -> None:
        drop_dir = os.path.abspath(drop_dir)
        if self.conn.execute("SELECT 1 FROM ingested_drops WHERE drop_path = ?", (drop_dir,)).fetchone():
            return

        catalog = dispositions_by_workflow()
        seen_event_ids = {row["event_id"] for row in self.conn.execute("SELECT event_id FROM event_rows").fetchall()}
        for workflow_name in sorted(os.listdir(os.path.join(drop_dir, "leads"))) if os.path.isdir(os.path.join(drop_dir, "leads")) else []:
            workflow_path = os.path.join(drop_dir, "leads", workflow_name)
            if not os.path.isdir(workflow_path):
                continue
            schema = load_schema(workflow_name)
            fields = schema["fields"]
            for csv_path in sorted(Path(workflow_path).glob("*.csv"), key=lambda p: p.name):
                with open(csv_path, "r", encoding="utf-8", newline="") as fh:
                    reader = csv.DictReader(fh)
                    header_aliases = {}
                    if reader.fieldnames:
                        for header in reader.fieldnames:
                            if header is not None:
                                header_aliases[normalize_header(header)] = header
                    file_row_number = 0
                    for row in reader:
                        if row is None:
                            continue
                        if all((value is None or str(value).strip() == "") for value in row.values()):
                            continue
                        file_row_number += 1
                        self.inc_stat("lead_rows_total")
                        row_number = file_row_number
                        values: dict[str, Any] = {}
                        reason = None
                        for field in fields:
                            field_name = field["name"]
                            matched_header = None
                            for candidate in [field_name, *field.get("aliases", [])]:
                                normalized = normalize_header(candidate)
                                if normalized in header_aliases:
                                    matched_header = header_aliases[normalized]
                                    break
                            raw = row.get(matched_header, "") if matched_header is not None else ""
                            cleaned = str(raw).strip() if raw is not None else ""
                            if cleaned == "":
                                if field.get("required"):
                                    reason = f"missing_required:{field_name}"
                                    break
                                values[field_name] = None
                                continue
                            if field["type"] == "string":
                                values[field_name] = cleaned
                            elif field["type"] == "phone":
                                converted = parse_phone(cleaned)
                                if converted is None:
                                    if field.get("required"):
                                        reason = "invalid_phone"
                                        break
                                    values[field_name] = None
                                    self.inc_stat("field_warnings")
                                else:
                                    values[field_name] = converted
                            elif field["type"] == "date":
                                converted = parse_date(cleaned)
                                if converted is None:
                                    if field.get("required"):
                                        reason = f"missing_required:{field_name}"
                                        break
                                    values[field_name] = None
                                    self.inc_stat("field_warnings")
                                else:
                                    values[field_name] = converted.isoformat()
                            elif field["type"] == "decimal":
                                converted = parse_decimal(cleaned)
                                if converted is None:
                                    if field.get("required"):
                                        reason = f"missing_required:{field_name}"
                                        break
                                    values[field_name] = None
                                    self.inc_stat("field_warnings")
                                else:
                                    values[field_name] = str(converted)
                            elif field["type"] == "enum":
                                converted = normalize_enum(cleaned, field.get("values", []))
                                if converted is None:
                                    if field.get("required"):
                                        reason = f"missing_required:{field_name}"
                                        break
                                    values[field_name] = None
                                    self.inc_stat("field_warnings")
                                else:
                                    values[field_name] = converted
                            else:
                                values[field_name] = cleaned
                        if reason is not None:
                            self.inc_stat("lead_rows_rejected")
                            self.conn.execute(
                                "INSERT INTO rejections(workflow_id, batch_id, row_number, reason) VALUES (?, ?, ?, ?)",
                                (workflow_name, csv_path.stem, row_number, reason),
                            )
                            continue
                        phone = values.get("phone")
                        if not phone:
                            self.inc_stat("lead_rows_rejected")
                            self.conn.execute(
                                "INSERT INTO rejections(workflow_id, batch_id, row_number, reason) VALUES (?, ?, ?, ?)",
                                (workflow_name, csv_path.stem, row_number, "invalid_phone"),
                            )
                            continue
                        identity = (workflow_name, phone)
                        existing = self.conn.execute(
                            "SELECT payload_json FROM leads WHERE workflow_id = ? AND canonical_phone = ?",
                            identity,
                        ).fetchone()
                        if existing is None:
                            payload = {**values, "batch_id": csv_path.stem, "workflow_id": workflow_name}
                            self.conn.execute(
                                "INSERT INTO leads(workflow_id, canonical_phone, batch_id, payload_json) VALUES (?, ?, ?, ?)",
                                (workflow_name, phone, csv_path.stem, json.dumps(payload, ensure_ascii=False)),
                            )
                        else:
                            payload = json.loads(existing["payload_json"])
                            for key, item in values.items():
                                if item is not None:
                                    payload[key] = item
                            payload["batch_id"] = payload.get("batch_id") or csv_path.stem
                            payload["workflow_id"] = workflow_name
                            self.inc_stat("duplicate_lead_rows_merged")
                            self.conn.execute(
                                "UPDATE leads SET payload_json = ? WHERE workflow_id = ? AND canonical_phone = ?",
                                (json.dumps(payload, ensure_ascii=False), workflow_name, phone),
                            )
                        for field in fields:
                            if field.get("default") is not None:
                                current = self.conn.execute(
                                    "SELECT payload_json FROM leads WHERE workflow_id = ? AND canonical_phone = ?",
                                    identity,
                                ).fetchone()
                                if current is not None:
                                    payload = json.loads(current["payload_json"])
                                    if payload.get(field["name"]) is None:
                                        payload[field["name"]] = field["default"]
                                        self.conn.execute(
                                            "UPDATE leads SET payload_json = ? WHERE workflow_id = ? AND canonical_phone = ?",
                                            (json.dumps(payload, ensure_ascii=False), workflow_name, phone),
                                        )
                        self.conn.commit()

        for event_file in sorted(Path(drop_dir).joinpath("events").glob("*.jsonl")) if os.path.isdir(os.path.join(drop_dir, "events")) else []:
            with open(event_file, "r", encoding="utf-8") as fh:
                for raw_line in fh:
                    if not raw_line.strip():
                        continue
                    self.inc_stat("events_total")
                    try:
                        obj = json.loads(raw_line)
                    except json.JSONDecodeError:
                        self.inc_stat("malformed_events")
                        continue
                    event_id = obj.get("event_id")
                    if not event_id:
                        self.inc_stat("malformed_events")
                        continue
                    if event_id in seen_event_ids:
                        self.inc_stat("duplicate_events_dropped")
                        continue
                    seen_event_ids.add(event_id)
                    workflow_id = obj.get("workflow_id")
                    event_type = obj.get("event_type")
                    payload = obj.get("payload") or {}
                    if event_type and event_type.startswith("disposition.") and workflow_id:
                        code = payload.get("disposition_code")
                        if code and code.lower() not in set(catalog.get(workflow_id, {}).keys()):
                            self.inc_stat("unknown_disposition_events")
                            continue
                    self.conn.execute(
                        "INSERT INTO event_rows(event_id, workflow_id, event_type, call_id, occurred_at, phone, payload_json, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            event_id,
                            workflow_id,
                            event_type,
                            obj.get("call_id"),
                            obj.get("occurred_at"),
                            obj.get("phone"),
                            json.dumps(payload, ensure_ascii=False),
                            json.dumps(obj, ensure_ascii=False),
                        ),
                    )
                    self.conn.commit()

        self.conn.execute(
            "INSERT INTO ingested_drops(drop_path, ingested_at) VALUES (?, ?) ON CONFLICT(drop_path) DO NOTHING",
            (drop_dir, datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")),
        )
        self.conn.commit()

    def build_reports(self, output_dir: str) -> None:
        os.makedirs(output_dir, exist_ok=True)
        dispositions = dispositions_by_workflow()
        lead_rows = self.conn.execute("SELECT workflow_id, canonical_phone, batch_id, payload_json FROM leads ORDER BY workflow_id, canonical_phone").fetchall()
        lead_lookup: dict[tuple[str, str], dict[str, Any]] = {}
        for row in lead_rows:
            payload = json.loads(row["payload_json"])
            lead_lookup[(row["workflow_id"], row["canonical_phone"])] = payload

        # Link events to valid leads, finally.
        orphan_events = 0
        linked_events: list[dict[str, Any]] = []
        phone_to_lead: dict[tuple[str, str], tuple[str, str]] = {}
        call_to_lead: dict[tuple[str, str], tuple[str, str]] = {}
        event_rows = self.conn.execute("SELECT event_id, workflow_id, event_type, call_id, occurred_at, phone, payload_json FROM event_rows ORDER BY occurred_at").fetchall()
        for row in event_rows:
            payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
            if row["event_type"] and row["event_type"].startswith("disposition."):
                code = payload.get("disposition_code")
                if row["workflow_id"] and code and code.lower() not in set(dispositions.get(row["workflow_id"], {}).keys()):
                    continue
            if row["event_type"] == "disposition.override":
                call_id = row["call_id"]
                if not call_id:
                    orphan_events += 1
                    continue
                lead_key = call_to_lead.get((row["workflow_id"], call_id))
                if lead_key is None:
                    orphan_events += 1
                    continue
                linked_events.append({
                    "lead_key": lead_key,
                    "workflow_id": row["workflow_id"],
                    "event_type": row["event_type"],
                    "call_id": call_id,
                    "occurred_at": row["occurred_at"],
                    "payload": payload,
                })
                continue
            phone = row["phone"]
            if not phone:
                orphan_events += 1
                continue
            canonical = parse_phone(phone)
            if canonical is None:
                orphan_events += 1
                continue
            lead_key = (row["workflow_id"], canonical)
            if lead_key not in lead_lookup:
                orphan_events += 1
                continue
            phone_to_lead[(row["workflow_id"], canonical)] = lead_key
            if row["call_id"]:
                call_to_lead[(row["workflow_id"], row["call_id"])] = lead_key
            linked_events.append({
                "lead_key": lead_key,
                "workflow_id": row["workflow_id"],
                "event_type": row["event_type"],
                "call_id": row["call_id"],
                "occurred_at": row["occurred_at"],
                "payload": payload,
            })

        lead_summary: dict[tuple[str, str], dict[str, Any]] = {}
        for lead_key, payload in lead_lookup.items():
            workflow_id, phone = lead_key
            calls_for_lead: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for ev in linked_events:
                if ev["lead_key"] == lead_key and ev["call_id"]:
                    calls_for_lead[ev["call_id"]].append(ev)
            call_records: list[dict[str, Any]] = []
            for call_id, evs in calls_for_lead.items():
                initiated_times = [parse_utc_or_ist(ev["occurred_at"]) for ev in evs if ev["event_type"] == "call.initiated" and parse_utc_or_ist(ev["occurred_at"]) is not None]
                initiated = max(initiated_times) if initiated_times else None
                connected = any(ev["event_type"] == "call.connected" for ev in evs)
                latest_override = None
                latest_set = None
                for ev in evs:
                    if ev["event_type"] == "disposition.override":
                        code = str((ev["payload"] or {}).get("disposition_code", "")).lower()
                        ts = parse_utc_or_ist(ev["occurred_at"])
                        if code and ts is not None:
                            if latest_override is None or ts > latest_override[0]:
                                latest_override = (ts, code)
                    elif ev["event_type"] == "disposition.set":
                        code = str((ev["payload"] or {}).get("disposition_code", "")).lower()
                        ts = parse_utc_or_ist(ev["occurred_at"])
                        if code and ts is not None:
                            if latest_set is None or ts > latest_set[0]:
                                latest_set = (ts, code)
                effective = latest_override[1] if latest_override else (latest_set[1] if latest_set else None)
                call_records.append({
                    "call_id": call_id,
                    "initiated_at": initiated,
                    "connected": connected,
                    "effective_disposition": effective,
                    "events": evs,
                })
            summary = {
                "calls": len(call_records),
                "connected": sum(1 for c in call_records if c["connected"]),
                "latest_call_at": max((c["initiated_at"] for c in call_records if c["initiated_at"] is not None), default=None),
                "current_disposition": None,
                "ever_success": False,
                "bucket": "yet_to_reach_out",
            }
            if call_records:
                latest = max((c["initiated_at"] for c in call_records if c["initiated_at"] is not None), default=None)
                candidates = [(c["effective_disposition"], c["initiated_at"]) for c in call_records if c["effective_disposition"] and c["initiated_at"] is not None]
                if candidates:
                    summary["current_disposition"] = max(candidates, key=lambda item: item[1])[0]
                    summary["ever_success"] = any(dispositions.get(workflow_id, {}).get(code, {}).get("is_success") for code, _ in candidates)
                if any(dispositions.get(workflow_id, {}).get(c["effective_disposition"], {}).get("is_terminal") for c in call_records if c["effective_disposition"]):
                    summary["bucket"] = "completed"
                elif all(any(ev["event_type"] == "call.telephony_failed" for ev in c["events"]) for c in call_records):
                    summary["bucket"] = "in_progress_telephony_failed"
                else:
                    summary["bucket"] = "in_progress_dialed"
            lead_summary[lead_key] = summary

        batch_map: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: {"total_leads": 0, "yet_to_reach_out": 0, "in_progress_telephony_failed": 0, "in_progress_dialed": 0, "completed": 0})
        for (workflow_id, phone), payload in lead_lookup.items():
            batch_id = payload.get("batch_id") or "unknown"
            key = (workflow_id, batch_id)
            bucket = lead_summary.get((workflow_id, phone), {}).get("bucket", "yet_to_reach_out")
            batch_map[key]["total_leads"] += 1
            if bucket in batch_map[key]:
                batch_map[key][bucket] += 1

        with open(os.path.join(output_dir, "batch_summary.csv"), "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh, lineterminator="\n")
            writer.writerow(["workflow_id", "batch_id", "total_leads", "yet_to_reach_out", "in_progress_telephony_failed", "in_progress_dialed", "completed"])
            for key in sorted(batch_map):
                values = batch_map[key]
                writer.writerow([key[0], key[1], values["total_leads"], values["yet_to_reach_out"], values["in_progress_telephony_failed"], values["in_progress_dialed"], values["completed"]])

        daily_map: dict[tuple[str, str], dict[str, Any]] = defaultdict(lambda: {"total_calls": 0, "connected_calls": 0, "success_leads": set()})
        for lead_key, payload in lead_lookup.items():
            workflow_id, phone = lead_key
            for call_id, evs in defaultdict(list, {}).items():
                pass
            calls_for_lead: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for ev in linked_events:
                if ev["lead_key"] == lead_key and ev["call_id"]:
                    calls_for_lead[ev["call_id"]].append(ev)
            for call_id, evs in calls_for_lead.items():
                initiated = [parse_utc_or_ist(ev["occurred_at"]) for ev in evs if ev["event_type"] == "call.initiated" and parse_utc_or_ist(ev["occurred_at"]) is not None]
                if not initiated:
                    continue
                started = max(initiated)
                connected = any(ev["event_type"] == "call.connected" for ev in evs)
                latest_override = None
                latest_set = None
                for ev in evs:
                    if ev["event_type"] == "disposition.override":
                        ts = parse_utc_or_ist(ev["occurred_at"])
                        code = str((ev["payload"] or {}).get("disposition_code", "")).lower()
                        if ts is not None and code:
                            if latest_override is None or ts > latest_override[0]:
                                latest_override = (ts, code)
                    elif ev["event_type"] == "disposition.set":
                        ts = parse_utc_or_ist(ev["occurred_at"])
                        code = str((ev["payload"] or {}).get("disposition_code", "")).lower()
                        if ts is not None and code:
                            if latest_set is None or ts > latest_set[0]:
                                latest_set = (ts, code)
                effective = latest_override[1] if latest_override else (latest_set[1] if latest_set else None)
                date_key = started.astimezone(IST).strftime("%Y-%m-%d")
                daily_map[(workflow_id, date_key)]["total_calls"] += 1
                if connected:
                    daily_map[(workflow_id, date_key)]["connected_calls"] += 1
                if effective and dispositions.get(workflow_id, {}).get(effective, {}).get("is_success"):
                    daily_map[(workflow_id, date_key)]["success_leads"].add(lead_key)

        with open(os.path.join(output_dir, "daily_overview.csv"), "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh, lineterminator="\n")
            writer.writerow(["workflow_id", "date", "total_calls", "connected_calls", "connectivity_rate", "success_leads"])
            for (workflow_id, date_key) in sorted(daily_map):
                values = daily_map[(workflow_id, date_key)]
                total = values["total_calls"]
                connected = values["connected_calls"]
                rate = (connected / total) if total else 0.0
                writer.writerow([workflow_id, date_key, total, connected, f"{rate:.4f}", len(values["success_leads"])])

        with open(os.path.join(output_dir, "lead_report.csv"), "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh, lineterminator="\n")
            writer.writerow(["workflow_id", "external_ref", "batch_id", "phone_masked", "bucket", "total_calls", "connected_calls", "last_call_at", "current_disposition", "ever_success"])
            for (workflow_id, phone), payload in sorted(lead_lookup.items(), key=lambda item: (item[0][0], payload_to_external(item[1], item[0][0]))):
                ref_field = next((field["name"] for field in load_schema(workflow_id)["fields"] if field.get("reference")), "")
                summary = lead_summary.get((workflow_id, phone), {})
                writer.writerow([
                    workflow_id,
                    payload.get(ref_field, ""),
                    payload.get("batch_id", ""),
                    mask_phone(phone),
                    summary.get("bucket", "yet_to_reach_out"),
                    summary.get("calls", 0),
                    summary.get("connected", 0),
                    format_ist_iso(summary.get("latest_call_at")),
                    summary.get("current_disposition") or "",
                    "true" if summary.get("ever_success") else "false",
                ])

        with open(os.path.join(output_dir, "rejections.csv"), "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh, lineterminator="\n")
            writer.writerow(["workflow_id", "batch_id", "row_number", "reason"])
            for row in self.conn.execute("SELECT workflow_id, batch_id, row_number, reason FROM rejections ORDER BY workflow_id, batch_id, row_number").fetchall():
                writer.writerow([row["workflow_id"], row["batch_id"], row["row_number"], row["reason"]])

        failure_reason_counts: dict[str, int] = defaultdict(int)
        for row in self.conn.execute("SELECT reason FROM rejections").fetchall():
            failure_reason_counts[row["reason"]] += 1
        quality = {
            "lead_rows_total": self.stat("lead_rows_total"),
            "lead_rows_rejected": self.stat("lead_rows_rejected"),
            "rejections_by_reason": dict(sorted(failure_reason_counts.items())),
            "duplicate_lead_rows_merged": self.stat("duplicate_lead_rows_merged"),
            "field_warnings": self.stat("field_warnings"),
            "events_total": self.stat("events_total"),
            "malformed_events": self.stat("malformed_events"),
            "duplicate_events_dropped": self.stat("duplicate_events_dropped"),
            "orphan_events": orphan_events,
            "unknown_disposition_events": self.stat("unknown_disposition_events"),
        }
        with open(os.path.join(output_dir, "data_quality.json"), "w", encoding="utf-8") as fh:
            json.dump(quality, fh, ensure_ascii=False, indent=2)
            fh.write("\n")


def payload_to_external(payload: dict[str, Any], workflow_id: str) -> str:
    ref_field = next((field["name"] for field in load_schema(workflow_id)["fields"] if field.get("reference")), "")
    return str(payload.get(ref_field, ""))


def cmd_ingest(args: argparse.Namespace) -> None:
    PipelineDB(args.db).ingest_drop(args.drop)


def cmd_report(args: argparse.Namespace) -> None:
    PipelineDB(args.db).build_reports(args.out)


def main() -> None:
    parser = argparse.ArgumentParser(description="Assignment A lead/events pipeline")
    parser.add_argument("--db", default="assignment_a.sqlite3")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest")
    ingest.add_argument("drop")
    ingest.set_defaults(func=cmd_ingest)

    report = sub.add_parser("report")
    report.add_argument("--out", required=True)
    report.set_defaults(func=cmd_report)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
