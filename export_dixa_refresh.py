#!/usr/bin/env python3
"""
Export Dixa Conversations - Refresh for Power BI

- Fetches all telephone (channel = pstnPhone) conversations from a date range
  using /v1/search/conversations in daily batches with pagination
- Adds computed columns and writes UTF-8 CSV: conversations_ytd.csv

Usage examples:
  python export_dixa_refresh.py --ytd
  python export_dixa_refresh.py --last7
  python export_dixa_refresh.py --range 2025-06-01 2025-09-11
"""

import argparse
import sys
import time
import os
import json
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(override=True)
from datetime import datetime, timedelta, timezone, date
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd
import requests

API_KEY = os.getenv("DIXA_TOKEN"); assert API_KEY, "Set DIXA_TOKEN"
USE_BEARER = os.getenv("DIXA_USE_BEARER", "true").lower() == "true"
BASE_V1 = os.getenv("DIXA_BASE_URL", "https://api.dixa.io/v1")
BASE_EXPORTS = os.getenv("DIXA_EXPORTS_BASE", "https://exports.dixa.io/v1")

HEADERS_V1 = {
    "Authorization": (f"Bearer {API_KEY}" if USE_BEARER else API_KEY),
    "Accept": "application/json",
    "Content-Type": "application/json",
}
HEADERS_EXPORTS = {
    "Authorization": (f"Bearer {API_KEY}" if USE_BEARER else API_KEY),
    "Accept": "application/json",
}


# ----------------------------
# Date parsing and CLI options
# ----------------------------

DEFAULT_START_ISO = os.getenv("DIXA_START_ISO", "2020-01-01T00:00:00Z")
DEFAULT_END_ISO = os.getenv("DIXA_END_ISO")  # optional


def parse_iso_utc(dt_str: str) -> datetime:
    """Parse ISO string or YYYY-MM-DD into aware UTC datetime.

    - If only date is provided, interpret as YYYY-MM-DDT00:00:00Z.
    - Accepts trailing Z or timezone offsets; result converted to UTC.
    """
    dt_str = dt_str.strip()
    # Try YYYY-MM-DD first
    try:
        d = datetime.strptime(dt_str, "%Y-%m-%d").date()
        return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    except Exception:
        pass

    # Fallback to pandas for broader ISO parsing then normalize to UTC
    try:
        ts = pd.to_datetime(dt_str, utc=True)
        return ts.to_pydatetime()
    except Exception as exc:
        raise ValueError(f"Invalid date/time format: {dt_str}") from exc


def normalize_date_only(dt: datetime) -> date:
    """Return date component in UTC for iteration."""
    return dt.astimezone(timezone.utc).date()


def end_of_day_utc(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=timezone.utc)


def start_of_day_utc(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)


def format_iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def determine_range_from_cli(argv: Optional[List[str]] = None) -> Tuple[datetime, datetime, str, argparse.Namespace]:
    """Return (start_dt, end_dt, label, args) in UTC.

    Default: from 2025-01-01T00:00:00Z to now (UTC).
    """
    parser = argparse.ArgumentParser(description="Export Dixa telephone conversations to CSV")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--last7", action="store_true", help="Use last 7 days (UTC)")
    group.add_argument("--ytd", action="store_true", help="Use year-to-date from 2025-01-01 (UTC)")
    group.add_argument("--range", nargs=2, metavar=("START", "END"), help="Custom range (ISO or YYYY-MM-DD)")

    parser.add_argument("--channel", nargs="?", const="", default="",
                        help="Channel filter; empty/omitted = ALL")
    parser.add_argument("--single-file", action="store_true",
                        help="Write single CSV conversations_ytd.csv (default)")
    parser.add_argument("--daily-files", action="store_true",
                        help="Write per-day CSVs to ./data/dixa_daily/")

    args = parser.parse_args(argv)

    # Default to single-file when neither flag is provided
    if not args.single_file and not args.daily_files:
        args.single_file = True

    now_utc = datetime.now(timezone.utc)

    if args.last7:
        end_dt = now_utc
        start_dt = end_dt - timedelta(days=7)
        label = "last7"
    elif args.ytd:
        start_dt = parse_iso_utc(DEFAULT_START_ISO)
        end_dt = parse_iso_utc(DEFAULT_END_ISO) if DEFAULT_END_ISO else now_utc
        label = "ytd"
    elif args.range:
        start_dt = parse_iso_utc(args.range[0])
        end_dt = parse_iso_utc(args.range[1])
        label = "range"
    else:
        # Default from env-configurable start to env-configurable end/now
        start_dt = parse_iso_utc(DEFAULT_START_ISO)
        end_dt = parse_iso_utc(DEFAULT_END_ISO) if DEFAULT_END_ISO else now_utc
        label = "default"

    if end_dt < start_dt:
        raise ValueError("End date must not be before start date")

    # Log chosen range
    print(f"Using date range (UTC): {format_iso_z(start_dt)} -> {format_iso_z(end_dt)} [{label}]")
    sys.stdout.flush()

    return start_dt, end_dt, label, args


# ----------------------------
# Exports API helpers
# ----------------------------

def _ms_to_iso(ms):
    if ms is None: return None
    import pandas as pd
    return pd.to_datetime(int(ms), unit="ms", utc=True).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_exports_day(day_iso: str) -> List[Dict[str, Any]]:
    import requests
    url = f"{BASE_EXPORTS}/conversation_export?created_after={day_iso}&created_before={day_iso}"
    r = requests.get(url, headers=HEADERS_EXPORTS, timeout=60)
    if r.status_code != 200:
        print("Exports HTTP:", r.status_code, r.text[:300]); return []
    try:
        data = r.json()
    except Exception:
        return []
    return data if isinstance(data, list) else data.get("data", [])


def map_export_row(rec: dict) -> Dict[str, Any]:
    ch = (rec.get("initial_channel") or "").lower()
    return {
      "id": rec.get("id"),
      "createdAt": _ms_to_iso(rec.get("created_at")),
      "answeredAt": _ms_to_iso(rec.get("assigned_at")),
      "closedAt": _ms_to_iso(rec.get("closed_at")),
      "direction": rec.get("direction"),
      "channel": ch,
      "assigneeId": rec.get("assignee_id"),
      "assigneeName": rec.get("assignee_name"),
      "queueId": rec.get("queue_id"),
      "queueName": rec.get("queue_name"),
    }


def fetch_detail(conv_id: str) -> Optional[Dict[str, Any]]:
    url = f"{BASE_V1}/conversations/{conv_id}"
    try:
        r = requests.get(url, headers=HEADERS_V1, timeout=30)
        if r.status_code != 200:
            return None
        j = r.json()
        return j.get("data")
    except Exception:
        return None


def compute_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        # Ensure columns exist
        for col in [
            "AnsweredWithin1Min",
            "TakenFromQueue",
            "TakenFromForward",
            "RejectedOrForwarded",
        ]:
            if col not in df.columns:
                df[col] = pd.Series(dtype="bool")
        return df

    # Parse datetimes
    created = pd.to_datetime(df["createdAt"], utc=True, errors="coerce") if "createdAt" in df.columns else pd.Series(pd.NaT, index=df.index)
    answered = pd.to_datetime(df.get("answeredAt"), utc=True, errors="coerce")

    # Computed
    within_1m = (answered - created).dt.total_seconds() <= 60
    within_1m = within_1m.fillna(False)

    assignment_reason = df.get("assignmentReason").fillna("")
    taken_from_queue = assignment_reason == "queue"
    taken_from_forward = assignment_reason == "forward"
    rejected_or_forwarded = answered.isna() | assignment_reason.isin(["forward", "rejected"])  # type: ignore[attr-defined]

    df["AnsweredWithin1Min"] = within_1m
    df["TakenFromQueue"] = taken_from_queue
    df["TakenFromForward"] = taken_from_forward
    df["RejectedOrForwarded"] = rejected_or_forwarded

    return df


def main(argv: Optional[List[str]] = None) -> None:
    print("Export Dixa Conversations - Refresh (Search API)")
    print("=" * 60)

    start_dt, end_dt, label, args = determine_range_from_cli(argv)

    # Debug channel
    ch_dbg = "ALL" if (args.channel or "").strip()=="" else args.channel
    print(f"channel={ch_dbg}")
    sys.stdout.flush()

    start_d = normalize_date_only(start_dt)
    end_d = normalize_date_only(end_dt)

    total_days = (end_d - start_d).days + 1
    print(f"Fetching days: {start_d.isoformat()} -> {end_d.isoformat()} (inclusive), total days: {total_days}")
    sys.stdout.flush()

    all_rows: List[Dict[str, Any]] = []

    day = start_d
    processed = 0
    # Output mode: default single-file unless --daily-files explicitly set
    write_daily = bool(args.daily_files)
    write_single = bool(args.single_file) or not write_daily

    # Ensure output directory for daily mode
    daily_dir = Path("data/dixa_daily")
    daily_dir.mkdir(parents=True, exist_ok=True)

    while day <= end_d:
        processed += 1
        print(f"Day {processed}/{total_days}: {day.isoformat()} ...", end=" ")
        sys.stdout.flush()

        # Fetch via Exports API for this day
        rows_raw = fetch_exports_day(day.isoformat())
        day_rows = [to_row(x) for x in rows_raw]
        # Optional channel filter
        if (args.channel or "") != "":
            day_rows = [r for r in day_rows if (r.get("channel") or "") == str(args.channel).lower()]
        # Optional enrichment (state/answeredAt) - could be toggled later
        # for r in day_rows:
        #     det = fetch_detail(r.get("id"))
        #     if det:
        #         r["state"] = det.get("state")
        #         r["answeredAt"] = det.get("answeredAt") or r.get("answeredAt")
        print("Exports day", day.isoformat(), "rows:", len(day_rows))
        sys.stdout.flush()
        if write_daily:
            # Write per-day CSV
            out = daily_dir / f"conversations_{day.isoformat()}.csv"
            df_day = pd.DataFrame(day_rows)
            df_day = compute_columns(df_day)
            df_day.to_csv(out, index=False, encoding="utf-8")
        if write_single:
            all_rows.extend(day_rows)

        # Courtesy delay between days
        time.sleep(0.05)

        day = day + timedelta(days=1)

    if write_single:
        if not all_rows:
            print("No conversations found for the selected range.")
            return

        # Build DataFrame and drop duplicate ids
        df = pd.DataFrame(all_rows)
        if "id" in df.columns:
            before = len(df)
            df = df.drop_duplicates(subset=["id"]).reset_index(drop=True)
            removed = before - len(df)
            if removed:
                print(f"Removed {removed} duplicate ids")
                sys.stdout.flush()

        # Add computed columns
        df = compute_columns(df)
        df_all = df

        # Write CSV (single file)
        if args.single_file:
            out = "conversations_ytd.csv"
            df_all.to_csv(out, index=False, encoding="utf-8")

        # Summary
        total_calls = len(df_all)
        calls_1m = int(df_all["AnsweredWithin1Min"].sum()) if "AnsweredWithin1Min" in df_all.columns else 0
        not_answered_or_fwd = int(df_all["RejectedOrForwarded"].sum()) if "RejectedOrForwarded" in df_all.columns else 0
        from_queue = int(df_all["TakenFromQueue"].sum()) if "TakenFromQueue" in df_all.columns else 0
        from_forward = int(df_all["TakenFromForward"].sum()) if "TakenFromForward" in df_all.columns else 0

        print("\n" + "-" * 60)
        print("SUMMARY")
        print("-" * 60)
        print(f"Total rows: {total_calls}")
        print(f"<= 1 minute: {calls_1m}")
        print(f"Rejected/Forwarded: {not_answered_or_fwd}")
        print(f"Via queue: {from_queue}")
        print(f"Via forward: {from_forward}")
        print(f"Date range: {format_iso_z(start_dt)} -> {format_iso_z(end_dt)} ({label})")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


