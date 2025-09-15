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

# Windowed export config
WINDOW_DAYS = int(os.getenv("DIXA_WINDOW_DAYS", "7"))  # 7 = week
BASE_DELAY = float(os.getenv("DIXA_BASE_DELAY", "7.5"))  # seconds between windows


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


def ms_to_iso(ms):
    return _ms_to_iso(ms)


def fetch_exports_day(day_iso: str) -> List[Dict[str, Any]]:
    import requests
    fields = (
        "id,initial_channel,direction,created_at,queued_at,assigned_at,closed_at,"
        "queue_id,queue_name,assignee_id,assignee_name"
    )
    url = (
        f"{BASE_EXPORTS}/conversation_export?created_after={day_iso}&created_before={day_iso}"
        f"&fields={fields}"
    )
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
      "queuedAt": _ms_to_iso(rec.get("queued_at")),
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


def map_row(rec: dict) -> Dict[str, Any]:
    def ms_to_int(ms):
        return None if ms is None else int(ms)

    created_ms  = rec.get("created_at")
    answered_ms = rec.get("assigned_at")
    queued_ms   = rec.get("queued_at")
    closed_ms   = rec.get("closed_at")

    ans1m = (
        created_ms is not None and answered_ms is not None
        and (ms_to_int(answered_ms) - ms_to_int(created_ms)) <= 60_000
    )

    taken_from_queue   = (queued_ms  is not None and answered_ms is not None)
    taken_from_forward = (queued_ms  is None     and answered_ms is not None)
    rejected_or_fwd    = (answered_ms is None) or taken_from_forward

    row = {
      "id": rec.get("id"),
      "createdAt": ms_to_iso(created_ms),
      "queued_at":   ms_to_iso(queued_ms),
      "assigned_at": ms_to_iso(answered_ms),
      "answeredAt": ms_to_iso(answered_ms),
      "queuedAt":   ms_to_iso(queued_ms),
      "closedAt":   ms_to_iso(closed_ms),
      "direction": (rec.get("direction") or ""),
      "channel":   (rec.get("initial_channel") or "").lower(),
      "assigneeName": rec.get("assignee_name"),
      "queueName":    rec.get("queue_name"),
      "AnsweredWithin1Min": ans1m,
      "TakenFromQueue":     taken_from_queue,
      "TakenFromForward":   taken_from_forward,
      "RejectedOrForwarded": rejected_or_fwd
    }

    return row


def date_windows(start_d: date, end_d: date, step_days: int):
    cur = start_d
    from datetime import timedelta
    while cur <= end_d:
        win_end = min(end_d, cur + timedelta(days=step_days - 1))
        yield cur, win_end
        cur = win_end + timedelta(days=1)


def fetch_exports_window(win_start_d: date, win_end_d: date, max_retries: int = 6) -> List[Dict[str, Any]]:
    import requests
    created_after = win_start_d.isoformat()
    created_before = win_end_d.isoformat()
    fields = (
        "id,initial_channel,direction,created_at,queued_at,assigned_at,closed_at,"
        "queue_id,queue_name,assignee_id,assignee_name"
    )
    url = (
        f"{BASE_EXPORTS}/conversation_export?created_after={created_after}&created_before={created_before}"
        f"&fields={fields}"
    )
    delay = BASE_DELAY
    tries = 0
    while True:
        r = requests.get(url, headers=HEADERS_EXPORTS, timeout=60)
        if r.status_code == 200:
            try:
                data = r.json()
            except Exception:
                return []
            return data if isinstance(data, list) else data.get("data", [])
        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            try:
                wait = float(ra) if ra else delay
            except Exception:
                wait = delay
            wait = max(wait, BASE_DELAY)
            print(f"429 rate limited, waiting {wait:.1f}s ...")
            time.sleep(wait)
            tries += 1
            delay = min(delay * 1.5, 60)
            if tries >= max_retries:
                print("Giving up window due to repeated 429")
                return []
            continue
        if 500 <= r.status_code < 600:
            time.sleep(delay)
            tries += 1
            if tries >= max_retries:
                print(f"Server error {r.status_code}, giving up")
                return []
            continue
        print(f"Exports HTTP {r.status_code}: {r.text[:200]}")
        return []


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
    assigned_plain = pd.to_datetime(df.get("assigned_at"), utc=True, errors="coerce") if "assigned_at" in df.columns else pd.Series(pd.NaT, index=df.index)
    closed = pd.to_datetime(df.get("closedAt"), utc=True, errors="coerce") if "closedAt" in df.columns else pd.Series(pd.NaT, index=df.index)

    # Computed using enriched fields where available
    within_1m = (answered - created).dt.total_seconds() <= 60
    within_1m = within_1m.fillna(False)

    # Prefer 'assignment.reason' if present; fallback to 'assignmentReason'; else empty string
    if "assignment.reason" in df.columns:
        assignment_reason_series = df["assignment.reason"].fillna("")
    elif "assignmentReason" in df.columns:
        assignment_reason_series = df["assignmentReason"].fillna("")
    else:
        assignment_reason_series = pd.Series([""] * len(df), index=df.index)

    assignment_reason_lower = assignment_reason_series.astype(str).str.lower()
    taken_from_queue = assignment_reason_lower == "queue"
    taken_from_forward = assignment_reason_lower == "forward"
    rejected_or_forwarded = answered.isna() | assignment_reason_lower.isin(["forward", "rejected"])  # type: ignore[attr-defined]

    # Ensure pandas datetimes (UTC) then compute CallDurationSec as closedAt - answeredAt
    df["closedAt"] = pd.to_datetime(df["closedAt"], utc=True, errors="coerce")
    df["answeredAt"] = pd.to_datetime(df["answeredAt"], utc=True, errors="coerce")
    if "assigned_at" in df.columns:
        df["assigned_at"] = pd.to_datetime(df["assigned_at"], utc=True, errors="coerce")
    # Primary: closedAt - answeredAt (seconds)
    df["CallDurationSec"] = (df["closedAt"] - df["answeredAt"]).dt.total_seconds()
    # Fallback for rows without answeredAt: use assigned_at when available
    if "assigned_at" in df.columns:
        fallback = (df["closedAt"] - df["assigned_at"]).dt.total_seconds()
        df["CallDurationSec"] = df["CallDurationSec"].fillna(fallback)

    df["AnsweredWithin1Min"] = within_1m
    df["TakenFromQueue"] = taken_from_queue
    df["TakenFromForward"] = taken_from_forward
    df["RejectedOrForwarded"] = rejected_or_forwarded

    # CallType derived from assignment reason (queue/forward/direct)
    call_type = pd.Series("direct", index=df.index)
    call_type = call_type.mask(assignment_reason_lower == "queue", "queue")
    call_type = call_type.mask(assignment_reason_lower == "forward", "forward")
    df["CallType"] = call_type

    # Binnen1MinFair: FairTTASeconds <= 60 and CallType == 'direct'
    fair = pd.to_numeric(df.get("FairTTASeconds"), errors="coerce")
    df["Binnen1MinFair"] = (fair <= 60) & (df["CallType"].astype(str) == "direct")

    # CallDurationSec already computed above

    # FairTTASeconds: time-to-answer from queuedAt (fallback createdAt) to answeredAt (fallback assignment.assignedAt)
    queued = pd.to_datetime(df.get("queuedAt"), utc=True, errors="coerce") if "queuedAt" in df.columns else pd.Series(pd.NaT, index=df.index)
    assigned_detail = pd.to_datetime(df.get("assignment.assignedAt"), utc=True, errors="coerce") if "assignment.assignedAt" in df.columns else pd.Series(pd.NaT, index=df.index)
    effective_answered = answered.combine_first(assigned_detail)
    start_time = queued.combine_first(created)
    fair_seconds = (effective_answered - start_time).dt.total_seconds()
    # If no answered timestamp at all, leave empty (NaN)
    fair_seconds = fair_seconds.where(~effective_answered.isna(), other=pd.NA)
    df["FairTTASeconds"] = fair_seconds

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

    processed = 0
    # Output mode: default single-file unless --daily-files explicitly set
    write_daily = bool(args.daily_files)
    write_single = bool(args.single_file) or not write_daily

    # Ensure output directory for daily mode
    daily_dir = Path("data/dixa_daily")
    daily_dir.mkdir(parents=True, exist_ok=True)

    for (w_start, w_end) in date_windows(start_d, end_d, WINDOW_DAYS):
        processed += 1
        win_label = f"{w_start.isoformat()}->{w_end.isoformat()}"
        print(f"Window {win_label} ...", end=" ")
        sys.stdout.flush()

        rows_raw = fetch_exports_window(w_start, w_end)
        if not rows_raw:
            print("no rows (skipping write)")
            sys.stdout.flush()
            time.sleep(BASE_DELAY)
            continue

        rows = [map_row(x) for x in rows_raw]
        if (args.channel or "") != "":
            rows = [r for r in rows if (r.get("channel") or "") == str(args.channel).lower()]

        # Detail enrichment per conversation id
        for r in rows:
            conv_id = r.get("id")
            if not conv_id:
                continue
            try:
                det = fetch_detail(conv_id)
            except Exception:
                det = None
            if not det:
                continue
            ans = det.get("answeredAt")
            if ans:
                r["answeredAt"] = ans
            assignment = det.get("assignment") or {}
            r["assignment.assignedAt"] = assignment.get("assignedAt")
            r["assignment.reason"] = assignment.get("reason")
            if "offeredAt" in assignment:
                r["assignment.offeredAt"] = assignment.get("offeredAt")
            time.sleep(0.1)

        print(f"{len(rows)} rows")
        sys.stdout.flush()

        if write_daily and rows:
            out = daily_dir / f"conversations_{w_start.isoformat()}__{w_end.isoformat()}.csv"
            df_day = pd.DataFrame(rows)
            df_day = compute_columns(df_day)
            df_day.to_csv(out, index=False, encoding="utf-8")
        if write_single and rows:
            all_rows.extend(rows)

        time.sleep(BASE_DELAY)

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
            # Ensure required export columns exist and in order
            required_cols = [
                "createdAt",
                "queued_at",
                "assigned_at",
                "answeredAt",
                "assignmentReason",
                "AnsweredWithin1Min",
                "TakenFromQueue",
                "TakenFromForward",
                "RejectedOrForwarded",
                "FairTTASeconds",
                "CallDurationSec",
                "CallType",
                "Binnen1MinFair",
            ]
            # Backfill assignmentReason from assignment.reason if missing
            if "assignmentReason" not in df_all.columns and "assignment.reason" in df_all.columns:
                df_all["assignmentReason"] = df_all["assignment.reason"]
            # Add any missing required columns as empty
            for c in required_cols:
                if c not in df_all.columns:
                    df_all[c] = pd.NA
            df_export = df_all.reindex(columns=required_cols)
            out = "conversations_ytd.csv"
            df_export.to_csv(out, index=False, encoding="utf-8")

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


