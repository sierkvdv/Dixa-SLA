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
from datetime import datetime, timedelta, timezone, date
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd
import requests

# Token and config from environment
API_KEY = os.getenv("DIXA_TOKEN"); assert API_KEY, "Set DIXA_TOKEN"
USE_BEARER = os.getenv("DIXA_USE_BEARER", "false").lower() == "true"


BASE_V1 = "https://dev.dixa.io/v1"

HEADERS_V1 = {
    "Authorization": (f"Bearer {API_KEY}" if USE_BEARER else API_KEY),
    "Accept": "application/json",
    "Content-Type": "application/json",
}


# Compatibility helper: include both keys for APIs expecting either
def compat_field(field_type: str) -> dict:
    return {"_type": field_type, "type": field_type}


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

    parser.add_argument("--channel", default="pstnPhone",
                        help="Channel filter; empty string means no channel filter (all)")
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
# API client helpers
# ----------------------------

def post_search_conversations(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], int, str]:
    url = f"{BASE_V1}/search/conversations"
    # Basic retry for transient failures
    attempts = 0
    last_exc: Optional[BaseException] = None
    while attempts < 3:
        attempts += 1
        try:
            # Debug: print payload before request
            try:
                print("Payload:", json.dumps(payload)[:400])
            except Exception:
                pass
            r = requests.post(url, json=payload, headers=HEADERS_V1, timeout=60)
            # Debug: print HTTP status and optional body
            try:
                print("HTTP:", r.status_code)
                if r.status_code != 200:
                    print("Body:", r.text[:400])
            except Exception:
                pass
            if r.status_code != 200:
                # If server error, retry; otherwise raise
                if 500 <= r.status_code < 600 and attempts < 3:
                    time.sleep(0.5)
                    continue
                raise requests.HTTPError(f"HTTP {r.status_code}: {r.text[:300]}")
            return r.json(), r.status_code, r.text
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            if attempts < 3:
                time.sleep(0.5)
                continue
            raise
        except Exception as exc:
            last_exc = exc
            raise
    # Should not reach
    if last_exc:
        raise last_exc
    return {}, 0, ""


def extract_items_from_response(resp_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Attempt to extract list of conversations from the search response.

    Tries common shapes: {data: {items|matches|results}}, {data: [...]}, top-level list.
    """
    if resp_json is None:
        return []

    if isinstance(resp_json, list):
        return [x for x in resp_json if isinstance(x, dict)]

    data = resp_json.get("data") if isinstance(resp_json, dict) else None
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ("items", "matches", "results", "conversations"):
            val = data.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
    # Fallback: try top-level conventional keys
    for key in ("items", "matches", "results", "conversations"):
        val = resp_json.get(key) if isinstance(resp_json, dict) else None
        if isinstance(val, list):
            return [x for x in val if isinstance(x, dict)]
    return []


def extract_fields(conv: Dict[str, Any]) -> Dict[str, Any]:
    """Extract required fields from a conversation without detail calls."""
    conv_id = conv.get("id")
    created_at = conv.get("createdAt")
    answered_at = conv.get("answeredAt")
    closed_at = conv.get("closedAt")
    state = conv.get("state")
    direction = conv.get("direction")
    channel = conv.get("channel") or conv.get("initialChannel") or "pstnPhone"

    # Assignment / assignee
    assignee_id = conv.get("assigneeId")
    assignee_name = conv.get("assigneeName")
    assignment_reason = conv.get("assignmentReason")

    assignment = conv.get("assignment") or {}
    if assignee_id is None:
        assignee = assignment.get("assignee") or {}
        if isinstance(assignee, dict):
            assignee_id = assignee.get("id", assignee_id)
            assignee_name = assignee.get("name", assignee_name)
    if assignment_reason is None:
        assignment_reason = assignment.get("reason")

    # Queue
    queue_id = conv.get("queueId")
    queue_name = conv.get("queueName")
    if queue_id is None or queue_name is None:
        queue = conv.get("queue") or {}
        if isinstance(queue, dict):
            queue_id = queue.get("id", queue_id)
            queue_name = queue.get("name", queue_name)

    return {
        "id": conv_id,
        "createdAt": created_at,
        "answeredAt": answered_at,
        "closedAt": closed_at,
        "state": state,
        "direction": direction,
        "channel": channel,
        "assigneeId": assignee_id,
        "assigneeName": assignee_name,
        "queueId": queue_id,
        "queueName": queue_name,
        "assignmentReason": assignment_reason,
    }


def fetch_day_conversations(day_dt: date, channel_filter: Optional[str]) -> Tuple[List[Dict[str, Any]], str]:
    """Fetch one day's conversations for channel pstnPhone with pagination.

    Returns (rows, channel_field_used) for debug output.
    """
    day_start = start_of_day_utc(day_dt)
    day_end = end_of_day_utc(day_dt)
    # If channel filter is empty -> do not filter on channel
    if channel_filter is None or str(channel_filter).strip() == "":
        results: List[Dict[str, Any]] = []
        last_created_at: Optional[str] = None
        while True:
            conditions = [
                {"field": compat_field("CreatedAtField"), "operator": "between",
                 "value": [format_iso_z(day_start), format_iso_z(day_end)]},
            ]
            # Optional simple channel filter when provided
            if channel_filter:
                conditions.append({
                    "field": "channel", "operator": "eq", "value": channel_filter,
                })
            if last_created_at:
                conditions.append({"field": compat_field("CreatedAtField"), "operator": "gt", "value": last_created_at})
            payload = {
                "limit": 200,
                "sort": {"field": "createdAt", "order": "asc"},
                "filters": {"strategy": "and", "conditions": conditions},
            }
            try:
                resp_json, status_code, resp_text = post_search_conversations(payload)
            except Exception as exc:
                print(f"Error fetching day {day_dt.isoformat()} (no channel): {exc}")
                sys.stdout.flush()
                break
            items = extract_items_from_response(resp_json)
            if not items:
                try:
                    print("Empty results, sample body:", resp_text[:400])
                except Exception:
                    pass
            if not items:
                break
            for c in items:
                results.append(extract_fields(c))
            created_values = [c.get("createdAt") for c in items if c.get("createdAt")]
            if len(items) < 200 or not created_values:
                break
            last_created_at = created_values[-1]
            time.sleep(0.15)
        return results, "-"

    # Trials for channel field/value combinations (provided channel and a capitalized variant)
    cand_vals = [str(channel_filter)]
    if channel_filter and (channel_filter[:1].upper() + channel_filter[1:]) != channel_filter:
        cand_vals.append(channel_filter[:1].upper() + channel_filter[1:])
    channel_trials = [
        (compat_field("ChannelTypeField"), cand_vals[0]),
        (compat_field("ChannelTypeField"), cand_vals[-1]),
        (compat_field("InitialChannelField"), cand_vals[0]),
        (compat_field("InitialChannelField"), cand_vals[-1]),
    ]

    def build_payload(channel_field_obj, channel_value, last_created_at=None):
        conditions = [
            {"field": channel_field_obj, "operator": "eq", "value": channel_value},
            {"field": compat_field("CreatedAtField"), "operator": "between",
             "value": [format_iso_z(day_start), format_iso_z(day_end)]},
        ]
        # Optional simple channel filter when provided (use original args.channel value)
        if channel_filter:
            conditions.append({
                "field": "channel", "operator": "eq", "value": channel_filter,
            })
        if last_created_at:
            conditions.append({"field": compat_field("CreatedAtField"), "operator": "gt", "value": last_created_at})
        return {
            "limit": 200,
            "sort": {"field": "createdAt", "order": "asc"},
            "filters": {"strategy": "and", "conditions": conditions}
        }

    results: List[Dict[str, Any]] = []
    used_trial = None

    for (field_obj, ch_val) in channel_trials:
        last_created_at = None
        trial_ok = False
        while True:
            payload = build_payload(field_obj, ch_val, last_created_at)
            try:
                resp_json, status_code, resp_text = post_search_conversations(payload)
            except Exception as exc:
                if "HTTP 400" in str(exc):
                    break
                print(f"Transient error: {exc}")
                break
            items = extract_items_from_response(resp_json)
            if not items:
                try:
                    print("Empty results, sample body:", resp_text[:400])
                except Exception:
                    pass
            if not items:
                trial_ok = True
                break
            for c in items:
                results.append(extract_fields(c))
            created_values = [c.get("createdAt") for c in items if c.get("createdAt")]
            if len(items) < 200 or not created_values:
                trial_ok = True
                break
            last_created_at = created_values[-1]
            time.sleep(0.15)
        if trial_ok:
            used_trial = (field_obj, ch_val)
            break

    if not results:
        print(f"Day {day_dt.isoformat()}: all channel trials failed or returned 0")
        return results, "-"

    # optional debug
    if used_trial is not None:
        print(f"Using channel trial: field keys={list(used_trial[0].keys())}, value={used_trial[1]}")

    # Infer channel field name for debug print in main
    used_field_name = "InitialChannelField" if used_trial and "Initial" in used_trial[0].get("_type", "") else "ChannelTypeField"
    return results, used_field_name


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

    # Debug info about payload casing/strategy
    ch_dbg = args.channel if (args.channel is not None and str(args.channel).strip() != "") else "ALL"
    print(f"Debug: filters.strategy=and; operators=lowercase; channel={ch_dbg}")
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

        day_rows, channel_field_used = fetch_day_conversations(day, args.channel)
        print(f"{len(day_rows)} records (channel={channel_field_used})")
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


