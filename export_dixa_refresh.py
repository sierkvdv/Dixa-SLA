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
from datetime import datetime, timedelta, timezone, date
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd
import requests

# Reuse existing API_KEY from project
from export_dixa_prev_month_exports import API_KEY  # noqa: F401


BASE_V1 = "https://dev.dixa.io/v1"

HEADERS_V1 = {
    "Authorization": API_KEY,  # no Bearer prefix per requirement
    "Accept": "application/json",
    "Content-Type": "application/json",
}


# ----------------------------
# Date parsing and CLI options
# ----------------------------

DEFAULT_START_ISO = "2025-01-01T00:00:00Z"


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


def determine_range_from_cli(argv: Optional[List[str]] = None) -> Tuple[datetime, datetime, str]:
    """Return (start_dt, end_dt, label) in UTC.

    Default: from 2025-01-01T00:00:00Z to now (UTC).
    """
    parser = argparse.ArgumentParser(description="Export Dixa telephone conversations to CSV")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--last7", action="store_true", help="Use last 7 days (UTC)")
    group.add_argument("--ytd", action="store_true", help="Use year-to-date from 2025-01-01 (UTC)")
    group.add_argument("--range", nargs=2, metavar=("START", "END"), help="Custom range (ISO or YYYY-MM-DD)")

    args = parser.parse_args(argv)

    now_utc = datetime.now(timezone.utc)

    if args.last7:
        end_dt = now_utc
        start_dt = end_dt - timedelta(days=7)
        label = "last7"
    elif args.ytd:
        start_dt = parse_iso_utc(DEFAULT_START_ISO)
        end_dt = now_utc
        label = "ytd"
    elif args.range:
        start_dt = parse_iso_utc(args.range[0])
        end_dt = parse_iso_utc(args.range[1])
        label = "range"
    else:
        # Default
        start_dt = parse_iso_utc(DEFAULT_START_ISO)
        end_dt = now_utc
        label = "default"

    if end_dt < start_dt:
        raise ValueError("End date must not be before start date")

    # Clamp to 2025-01-01 minimum as per requirement context
    min_dt = parse_iso_utc(DEFAULT_START_ISO)
    if start_dt < min_dt:
        start_dt = min_dt

    # Log chosen range
    print(f"Using date range (UTC): {format_iso_z(start_dt)} -> {format_iso_z(end_dt)} [{label}]")
    sys.stdout.flush()

    return start_dt, end_dt, label


# ----------------------------
# API client helpers
# ----------------------------

def post_search_conversations(payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{BASE_V1}/search/conversations"
    # Basic retry for transient failures
    attempts = 0
    last_exc: Optional[BaseException] = None
    while attempts < 3:
        attempts += 1
        try:
            r = requests.post(url, json=payload, headers=HEADERS_V1, timeout=60)
            if r.status_code != 200:
                # If server error, retry; otherwise raise
                if 500 <= r.status_code < 600 and attempts < 3:
                    time.sleep(0.5)
                    continue
                raise requests.HTTPError(f"HTTP {r.status_code}: {r.text[:300]}")
            return r.json()
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
    return {}


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


def fetch_day_conversations(day_dt: date) -> List[Dict[str, Any]]:
    """Fetch one day's conversations for channel pstnPhone with pagination."""
    day_start = start_of_day_utc(day_dt)
    day_end = end_of_day_utc(day_dt)

    base_conditions: List[Dict[str, Any]] = [
        {"field": {"_type": "ChannelTypeField"}, "operator": "eq", "value": "pstnPhone"},
        {
            "field": {"_type": "CreatedAtField"},
            "operator": "between",
            "value": [format_iso_z(day_start), format_iso_z(day_end)],
        },
    ]

    results: List[Dict[str, Any]] = []
    last_created_at: Optional[str] = None

    while True:
        conditions = list(base_conditions)
        if last_created_at is not None:
            conditions.append({
                "field": {"_type": "CreatedAtField"},
                "operator": "gt",
                "value": last_created_at,
            })

        payload = {
            "limit": 200,
            "sort": {"field": "createdAt", "order": "asc"},
            "filters": {
                "strategy": "and",
                "conditions": conditions,
            },
        }

        try:
            resp_json = post_search_conversations(payload)
        except Exception as exc:
            print(f"Error fetching day {day_dt.isoformat()}: {exc}")
            sys.stdout.flush()
            break

        items = extract_items_from_response(resp_json)
        if not items:
            break

        # Map fields and extend
        for c in items:
            results.append(extract_fields(c))

        # Prepare for pagination
        if len(items) < 200:
            break

        # Track last createdAt from this page
        # Use the max createdAt available
        created_values = [c.get("createdAt") for c in items if c.get("createdAt")]
        if not created_values:
            break
        # Items are sorted asc; last element should be max
        last_created_at = created_values[-1]

        # Respect rate limiting
        time.sleep(0.15)

    return results


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

    start_dt, end_dt, label = determine_range_from_cli(argv)

    start_d = normalize_date_only(start_dt)
    end_d = normalize_date_only(end_dt)

    total_days = (end_d - start_d).days + 1
    print(f"Fetching days: {start_d.isoformat()} -> {end_d.isoformat()} (inclusive), total days: {total_days}")
    sys.stdout.flush()

    all_rows: List[Dict[str, Any]] = []

    day = start_d
    processed = 0
    while day <= end_d:
        processed += 1
        print(f"Day {processed}/{total_days}: {day.isoformat()} ...", end=" ")
        sys.stdout.flush()

        day_rows = fetch_day_conversations(day)
        print(f"{len(day_rows)} records")
        sys.stdout.flush()
        all_rows.extend(day_rows)

        # Courtesy delay between days
        time.sleep(0.05)

        day = day + timedelta(days=1)

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

    # Write CSV
    out_file = "conversations_ytd.csv"
    df.to_csv(out_file, index=False, encoding="utf-8")

    # Summary
    total_calls = len(df)
    calls_1m = int(df["AnsweredWithin1Min"].sum()) if "AnsweredWithin1Min" in df.columns else 0
    not_answered_or_fwd = int(df["RejectedOrForwarded"].sum()) if "RejectedOrForwarded" in df.columns else 0
    from_queue = int(df["TakenFromQueue"].sum()) if "TakenFromQueue" in df.columns else 0
    from_forward = int(df["TakenFromForward"].sum()) if "TakenFromForward" in df.columns else 0

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


