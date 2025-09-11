#!/usr/bin/env python3
"""
Export Dixa Conversations - Previous Month (Exports API + Detail Enrichment)
- Fetches previous calendar month's conversations via Exports API
- Filters initial_channel == pstnphone (telephone)
- Enriches each record with details from /v1/conversations/{id}
- Computes metrics and writes conversations_prev_month.csv (UTF-8)
"""

import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import time
import sys

# Configuration
API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJmZTVlMWJmMC04ZTY3LTQwNjgtOTc5Zi03OTAyY2VkOThhYzgiLCJ0eXBlIjoiYXBpIiwib3JnX2lkIjoiMDljOTU0MGEtNTI1Zi00MmE4LWE4NGMtNzE5Y2JkZmNhOWFkIiwianRpIjoiMjNlOGFiYWQwNzUyNGE0ZWIzZTAxNGZjNTNkYWY0MzEifQ.NoU5eGkEWPtgmljdFEyzd8FAY82O2ZzEVvKDdZqsD2k"
BASE_V1 = "https://dev.dixa.io/v1"
BASE_EXPORTS = "https://exports.dixa.io/v1"

HEADERS_V1 = {
    "Authorization": API_KEY,
    "Accept": "application/json",
    "Content-Type": "application/json",
}

HEADERS_EXPORTS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
}

# Toggle detail enrichment (slower)
ENRICH_DETAILS = True


def get_previous_month_range():
    today = datetime.now()
    first_day_current = today.replace(day=1)
    last_day_prev = first_day_current - timedelta(days=1)
    first_day_prev = last_day_prev.replace(day=1)
    return first_day_prev.strftime("%Y-%m-%d"), last_day_prev.strftime("%Y-%m-%d")


def parse_cli_range():
    if len(sys.argv) == 3:
        start = sys.argv[1]
        end = sys.argv[2]
        # Expect YYYY-MM-DD
        try:
            datetime.strptime(start, "%Y-%m-%d")
            datetime.strptime(end, "%Y-%m-%d")
        except ValueError:
            print("ERROR: Use dates in format YYYY-MM-DD, e.g. 2025-07-01 2025-07-31")
            sys.exit(1)
        return start, end
    return get_previous_month_range()


def ms_to_iso(ms):
    if ms is None:
        return None
    try:
        return datetime.utcfromtimestamp(int(ms) / 1000).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def fetch_exports(start_date, end_date):
    url = f"{BASE_EXPORTS}/conversation_export?created_after={start_date}&created_before={end_date}"
    try:
        resp = requests.get(url, headers=HEADERS_EXPORTS, timeout=60)
        if resp.status_code != 200:
            print(f"ERROR: Exports API {resp.status_code}: {resp.text[:300]}")
            return []
        try:
            data = resp.json()
        except json.JSONDecodeError:
            print("ERROR: Exports API did not return JSON")
            return []
        if isinstance(data, list):
            return data
        # Some deployments wrap as {data: [...]}
        return data.get("data", [])
    except Exception as e:
        print(f"ERROR: Exports API request failed: {e}")
        return []


def fetch_detail(conversation_id):
    url = f"{BASE_V1}/conversations/{conversation_id}"
    try:
        r = requests.get(url, headers=HEADERS_V1, timeout=30)
        if r.status_code != 200:
            return None
        j = r.json()
        return j.get("data")
    except Exception:
        return None


def calculate_metrics_from_ms(created_at_ms, assigned_at_ms, queued_at_ms):
    answered_within_1min = False
    if created_at_ms and assigned_at_ms:
        try:
            answered_within_1min = (int(assigned_at_ms) - int(created_at_ms)) <= 60000
        except Exception:
            answered_within_1min = False

    # Not answered if no assigned_at
    rejected_or_forwarded = assigned_at_ms is None

    # Queue vs forward inferred from queued_at presence
    taken_from_queue = (queued_at_ms is not None) and (assigned_at_ms is not None)
    taken_from_forward = (queued_at_ms is None) and (assigned_at_ms is not None)

    # Derive a human assignmentReason for CSV
    assignment_reason = None
    if taken_from_queue:
        assignment_reason = "queue"
    elif taken_from_forward:
        assignment_reason = "forward"

    return answered_within_1min, rejected_or_forwarded, taken_from_queue, taken_from_forward, assignment_reason


def build_rows(exports_rows):
    rows = []
    detail_ok = 0
    detail_fail = 0
    
    # Pre-filter candidates to know total upfront
    candidates = [rec for rec in exports_rows if (rec.get("initial_channel") or "").lower() == "pstnphone"]
    total = len(candidates)
    print(f"Telephone candidates to process: {total}")
    sys.stdout.flush()

    for idx, rec in enumerate(candidates, 1):
        conv_id = rec.get("id")
        created_at_ms = rec.get("created_at")
        queued_at_ms = rec.get("queued_at")
        assigned_at_ms = rec.get("assigned_at")
        closed_at_ms = rec.get("closed_at")

        created_at = ms_to_iso(created_at_ms)
        closed_at = ms_to_iso(closed_at_ms)
        answered_at = ms_to_iso(assigned_at_ms)
        direction = rec.get("direction")
        assignee_id = rec.get("assignee_id")
        assignee_name = rec.get("assignee_name")
        queue_id = rec.get("queue_id")
        queue_name = rec.get("queue_name")
        channel = "PstnPhone"

        # Compute metrics from exports timestamps
        ans1m, rej_fwd, from_queue, from_forward, assignment_reason = calculate_metrics_from_ms(
            created_at_ms, assigned_at_ms, queued_at_ms
        )

        # Optional: enrich with details (state, answeredAt refinement)
        state = None
        if ENRICH_DETAILS and conv_id is not None:
            details = fetch_detail(conv_id)
            if details:
                detail_ok += 1
                state = details.get("state")
                # If answeredAt exists in details, prefer it
                answered_at = details.get("answeredAt") or answered_at
                # Enrich assignmentReason if available
                assignment = details.get("assignment") or {}
                assignment_reason = assignment.get("reason") or assignment_reason
                # Enrich queue name/id if available
                q = details.get("queue") or {}
                queue_id = q.get("id") or queue_id
                queue_name = q.get("name") or queue_name
            else:
                detail_fail += 1

        rows.append({
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
            "AnsweredWithin1Min": ans1m,
            "TakenFromQueue": from_queue,
            "TakenFromForward": from_forward,
            "RejectedOrForwarded": rej_fwd,
        })

        # Be nice to the API if enriching
        if ENRICH_DETAILS:
            # Progress output every 50 items
            if idx % 50 == 0 or idx == total:
                print(f"Processed {idx}/{total} (details ok={detail_ok}, failed={detail_fail})")
                sys.stdout.flush()
            time.sleep(0.1)

    return rows, detail_ok, detail_fail, total


def main():
    print("Export Dixa Conversations - Previous Month (Exports API)")
    print("=" * 60)

    start_date, end_date = parse_cli_range()
    print(f"Period: {start_date} to {end_date}")

    exports_rows = fetch_exports(start_date, end_date)
    print(f"Exports returned: {len(exports_rows)} records (all channels)")

    rows, detail_ok, detail_fail, total_candidates = build_rows(exports_rows)
    print(f"Telephone conversations after filter: {len(rows)} (from {total_candidates} candidates)")
    print(f"Details fetched: ok={detail_ok}, failed={detail_fail}")

    if not rows:
        print("No conversations to export")
        return

    df = pd.DataFrame(rows)
    out = "conversations_prev_month.csv"
    df.to_csv(out, index=False, encoding="utf-8")
    print(f"Wrote {len(df)} rows to {out}")

    # Summary
    total_calls = len(df)
    calls_1m = int(df["AnsweredWithin1Min"].sum()) if "AnsweredWithin1Min" in df.columns else 0
    not_answered_or_fwd = int(df["RejectedOrForwarded"].sum()) if "RejectedOrForwarded" in df.columns else 0
    from_queue = int(df["TakenFromQueue"].sum()) if "TakenFromQueue" in df.columns else 0
    from_forward = int(df["TakenFromForward"].sum()) if "TakenFromForward" in df.columns else 0

    print("\n" + "-" * 60)
    print("SUMMARY")
    print("-" * 60)
    print(f"Total calls: {total_calls}")
    print(f"Calls <= 1 minute: {calls_1m}")
    print(f"Not Answered/Forwarded: {not_answered_or_fwd}")
    print(f"Taken From Queue: {from_queue}")
    print(f"Taken From Forward: {from_forward}")


if __name__ == "__main__":
    main()
