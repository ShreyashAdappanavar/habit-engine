"""
One-off reconstruction: for every historical RULE_BREAK streak, replay
rule_logs day-by-day (using the SAME buffer/window logic as the live
engine) to recover every rule that had exceeded its buffer on the
closing day — not just the single rule the old buggy code recorded.

Usage:
    python reconstruct_broken_rules.py          # dry run, prints only
    python reconstruct_broken_rules.py --apply  # writes to Supabase
"""
import sys
import datetime as dt
from typing import Dict, List, Optional

import engine  # reuses _sb, _date, _row_applies_on — do not duplicate this logic


def applicable_rules_for_date(rule_rows: List[dict], d: dt.date) -> Dict[str, dict]:
    """Mirrors engine._load_applicable_rule_rows_for_date, in-memory."""
    latest: Dict[str, dict] = {}
    for r in sorted(rule_rows, key=lambda x: x["effective_from"], reverse=True):
        k = r["rule_key"]
        if k in latest:
            continue
        if engine._row_applies_on(r, d):
            latest[k] = r
    return latest


def simulate_streak(
    streak_start: dt.date,
    streak_end: dt.date,
    rule_rows: List[dict],
    logs: Dict[tuple, str],
) -> List[dict]:
    rule_state: Dict[str, dict] = {}

    d = streak_start
    while d <= streak_end:
        applicable = applicable_rules_for_date(rule_rows, d)
        for rule_key, rdef in applicable.items():
            window_days = int(rdef["window_days"])
            widx = (d - streak_start).days // window_days

            st = rule_state.get(rule_key)
            if not st or st["widx"] != widx:
                st = {"widx": widx, "misses": 0}

            log_state = logs.get((d, rule_key))  # None if row absent — do NOT default

            if log_state is None:
                if d == streak_end:
                    # Row absent on the closing day = the old bug never reached
                    # this rule to force-write it. We cannot know what it would
                    # have been. Do not count it, do not evaluate it this day.
                    rule_state[rule_key] = st
                    d_key = d
                    continue
                # Absent on a non-closing day = genuinely never logged (gap in
                # rule_logs itself, unrelated to the break-loop bug). Treat as
                # UNKNOWN, same as the live engine's force-miss would.
                log_state = "UNKNOWN"

            if log_state != "PASS":
                st["misses"] += 1

            rule_state[rule_key] = st
        d += dt.timedelta(days=1)

    applicable_end = applicable_rules_for_date(rule_rows, streak_end)
    broken = []
    for rule_key, rdef in applicable_end.items():
        st = rule_state.get(rule_key)
        if not st:
            continue
        # Skip rules whose closing-day state we couldn't determine
        if logs.get((streak_end, rule_key)) is None:
            continue
        buffer_misses = int(rdef["buffer_misses"])
        if st["misses"] > buffer_misses:
            broken.append({
                "rule_key": rule_key,
                "misses_in_window": st["misses"],
                "buffer_misses": buffer_misses,
                "window_days": int(rdef["window_days"]),
                "rule_version": int(rdef["version"]),
            })
    return broken


def main():
    apply = "--apply" in sys.argv
    sb = engine._sb()

    rule_rows = sb.table("rule_defs").select("*").execute().data

    log_rows = []
    page_size = 1000
    offset = 0
    while True:
        page = (
            sb.table("rule_logs")
            .select("log_date,rule_key,state")
            .range(offset, offset + page_size - 1)
            .execute()
            .data
        )
        log_rows.extend(page)
        if len(page) < page_size:
            break
        offset += page_size

    print(f"Fetched {len(log_rows)} rule_logs rows (expected 1116).")

    logs = {
        (dt.date.fromisoformat(r["log_date"]), r["rule_key"]): r["state"]
        for r in log_rows
    }

    streaks = (
        sb.table("streaks")
        .select("streak_id,start_date,end_date,end_reason_json")
        .not_.is_("end_date", "null")
        .execute()
        .data
    )

    targets = [s for s in streaks if (s.get("end_reason_json") or {}).get("type") == "RULE_BREAK"]
    print(f"Found {len(targets)} RULE_BREAK streaks to reconstruct.\n")

    mismatches = []
    updates = []

    for s in targets:
        start = dt.date.fromisoformat(s["start_date"])
        end = dt.date.fromisoformat(s["end_date"])
        old_broken = (s["end_reason_json"] or {}).get("broken_rules", [])
        old_keys = {r["rule_key"] for r in old_broken}

        new_broken = simulate_streak(start, end, rule_rows, logs)
        new_keys = {r["rule_key"] for r in new_broken}

        status = "MATCH" if old_keys == new_keys else \
                 "SUPERSET" if old_keys.issubset(new_keys) else "MISMATCH"

        print(f"streak_id={s['streak_id']:>3}  {start}..{end}  "
              f"old={sorted(old_keys)}  new={sorted(new_keys)}  [{status}]")

        if status == "MISMATCH":
            mismatches.append(s["streak_id"])
            continue  # never auto-write a mismatch — needs manual review

        if new_keys != old_keys:  # only write if there's actually new info
            new_reason = dict(s["end_reason_json"])
            new_reason["broken_rules"] = new_broken
            updates.append((s["streak_id"], new_reason))

    print(f"\n{len(updates)} streaks have additional rules to backfill.")
    print(f"{len(mismatches)} streaks flagged MISMATCH — old rule_key not reproduced, skipped: {mismatches}")

    if not apply:
        print("\nDry run only. Re-run with --apply to write changes.")
        return

    for streak_id, new_reason in updates:
        sb.table("streaks").update({"end_reason_json": new_reason}).eq("streak_id", streak_id).execute()
    print(f"\nApplied {len(updates)} updates.")


if __name__ == "__main__":
    main()