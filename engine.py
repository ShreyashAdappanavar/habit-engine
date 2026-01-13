# engine.py
import os
import datetime as dt
from typing import Dict, Any, List, Optional, Tuple
import statistics as stats

from supabase import create_client

from zoneinfo import ZoneInfo
IST = ZoneInfo("Asia/Kolkata")

def _get_cfg(name: str) -> str:
    # Prefer Streamlit secrets if available, else env.
    try:
        import streamlit as st  # type: ignore
        v = st.secrets.get(name)  # type: ignore[attr-defined]
        if v is not None:
            return str(v)
    except Exception:
        pass
    return os.environ.get(name, "")


def _sb():
    url = _get_cfg("SUPABASE_URL")
    key = _get_cfg("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_KEY in secrets or env.")
    return create_client(url, key)



def _today() -> dt.date:
    return dt.datetime.now(IST).date()


def _tomorrow() -> dt.date:
    return _today() + dt.timedelta(days=1)


def _date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


def ensure_app_start_date(sb) -> dt.date:
    row = sb.table("app_meta").select("start_date").eq("id", 1).execute().data
    if row:
        return _date(row[0]["start_date"])
    start = _today()
    sb.table("app_meta").insert({"id": 1, "start_date": start.isoformat()}).execute()
    return start


def get_app_start_date(sb) -> dt.date:
    return ensure_app_start_date(sb)


def get_open_streak(sb) -> Optional[dict]:
    data = (
        sb.table("streaks")
        .select("*")
        .eq("status", "OPEN")
        .order("streak_id", desc=True)
        .limit(1)
        .execute()
        .data
    )
    return data[0] if data else None


def create_open_streak(sb, start_date: dt.date) -> dict:
    payload = {
        "start_date": start_date.isoformat(),
        "end_date": None,
        "status": "OPEN",
        "processed_through_date": (start_date - dt.timedelta(days=1)).isoformat(),
        "rule_state_json": {},
        "end_reason_json": None,
    }
    return sb.table("streaks").insert(payload).execute().data[0]


def close_streak_and_open_next(sb, open_streak: dict, end_date: dt.date, reason: Dict[str, Any]) -> dict:
    sb.table("streaks").update(
        {
            "end_date": end_date.isoformat(),
            "status": "CLOSED",
            "processed_through_date": end_date.isoformat(),
            "end_reason_json": reason,
            "updated_at": dt.datetime.utcnow().isoformat(),
        }
    ).eq("streak_id", open_streak["streak_id"]).execute()

    return create_open_streak(sb, end_date + dt.timedelta(days=1))


def _row_applies_on(sb_row: dict, d: dt.date) -> bool:
    eff_from = _date(sb_row["effective_from"])
    eff_to_raw = sb_row.get("effective_to")
    eff_to = _date(eff_to_raw) if eff_to_raw else None
    if eff_from > d:
        return False
    if eff_to is not None and d > eff_to:
        return False
    return True


def _load_applicable_rule_rows_for_date(sb, d: dt.date) -> Dict[str, dict]:
    # Fetch all versions with effective_from <= d and pick the latest that still applies (effective_to NULL or >= d)
    rows = (
        sb.table("rule_defs")
        .select("*")
        .lte("effective_from", d.isoformat())
        .order("rule_key", desc=False)
        .order("effective_from", desc=True)
        .execute()
        .data
    )
    picked: Dict[str, dict] = {}
    for r in rows:
        k = r["rule_key"]
        if k in picked:
            continue
        if _row_applies_on(r, d):
            picked[k] = r
    return picked


def _get_log_state(sb, d: dt.date, rule_key: str) -> str:
    row = (
        sb.table("rule_logs")
        .select("state")
        .eq("log_date", d.isoformat())
        .eq("rule_key", rule_key)
        .limit(1)
        .execute()
        .data
    )
    if not row:
        return "UNKNOWN"
    return row[0]["state"]


def _force_miss_if_unknown(sb, d: dt.date, rule_key: str, state: str) -> str:
    if state != "UNKNOWN":
        return state
    resp = sb.table("rule_logs").upsert(
        {
            "log_date": d.isoformat(),
            "rule_key": rule_key,
            "state": "MISS",
            "updated_at": dt.datetime.utcnow().isoformat(),
        },
        on_conflict="log_date,rule_key",
    ).execute()
    if getattr(resp, "error", None):
        raise RuntimeError(resp.error)
    return "MISS"


def _calc_window_index(streak_start: dt.date, d: dt.date, window_days: int) -> int:
    return (d - streak_start).days // window_days


def process_up_to(sb, max_date: dt.date) -> Dict[str, Any]:
    """
    Processes days up to max_date (inclusive).
    - If you call with yesterday: approximates auto-finalize on next run after 00:00.
    - If you call with today: finalize+evaluate today.
    """
    _validate_rule_defs_no_overlaps(sb)
    app_start = ensure_app_start_date(sb)
    open_streak = get_open_streak(sb) or create_open_streak(sb, app_start)

    events: List[Dict[str, Any]] = []

    while True:
        open_streak = get_open_streak(sb) or create_open_streak(sb, app_start)

        s_start = _date(open_streak["start_date"])
        processed_through = _date(open_streak["processed_through_date"])
        next_day = processed_through + dt.timedelta(days=1)

        if next_day > max_date:
            break

        applicable_rules = _load_applicable_rule_rows_for_date(sb, next_day)
        rule_state: Dict[str, Any] = open_streak.get("rule_state_json") or {}

        ended = False
        end_reason: Dict[str, Any] = {}

        for rule_key, rdef in applicable_rules.items():
            st = rule_state.get(rule_key) or {}

            cur_ver = int(rdef["version"])
            window_days = int(rdef["window_days"])
            buffer_misses = int(rdef["buffer_misses"])
            widx = _calc_window_index(s_start, next_day, window_days)

            # No reset on version change. Reset only when the window index changes.
            if not st:
                st = {"ver": cur_ver, "widx": widx, "misses": 0}
            else:
                if st.get("widx") != widx:
                    st["widx"] = widx
                    st["misses"] = 0
                st["ver"] = cur_ver


            state = _get_log_state(sb, next_day, rule_key)
            state = _force_miss_if_unknown(sb, next_day, rule_key, state)

            if state == "MISS":
                st["misses"] = int(st.get("misses", 0)) + 1
                if st["misses"] > buffer_misses:
                    # FIX (1): persist final-day counters for the failing rule
                    rule_state[rule_key] = st

                    ended = True
                    end_reason = {
                        "rule_key": rule_key,
                        "date": next_day.isoformat(),
                        "misses_in_window": st["misses"],
                        "buffer_misses": buffer_misses,
                        "window_days": window_days,
                        "rule_version": cur_ver,
                    }
                    break

            rule_state[rule_key] = st  # unchanged

        if ended:
            events.append({"type": "STREAK_ENDED", "reason": end_reason})

            # FIX (2): persist the final rule_state_json onto the closing streak
            sb.table("streaks").update(
                {"rule_state_json": rule_state}
            ).eq("streak_id", open_streak["streak_id"]).execute()

            close_streak_and_open_next(sb, open_streak, next_day, end_reason)
            continue

        sb.table("streaks").update(
            {
                "processed_through_date": next_day.isoformat(),
                "rule_state_json": rule_state,
                "updated_at": dt.datetime.utcnow().isoformat(),
            }
        ).eq("streak_id", open_streak["streak_id"]).execute()

    return {"events": events, "open_streak": get_open_streak(sb)}

def _validate_rule_defs_no_overlaps(sb) -> None:
    rows = (
        sb.table("rule_defs")
        .select("rule_key,effective_from,effective_to")
        .order("rule_key", desc=False)
        .order("effective_from", desc=False)
        .execute()
        .data
    )

    by_key: Dict[str, List[Tuple[dt.date, Optional[dt.date]]]] = {}
    for r in rows:
        k = r["rule_key"]
        a = _date(r["effective_from"])
        b = _date(r["effective_to"]) if r.get("effective_to") else None
        if b is not None and b < a:
            raise ValueError(f"Invalid rule_defs range for {k}: effective_to < effective_from ({a}..{b})")
        by_key.setdefault(k, []).append((a, b))

    for k, spans in by_key.items():
        prev_a, prev_b = spans[0]
        prev_end = prev_b or dt.date.max
        for a, b in spans[1:]:
            end = b or dt.date.max
            # inclusive overlap check: overlap exists unless prev_end < a
            if prev_end >= a:
                raise ValueError(
                    f"Overlapping versions for {k}: [{prev_a}..{prev_b or 'NULL'}] overlaps [{a}..{b or 'NULL'}]"
                )
            prev_a, prev_b, prev_end = a, b, end



def auto_process_until_yesterday() -> Dict[str, Any]:
    sb = _sb()
    return process_up_to(sb, _today() - dt.timedelta(days=1))


def finalize_today() -> Dict[str, Any]:
    sb = _sb()
    return process_up_to(sb, _today())

def reset_streak_today() -> Dict[str, Any]:
    sb = _sb()
    today = _today()
    tomorrow = today + dt.timedelta(days=1)

    # Step 1: finalize + evaluate today (same as Finalize button)
    res = process_up_to(sb, today)

    # Step 2: if streak already ended today (engine opened a new streak starting tomorrow), no-op
    open_streak = get_open_streak(sb)
    if not open_streak:
        return {"reset": False, "reason": "no_open_streak", "events": res.get("events", [])}

    if _date(open_streak["start_date"]) == tomorrow:
        return {"reset": False, "reason": "already_ended_today", "events": res.get("events", [])}

    # Step 3: otherwise force-close the current open streak today and open the next one tomorrow
    reason = {"type": "MANUAL_RESET", "date": today.isoformat()}
    close_streak_and_open_next(sb, open_streak, today, reason)
    return {"reset": True, "reason": reason, "events": res.get("events", [])}



def compute_discipline_index(sb, end_date: dt.date, window_days: int) -> Dict[str, Any]:
    """
    Rolling average of daily weighted completion over finalized days only.
    Missing/UNKNOWN => 0 contribution.
    Date range: [max(app_start, end_date-window_days+1) .. end_date].
    Uses rule versions applicable on each day via (effective_from, effective_to).
    """
    _validate_rule_defs_no_overlaps(sb)
    app_start = get_app_start_date(sb)
    start_date = max(app_start, end_date - dt.timedelta(days=window_days - 1))
    if start_date > end_date:
        return {"di": 0.0, "days": 0, "start_date": start_date, "end_date": end_date}

    # Load all versions that could affect [start_date..end_date]
    rule_rows = (
        sb.table("rule_defs")
        .select("rule_key,effective_from,effective_to,weight")
        .lte("effective_from", end_date.isoformat())
        .order("rule_key", desc=False)
        .order("effective_from", desc=False)
        .execute()
        .data
    )

    versions: Dict[str, List[Tuple[dt.date, Optional[dt.date], float]]] = {}
    for r in rule_rows:
        k = r["rule_key"]
        eff_from = _date(r["effective_from"])
        eff_to_raw = r.get("effective_to")
        eff_to = _date(eff_to_raw) if eff_to_raw else None
        w = float(r.get("weight", 1))
        versions.setdefault(k, []).append((eff_from, eff_to, w))

    log_rows = (
        sb.table("rule_logs")
        .select("log_date,rule_key,state")
        .gte("log_date", start_date.isoformat())
        .lte("log_date", end_date.isoformat())
        .execute()
        .data
    )
    logs = {(dt.date.fromisoformat(x["log_date"]), x["rule_key"]): x["state"] for x in log_rows}

    idx: Dict[str, int] = {k: 0 for k in versions.keys()}

    daily_scores: List[float] = []
    d = start_date
    while d <= end_date:
        numer = 0.0
        denom = 0.0

        for k, vlist in versions.items():
            i = idx[k]
            while i + 1 < len(vlist) and vlist[i + 1][0] <= d:
                i += 1
            idx[k] = i

            eff_from, eff_to, w = vlist[i]
            if eff_from > d:
                continue
            if eff_to is not None and d > eff_to:
                continue

            denom += w
            state = logs.get((d, k), "UNKNOWN")
            if state == "PASS":
                numer += w

        if denom > 0:
            daily_scores.append(numer / denom)

        d += dt.timedelta(days=1)

    di = (sum(daily_scores) / len(daily_scores)) if daily_scores else 0.0
    return {"di": di, "days": len(daily_scores), "start_date": start_date, "end_date": end_date}

def compute_di_timeseries(
    sb,
    end_date: dt.date,
    plot_days: int = 14,
    windows: Tuple[int, int] = (7, 30),
) -> Dict[str, Any]:
    """
    Returns last `plot_days` finalized-day points ending at `end_date` (inclusive):
      - di1: daily weighted completion
      - diN: rolling mean of di1 over last N days (window clipped to app_start)

    Pre-app days are excluded (no implicit zeros).
    """
    _validate_rule_defs_no_overlaps(sb)

    app_start = get_app_start_date(sb)
    if end_date < app_start:
        return {"rows": [], "plot_start": app_start, "end_date": end_date}

    plot_start = max(app_start, end_date - dt.timedelta(days=plot_days - 1))
    max_w = max(windows)
    internal_start = max(app_start, plot_start - dt.timedelta(days=max_w - 1))

    # Load rule versions that could apply up to end_date
    rule_rows = (
        sb.table("rule_defs")
        .select("rule_key,effective_from,effective_to,weight")
        .lte("effective_from", end_date.isoformat())
        .order("rule_key", desc=False)
        .order("effective_from", desc=False)
        .execute()
        .data
    )

    versions: Dict[str, List[Tuple[dt.date, Optional[dt.date], float]]] = {}
    for r in rule_rows:
        k = r["rule_key"]
        eff_from = _date(r["effective_from"])
        eff_to_raw = r.get("effective_to")
        eff_to = _date(eff_to_raw) if eff_to_raw else None
        w = float(r.get("weight", 1))
        versions.setdefault(k, []).append((eff_from, eff_to, w))

    # Load logs for the internal range
    log_rows = (
        sb.table("rule_logs")
        .select("log_date,rule_key,state")
        .gte("log_date", internal_start.isoformat())
        .lte("log_date", end_date.isoformat())
        .execute()
        .data
    )
    logs = {(dt.date.fromisoformat(x["log_date"]), x["rule_key"]): x["state"] for x in log_rows}

    # Compute DI1 for every day in [internal_start..end_date]
    idx: Dict[str, int] = {k: 0 for k in versions.keys()}
    dates: List[dt.date] = []
    di1_vals: List[float] = []

    d = internal_start
    while d <= end_date:
        numer = 0.0
        denom = 0.0

        for k, vlist in versions.items():
            i = idx[k]
            while i + 1 < len(vlist) and vlist[i + 1][0] <= d:
                i += 1
            idx[k] = i

            eff_from, eff_to, w = vlist[i]
            if eff_from > d:
                continue
            if eff_to is not None and d > eff_to:
                continue

            denom += w
            state = logs.get((d, k), "UNKNOWN")
            if state == "PASS":
                numer += w

        di1 = (numer / denom) if denom > 0 else 0.0
        dates.append(d)
        di1_vals.append(di1)
        d += dt.timedelta(days=1)

    pos = {dates[i]: i for i in range(len(dates))}
    prefix = [0.0]
    for v in di1_vals:
        prefix.append(prefix[-1] + v)

    def _avg_for_day(day: dt.date, wdays: int) -> float:
        start = max(app_start, day - dt.timedelta(days=wdays - 1))
        i0 = pos[start]
        i1 = pos[day]
        return (prefix[i1 + 1] - prefix[i0]) / (i1 - i0 + 1)

    rows: List[Dict[str, Any]] = []
    d = plot_start
    while d <= end_date:
        i = pos[d]
        rows.append(
            {
                "date": d.isoformat(),
                "di1": di1_vals[i],
                f"di{windows[0]}": _avg_for_day(d, windows[0]),
                f"di{windows[1]}": _avg_for_day(d, windows[1]),
            }
        )
        d += dt.timedelta(days=1)

    return {"rows": rows, "plot_start": plot_start, "end_date": end_date}

def compute_statistics(
    sb,
    end_date: dt.date,
    consistency_window_days: int = 30,
) -> Dict[str, Any]:
    """
    Returns:
      - global streak stats (all streaks; OPEN uses processed_through_date as its current end)
      - 3 best / 3 worst rule consistency over last `consistency_window_days` finalized days
      - per-rule pass-run (individual rule streak) stats over full history
    All computations use finalized horizon = end_date (caller passes processed_through).
    """
    _validate_rule_defs_no_overlaps(sb)

    app_start = get_app_start_date(sb)
    if end_date < app_start:
        return {
            "global": {"count": 0, "mean": 0.0, "median": 0.0, "stdev": 0.0, "min": 0, "max": 0},
            "rule_consistency_window_days": consistency_window_days,
            "consistency": [],
            "rule_streaks": [],
            "range": {"start": app_start, "end": end_date},
        }

    def _mean(xs): return float(stats.mean(xs)) if xs else 0.0
    def _median(xs): return float(stats.median(xs)) if xs else 0.0
    def _stdev(xs): return float(stats.stdev(xs)) if len(xs) >= 2 else 0.0

    # ---------- Global streak stats ----------
    streak_rows = (
        sb.table("streaks")
        .select("start_date,end_date,status,processed_through_date")
        .order("streak_id", desc=False)
        .execute()
        .data
    )

    lengths: List[int] = []
    for r in streak_rows:
        s = _date(r["start_date"])
        e = _date(r["end_date"]) if r.get("end_date") else _date(r["processed_through_date"])
        l = (e - s).days + 1 if e >= s else 0
        lengths.append(l)

    global_stats = {
        "count": len(lengths),
        "mean": _mean(lengths),
        "median": _median(lengths),
        "stdev": _stdev(lengths),
        "min": int(min(lengths)) if lengths else 0,
        "max": int(max(lengths)) if lengths else 0,
    }

    # ---------- Rule versions ----------
    rule_rows = (
        sb.table("rule_defs")
        .select("rule_key,effective_from,effective_to,name,weight")
        .lte("effective_from", end_date.isoformat())
        .order("rule_key", desc=False)
        .order("effective_from", desc=False)
        .execute()
        .data
    )

    versions: Dict[str, List[Tuple[dt.date, Optional[dt.date], str, float]]] = {}
    for r in rule_rows:
        k = r["rule_key"]
        eff_from = _date(r["effective_from"])
        eff_to = _date(r["effective_to"]) if r.get("effective_to") else None
        nm = r.get("name") or k
        wt = float(r.get("weight") or 1.0)
        versions.setdefault(k, []).append((eff_from, eff_to, nm, wt))

    # ---------- Logs ----------
    log_rows = (
        sb.table("rule_logs")
        .select("log_date,rule_key,state")
        .gte("log_date", app_start.isoformat())
        .lte("log_date", end_date.isoformat())
        .execute()
        .data
    )
    logs = {(dt.date.fromisoformat(x["log_date"]), x["rule_key"]): x["state"] for x in log_rows}

    # ---------- Middle: per-rule consistency over last N finalized days ----------
    cons_start = max(app_start, end_date - dt.timedelta(days=consistency_window_days - 1))
    consistency: List[Dict[str, Any]] = []

    for k, vlist in versions.items():
        i = 0
        applicable = 0
        passed = 0
        last_name = vlist[-1][2] if vlist else k

        d = cons_start
        while d <= end_date:
            while i + 1 < len(vlist) and vlist[i + 1][0] <= d:
                i += 1
            eff_from, eff_to, nm, wt = vlist[i]
            last_name = nm

            if d < eff_from or (eff_to is not None and d > eff_to):
                d += dt.timedelta(days=1)
                continue

            applicable += 1
            if logs.get((d, k), "UNKNOWN") == "PASS":
                passed += 1
            d += dt.timedelta(days=1)

        rate = (passed / applicable) if applicable else None
        consistency.append(
            {
                "rule_key": k,
                "name": last_name,
                "applicable_days": applicable,
                "pass_days": passed,
                "pass_rate": rate,
            }
        )

    # ---------- Bottom: per-rule individual streak stats (PASS-runs) over full history ----------
    rule_streaks: List[Dict[str, Any]] = []
    for k, vlist in versions.items():
        i = 0
        last_name = vlist[-1][2] if vlist else k

        runs: List[int] = []
        cur = 0
        applicable_seen = 0

        d = app_start
        while d <= end_date:
            while i + 1 < len(vlist) and vlist[i + 1][0] <= d:
                i += 1
            eff_from, eff_to, nm, wt = vlist[i]
            last_name = nm

            if d < eff_from or (eff_to is not None and d > eff_to):
                d += dt.timedelta(days=1)
                continue

            applicable_seen += 1
            is_pass = (logs.get((d, k), "UNKNOWN") == "PASS")

            if is_pass:
                cur += 1
            else:
                if cur > 0:
                    runs.append(cur)
                    cur = 0

            d += dt.timedelta(days=1)

        # include ongoing current run as a streak sample
        if cur > 0:
            runs.append(cur)

        current_streak = cur if (runs and runs[-1] == cur) else 0

        rule_streaks.append(
            {
                "rule_key": k,
                "name": last_name,
                "current_streak": current_streak,
                "streak_count": len(runs),
                "mean": _mean(runs),
                "median": _median(runs),
                "stdev": _stdev(runs),
                "max": int(max(runs)) if runs else 0,
                "applicable_days": applicable_seen,
            }
        )

    return {
        "global": global_stats,
        "rule_consistency_window_days": consistency_window_days,
        "consistency": consistency,
        "rule_streaks": rule_streaks,
        "range": {"start": app_start, "end": end_date},
    }


# ---------------------- Admin: Rule Management (tomorrow-only, no is_active) ----------------------

def _assert_no_scheduled_beyond_tomorrow(sb, rule_key: str) -> None:
    tmr = _tomorrow()
    rows = (
        sb.table("rule_defs")
        .select("rule_key,version,effective_from,effective_to")
        .eq("rule_key", rule_key)
        .gt("effective_from", tmr.isoformat())
        .limit(1)
        .execute()
        .data
    )
    if rows:
        raise ValueError("Future scheduling beyond tomorrow exists for this rule_key. Remove those rows first.")


def _has_version_on_date(sb, rule_key: str, d: dt.date) -> bool:
    rows = (
        sb.table("rule_defs")
        .select("rule_key")
        .eq("rule_key", rule_key)
        .eq("effective_from", d.isoformat())
        .limit(1)
        .execute()
        .data
    )
    return bool(rows)


def _has_any_version_from_date(sb, rule_key: str, d: dt.date) -> bool:
    rows = (
        sb.table("rule_defs")
        .select("rule_key")
        .eq("rule_key", rule_key)
        .gte("effective_from", d.isoformat())
        .limit(1)
        .execute()
        .data
    )
    return bool(rows)


def admin_list_rule_keys(sb) -> List[str]:
    rows = sb.table("rule_defs").select("rule_key").order("rule_key", desc=False).execute().data
    return sorted({r["rule_key"] for r in rows})


def admin_get_versions(sb, rule_key: str) -> List[dict]:
    return (
        sb.table("rule_defs")
        .select("*")
        .eq("rule_key", rule_key)
        .order("version", desc=False)
        .execute()
        .data
    )


def admin_get_max_version(sb, rule_key: str) -> int:
    rows = (
        sb.table("rule_defs")
        .select("version")
        .eq("rule_key", rule_key)
        .order("version", desc=True)
        .limit(1)
        .execute()
        .data
    )
    return int(rows[0]["version"]) if rows else 0


def admin_get_version_applicable_on(sb, rule_key: str, d: dt.date) -> Optional[dict]:
    rows = (
        sb.table("rule_defs")
        .select("*")
        .eq("rule_key", rule_key)
        .lte("effective_from", d.isoformat())
        .order("effective_from", desc=True)
        .execute()
        .data
    )
    for r in rows:
        if _row_applies_on(r, d):
            return r
    return None


def admin_add_new_rule(
    sb,
    rule_key: str,
    name: str,
    description: str,
    window_days: int,
    buffer_misses: int,
    weight: float,
) -> dict:
    eff = _tomorrow()

    exists = (
        sb.table("rule_defs")
        .select("rule_key")
        .eq("rule_key", rule_key)
        .limit(1)
        .execute()
        .data
    )
    if exists:
        raise ValueError("rule_key already exists")

    row = sb.table("rule_defs").insert(
        {
            "rule_key": rule_key,
            "version": 1,
            "effective_from": eff.isoformat(),
            "effective_to": None,
            "name": name,
            "description": description or "",
            "window_days": int(window_days),
            "buffer_misses": int(buffer_misses),
            "weight": float(weight),
        }
    ).execute().data[0]
    return row


def admin_add_new_version(
    sb,
    rule_key: str,
    name: str,
    description: str,
    window_days: int,
    buffer_misses: int,
    weight: float,
) -> dict:
    today = _today()
    eff_new = _tomorrow()

    any_row = (
        sb.table("rule_defs").select("rule_key").eq("rule_key", rule_key).limit(1).execute().data
    )
    if not any_row:
        raise ValueError("rule_key does not exist")

    # Option (1) enforcement
    _assert_no_scheduled_beyond_tomorrow(sb, rule_key)

    # Don’t allow multiple edits “queued” for tomorrow
    if _has_version_on_date(sb, rule_key, eff_new):
        raise ValueError("A version is already scheduled for tomorrow. Remove it first.")

    # Close the version that applies today so it ends today.
    cur = admin_get_version_applicable_on(sb, rule_key, today)
    if cur:
        sb.table("rule_defs").update({"effective_to": today.isoformat()}).eq(
            "rule_key", rule_key
        ).eq("version", int(cur["version"])).execute()

    new_version = admin_get_max_version(sb, rule_key) + 1

    new_row = sb.table("rule_defs").insert(
        {
            "rule_key": rule_key,
            "version": int(new_version),
            "effective_from": eff_new.isoformat(),
            "effective_to": None,
            "name": name,
            "description": description or "",
            "window_days": int(window_days),
            "buffer_misses": int(buffer_misses),
            "weight": float(weight),
        }
    ).execute().data[0]

    return new_row


def admin_deactivate_rule_key(sb, rule_key: str) -> dict:
    today = _today()
    tmr = _tomorrow()

    # Option (1) enforcement
    _assert_no_scheduled_beyond_tomorrow(sb, rule_key)

    # Deactivate must not leave a scheduled “reactivation” tomorrow
    if _has_any_version_from_date(sb, rule_key, tmr):
        raise ValueError("A version is scheduled for tomorrow. Remove it first, then deactivate.")

    cur = admin_get_version_applicable_on(sb, rule_key, today)
    if not cur:
        raise ValueError("rule_key is not applicable today (already inactive)")

    sb.table("rule_defs").update({"effective_to": today.isoformat()}).eq(
        "rule_key", rule_key
    ).eq("version", int(cur["version"])).execute()

    updated = (
        sb.table("rule_defs")
        .select("*")
        .eq("rule_key", rule_key)
        .eq("version", int(cur["version"]))
        .limit(1)
        .execute()
        .data
    )
    return updated[0] if updated else cur