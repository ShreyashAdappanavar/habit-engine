import os
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional

from supabase import create_client


TZ_NOTE = "Asia/Kolkata (handled by local date only)"  # no tz math; Streamlit runtime provides local date


def _sb():
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]  # anon key is fine if you keep RLS disabled
    return create_client(url, key)


def _today() -> dt.date:
    return dt.date.today()


def ensure_app_start_date(sb) -> dt.date:
    row = sb.table("app_meta").select("start_date").eq("id", 1).execute().data
    if row:
        return dt.date.fromisoformat(row[0]["start_date"])
    start = _today()
    sb.table("app_meta").insert({"id": 1, "start_date": start.isoformat()}).execute()
    return start


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
    row = sb.table("streaks").insert(payload).execute().data[0]
    return row


def close_streak_and_open_next(
    sb, open_streak: dict, end_date: dt.date, reason: Dict[str, Any]
) -> dict:
    sb.table("streaks").update(
        {
            "end_date": end_date.isoformat(),
            "status": "CLOSED",
            "end_reason_json": reason,
            "updated_at": dt.datetime.utcnow().isoformat(),
        }
    ).eq("streak_id", open_streak["streak_id"]).execute()

    next_start = end_date + dt.timedelta(days=1)
    return create_open_streak(sb, next_start)


def _load_active_rule_versions_for_date(sb, d: dt.date) -> Dict[str, dict]:
    # One query: take latest effective_from<=d per rule_key
    rows = (
        sb.table("rule_defs")
        .select("*")
        .lte("effective_from", d.isoformat())
        .order("rule_key", desc=False)
        .order("effective_from", desc=True)
        .execute()
        .data
    )
    latest: Dict[str, dict] = {}
    for r in rows:
        k = r["rule_key"]
        if k not in latest:
            latest[k] = r
    # Filter inactive
    return {k: v for k, v in latest.items() if v.get("is_active", True)}


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
    # Persist strictness: UNKNOWN => MISS at finalization
    sb.table("rule_logs").upsert(
        {
            "log_date": d.isoformat(),
            "rule_key": rule_key,
            "state": "MISS",
            "updated_at": dt.datetime.utcnow().isoformat(),
        },
        on_conflict="log_date,rule_key",
    ).execute()
    return "MISS"


def _calc_window_index(streak_start: dt.date, d: dt.date, window_days: int) -> int:
    return (d - streak_start).days // window_days


def _state_get(rule_state_json: Dict[str, Any], rule_key: str) -> Dict[str, Any]:
    return rule_state_json.get(rule_key, {})


def _state_set(rule_state_json: Dict[str, Any], rule_key: str, st: Dict[str, Any]) -> None:
    rule_state_json[rule_key] = st


def process_up_to(sb, max_date: dt.date) -> Dict[str, Any]:
    """
    Processes all days <= max_date that are not yet processed, across streak boundaries.
    Auto-finalization is implicit: any day < today can be processed; today is processed only if caller sets max_date=today.
    """
    app_start = ensure_app_start_date(sb)
    open_streak = get_open_streak(sb) or create_open_streak(sb, app_start)

    processed_any = False
    events: List[Dict[str, Any]] = []

    while True:
        s_start = dt.date.fromisoformat(open_streak["start_date"])
        processed_through = dt.date.fromisoformat(open_streak["processed_through_date"])
        next_day = processed_through + dt.timedelta(days=1)

        if next_day > max_date:
            break

        active_rules = _load_active_rule_versions_for_date(sb, next_day)
        rule_state = open_streak.get("rule_state_json") or {}

        # Evaluate day
        for rule_key, rdef in active_rules.items():
            # Policy 2 reset on version effective_from day (and restriction effective_from>=today is your discipline, not enforced here)
            st = _state_get(rule_state, rule_key)
            cur_ver = int(rdef["version"])
            eff_from = dt.date.fromisoformat(rdef["effective_from"])

            window_days = int(rdef["window_days"])
            buffer_misses = int(rdef["buffer_misses"])

            widx = _calc_window_index(s_start, next_day, window_days)

            # Reset if version changed effective today OR natural window rollover OR first time seen
            if not st:
                st = {"ver": cur_ver, "widx": widx, "misses": 0}
            else:
                if st.get("ver") != cur_ver and next_day == eff_from:
                    st = {"ver": cur_ver, "widx": widx, "misses": 0}
                elif st.get("widx") != widx:
                    st["widx"] = widx
                    st["misses"] = 0
                else:
                    st["ver"] = cur_ver  # keep updated

            state = _get_log_state(sb, next_day, rule_key)
            state = _force_miss_if_unknown(sb, next_day, rule_key, state)

            if state == "MISS":
                st["misses"] = int(st.get("misses", 0)) + 1
                if st["misses"] > buffer_misses:
                    reason = {
                        "rule_key": rule_key,
                        "date": next_day.isoformat(),
                        "misses_in_window": st["misses"],
                        "buffer_misses": buffer_misses,
                        "window_days": window_days,
                        "rule_version": cur_ver,
                    }
                    events.append({"type": "STREAK_ENDED", "reason": reason})
                    open_streak = close_streak_and_open_next(sb, open_streak, next_day, reason)
                    processed_any = True
                    break

            _state_set(rule_state, rule_key, st)

        # If streak ended on this day, we already opened next streak and should continue loop (next_day advances)
        if get_open_streak(sb)["streak_id"] != open_streak["streak_id"]:
            # should not happen; defensive
            open_streak = get_open_streak(sb)  # pragma: no cover

        # Update processed_through_date for the CURRENT open streak if it didn't end on next_day
        # If it ended, open_streak is already the next one and next_day is its "previous day".
        current_open = get_open_streak(sb)
        if current_open["streak_id"] == open_streak["streak_id"]:
            # If we ended, open_streak changed; in that case, we must not write processed_through_date on the new streak yet.
            pass

        # Determine if we ended on next_day by checking latest closed streak end_date
        # Simpler: if an event ended today, skip updating processed_through on (old) streak because it's closed; new streak stays start-1.
        ended_today = any(e.get("type") == "STREAK_ENDED" and e["reason"]["date"] == next_day.isoformat() for e in events)

        if not ended_today:
            sb.table("streaks").update(
                {
                    "processed_through_date": next_day.isoformat(),
                    "rule_state_json": rule_state,
                    "updated_at": dt.datetime.utcnow().isoformat(),
                }
            ).eq("streak_id", open_streak["streak_id"]).execute()
            processed_any = True

        else:
            # closed streak processed_through is implicitly its end date; open streak starts tomorrow with processed_through=start-1
            pass

        open_streak = get_open_streak(sb)

    return {"processed_any": processed_any, "events": events, "open_streak": open_streak}

def get_app_start_date(sb) -> dt.date:
    return ensure_app_start_date(sb)


def compute_discipline_index(sb, end_date: dt.date, window_days: int) -> Dict[str, Any]:
    """
    Rolling average of daily weighted completion over finalized days only.
    Uses days in [max(app_start, end_date-window_days+1) .. end_date].
    Missing/UNKNOWN counts as MISS (0 contribution).
    """
    app_start = get_app_start_date(sb)
    start_date = max(app_start, end_date - dt.timedelta(days=window_days - 1))
    if start_date > end_date:
        return {"di": 0.0, "days": 0, "start_date": start_date, "end_date": end_date}

    # Rule versions up to end_date
    rule_rows = (
        sb.table("rule_defs")
        .select("rule_key,effective_from,is_active,weight")
        .lte("effective_from", end_date.isoformat())
        .order("rule_key", desc=False)
        .order("effective_from", desc=False)
        .execute()
        .data
    )

    versions: Dict[str, List[Tuple[dt.date, bool, float]]] = {}
    for r in rule_rows:
        k = r["rule_key"]
        eff = dt.date.fromisoformat(r["effective_from"])
        active = bool(r.get("is_active", True))
        w = float(r.get("weight", 1))
        versions.setdefault(k, []).append((eff, active, w))  # ascending by effective_from

    # Logs in range (single fetch)
    log_rows = (
        sb.table("rule_logs")
        .select("log_date,rule_key,state")
        .gte("log_date", start_date.isoformat())
        .lte("log_date", end_date.isoformat())
        .execute()
        .data
    )
    logs = {(dt.date.fromisoformat(x["log_date"]), x["rule_key"]): x["state"] for x in log_rows}

    # Per-rule cursor into version list (ascending)
    idx: Dict[str, int] = {k: 0 for k in versions.keys()}

    daily_scores: List[float] = []
    d = start_date
    while d <= end_date:
        numer = 0.0
        denom = 0.0

        for k, vlist in versions.items():
            # advance cursor while next version is effective on/before d
            i = idx[k]
            while i + 1 < len(vlist) and vlist[i + 1][0] <= d:
                i += 1
            idx[k] = i

            eff, active, w = vlist[i]
            if eff > d or not active:
                continue  # not applicable (not yet introduced) or deactivated

            denom += w
            state = logs.get((d, k), "UNKNOWN")
            if state == "PASS":
                numer += w

        if denom > 0:
            daily_scores.append(numer / denom)

        d += dt.timedelta(days=1)

    di = (sum(daily_scores) / len(daily_scores)) if daily_scores else 0.0
    return {"di": di, "days": len(daily_scores), "start_date": start_date, "end_date": end_date}

def auto_process_until_yesterday() -> Dict[str, Any]:
    sb = _sb()
    today = _today()
    return process_up_to(sb, today - dt.timedelta(days=1))


def finalize_today() -> Dict[str, Any]:
    sb = _sb()
    today = _today()
    return process_up_to(sb, today)