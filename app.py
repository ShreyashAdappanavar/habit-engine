# app.py
import datetime as dt
import os
import time

import streamlit as st
from supabase import create_client

import engine
import pandas as pd

from zoneinfo import ZoneInfo
IST = ZoneInfo("Asia/Kolkata")


def sb():
    url = _get_secret("SUPABASE_URL")
    key = _get_secret("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_KEY in secrets or env.")
    return create_client(url, key)



def _get_secret(name: str) -> str:
    try:
        v = st.secrets.get(name)  # type: ignore[attr-defined]
        if v is not None:
            return str(v)
    except Exception:
        pass
    return os.environ.get(name, "")


RULE_MGR_PASSWORD = _get_secret("RULE_MGR_PASSWORD")


def _today() -> dt.date:
    return dt.datetime.now(IST).date()


def _tomorrow() -> dt.date:
    return _today() + dt.timedelta(days=1)


def _date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


def _applies_on_row(r: dict, d: dt.date) -> bool:
    eff_from = _date(r["effective_from"])
    eff_to_raw = r.get("effective_to")
    eff_to = _date(eff_to_raw) if eff_to_raw else None
    if eff_from > d:
        return False
    if eff_to is not None and d > eff_to:
        return False
    return True


def load_latest_rules_for_date(sb_client, d: dt.date):
    rows = (
        sb_client.table("rule_defs")
        .select("rule_key,version,effective_from,effective_to,name,description,window_days,buffer_misses,weight")
        .lte("effective_from", d.isoformat())
        .order("rule_key", desc=False)
        .order("effective_from", desc=True)
        .execute()
        .data
    )
    latest = {}
    for r in rows:
        k = r["rule_key"]
        if k in latest:
            continue
        if _applies_on_row(r, d):
            latest[k] = r
    return latest


def load_active_rules_for_date(sb_client, d: dt.date):
    latest = load_latest_rules_for_date(sb_client, d)
    active = list(latest.values())
    return sorted(active, key=lambda r: (-float(r.get("weight", 1)), r.get("name", "")))


def load_logs_for_date(sb_client, d: dt.date):
    rows = (
        sb_client.table("rule_logs")
        .select("rule_key,state")
        .eq("log_date", d.isoformat())
        .execute()
        .data
    )
    return {r["rule_key"]: r["state"] for r in rows}


def upsert_logs_for_date(sb_client, d: dt.date, states: dict) -> bool:
    engine.ensure_app_start_date(sb_client)
    open_streak = engine.get_open_streak(sb_client)
    if open_streak is None:
        app_start = engine.get_app_start_date(sb_client)
        open_streak = engine.create_open_streak(sb_client, app_start)

    processed_through_db = _date(open_streak["processed_through_date"])
    if d <= processed_through_db:
        return False

    payload = []
    now = dt.datetime.utcnow().isoformat()
    for rule_key, checked in states.items():
        payload.append(
            {
                "log_date": d.isoformat(),
                "rule_key": rule_key,
                "state": "PASS" if checked else "UNKNOWN",
                "updated_at": now,
            }
        )
    if payload:
        sb_client.table("rule_logs").upsert(payload, on_conflict="log_date,rule_key").execute()
    return True



def streak_len_days(start_date: dt.date, processed_through: dt.date) -> int:
    if processed_through < start_date:
        return 0
    return (processed_through - start_date).days + 1


def compute_buffer_view(rules, rule_state_json, streak_start: dt.date, processed_through: dt.date):
    rows = []
    for r in rules:
        rule_key = r["rule_key"]
        name = r["name"]
        window_days = int(r["window_days"])
        buffer_misses = int(r["buffer_misses"])

        stt = (rule_state_json or {}).get(rule_key) or {}
        widx = stt.get("widx")
        misses = int(stt.get("misses", 0))

        if widx is None:
            widx = (processed_through - streak_start).days // window_days if processed_through >= streak_start else 0

        remaining_n = buffer_misses - misses
        remaining_str = f"{remaining_n}/{buffer_misses}"

        window_start = streak_start + dt.timedelta(days=widx * window_days)
        window_end = window_start + dt.timedelta(days=window_days - 1)
        resets_in = (window_end - processed_through).days
        if resets_in < 0:
            resets_in = 0

        rows.append(
            {
                "name": name,
                "remaining": remaining_str,
                "remaining_n": remaining_n,
                "window_days": window_days,
                "resets_in": resets_in,
            }
        )

    return rows


def _admin_gate():
    if "mgr_ok" not in st.session_state:
        st.session_state["mgr_ok"] = False

    if not RULE_MGR_PASSWORD:
        st.error("Admin password not configured. Set RULE_MGR_PASSWORD in secrets/env.")
        return False

    with st.form("admin_unlock"):
        p = st.text_input("Admin password", type="password")
        unlock = st.form_submit_button("Unlock")
        if unlock:
            st.session_state["mgr_ok"] = (p == RULE_MGR_PASSWORD)

    if st.session_state["mgr_ok"]:
        if st.button("Lock admin panel"):
            st.session_state["mgr_ok"] = False
            st.rerun()

    return st.session_state["mgr_ok"]


st.set_page_config(page_title="Discipline Engine", layout="wide")

st.markdown(
    """
<style>
:root{
  --bg: rgba(255,255,255,0.03);
  --br: rgba(255,255,255,0.10);
  --muted: rgba(255,255,255,0.65);
  --muted2: rgba(255,255,255,0.50);
}
.block-container{ padding-top: 0.7rem; padding-bottom: 0.9rem; max-width: 1180px; }
h1,h2,h3{ letter-spacing: -0.02em; margin-bottom: 0.2rem; }
.kpi{
  border: 1px solid var(--br);
  background: var(--bg);
  border-radius: 16px;
  padding: 10px 12px;
}
.kpi .label{ color: var(--muted2); font-size: 0.82rem; margin-bottom: 2px; }
.kpi .value{ font-size: 1.25rem; font-weight: 700; line-height: 1.2; }
.kpi .sub{ color: var(--muted); font-size: 0.85rem; margin-top: 4px; }
.panel{
  border: 1px solid var(--br);
  background: var(--bg);
  border-radius: 16px;
  padding: 10px 12px;
}
.smallnote{ color: var(--muted); font-size: 0.85rem; margin-top: 0.15rem; }
.stButton>button{ border-radius: 12px; padding: 0.45rem 0.65rem; }
hr{ opacity: 0.18; margin: 0.45rem 0; }
div[data-testid="stForm"] div[data-testid="stVerticalBlock"]{ gap: 0.0rem !important; }
div[data-testid="stForm"] [data-testid="stWidget"]{ margin-bottom: -0.70rem !important; }
div[data-testid="stForm"] [data-testid="stWidget"] > div{ padding-top: 0 !important; padding-bottom: 0 !important; }
div[data-testid="stToggle"] label{ margin: 0 !important; padding: 0 !important; }

/* ---- FIX: Streamlit tabs labels invisible/collapsed ---- */
div[data-testid="stTabs"] [data-baseweb="tab-list"]{
  gap: 0.35rem !important;
  padding-bottom: 0.25rem !important;
}

div[data-testid="stTabs"] [data-baseweb="tab"]{
  padding: 0.45rem 0.85rem !important;
  min-height: 2.2rem !important;
}

div[data-testid="stTabs"] [data-baseweb="tab"] p,
div[data-testid="stTabs"] [data-baseweb="tab"] span{
  font-size: 0.95rem !important;
  line-height: 1.2 !important;
  color: inherit !important;
  opacity: 1 !important;
  display: block !important;
}
div[data-testid="stTabs"]{
  min-height: 3rem !important;
}
</style>
""",
    unsafe_allow_html=True,
)

try:
    engine.auto_process_until_yesterday()
except ValueError as e:
    st.error(f"Rule configuration error (overlapping effective ranges): {e}")
    st.stop()


sb_client = sb()
today = _today()
tomorrow = _tomorrow()

engine.ensure_app_start_date(sb_client)
open_streak = engine.get_open_streak(sb_client)

s_start = _date(open_streak["start_date"])
processed_through = _date(open_streak["processed_through_date"])
today_locked = processed_through >= today

try:
    di7 = engine.compute_discipline_index(sb_client, processed_through, 7)
    di30 = engine.compute_discipline_index(sb_client, processed_through, 30)
except ValueError as e:
    st.error(f"Rule configuration error (overlapping effective ranges): {e}")
    st.stop()


finalized_len = streak_len_days(s_start, processed_through)
pending_from = processed_through + dt.timedelta(days=1)
pending_days = (today - pending_from).days + 1 if pending_from <= today else 0

rules_today = load_active_rules_for_date(sb_client, today)
logs_today = load_logs_for_date(sb_client, today)

st.title("Discipline Engine")
tab_dash, tab_trend, tab_stats, tab_admin = st.tabs(["Dashboard", "Trend (DI)", "Stats", "Admin"])


with tab_dash:
    st.header("Dashboard")

    k1, k2, k3, k4 = st.columns([1.1, 1.1, 1.1, 1.7])
    with k1:
        st.markdown(
            '<div class="kpi"><div class="label">Streak (finalized)</div>'
            f'<div class="value">{finalized_len}</div>'
            f'<div class="sub">Start: {s_start.isoformat()}</div></div>',
            unsafe_allow_html=True,
        )
    with k2:
        st.markdown(
            '<div class="kpi"><div class="label">Finalized through</div>'
            f'<div class="value">{processed_through.isoformat()}</div>'
            f'<div class="sub">Pending: {pending_days} day(s)</div></div>',
            unsafe_allow_html=True,
        )
    with k3:
        st.markdown(
            '<div class="kpi"><div class="label">Today</div>'
            f'<div class="value">{today.isoformat()}</div>'
            f'<div class="sub">{"LOCKED" if today_locked else "EDITABLE"}</div></div>',
            unsafe_allow_html=True,
        )
    with k4:
        total = len(rules_today)
        saved_pass = sum(1 for r in rules_today if logs_today.get(r["rule_key"], "UNKNOWN") == "PASS")
        st.markdown(
            '<div class="kpi"><div class="label">Progress (saved)</div>'
            f'<div class="value">{saved_pass}/{total}</div>'
            '<div class="sub">Updates apply only on Save/Finalize.</div></div>',
            unsafe_allow_html=True,
        )

    st.markdown("")

    # Today rules
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown("## Today")
    st.markdown('<div class="smallnote">Hover rule label for description. Ordered by weight.</div>', unsafe_allow_html=True)

    with st.form("today_form", clear_on_submit=False):
        ui_states = {}
        for i in range(0, len(rules_today), 2):
            cols = st.columns(2, gap="small")
            pair = rules_today[i : i + 2]
            for j, r in enumerate(pair):
                with cols[j]:
                    rule_key = r["rule_key"]
                    desc = (r.get("description") or "").strip()
                    default_checked = (logs_today.get(rule_key, "UNKNOWN") == "PASS")
                    ui_states[rule_key] = st.toggle(
                        r["name"],
                        value=default_checked,
                        disabled=today_locked,
                        help=desc if desc else None,
                    )

        b1, b2, b3 = st.columns([1, 1, 2.2])
        with b1:
            save_pressed = st.form_submit_button("Save", disabled=today_locked, use_container_width=True)
        with b2:
            finalize_pressed = st.form_submit_button("Finalize", disabled=today_locked, use_container_width=True)
        with b3:
            st.markdown(
                '<div class="smallnote">Finalize locks today and evaluates immediately. Otherwise processes on next run after 00:00.</div>',
                unsafe_allow_html=True,
            )

    if save_pressed:
        ok = upsert_logs_for_date(sb_client, today, ui_states)
        if not ok:
            st.warning("Today is finalized (possibly in another tab). Reloading.")
        time.sleep(0.2)
        st.rerun()


    if finalize_pressed:
        ok = upsert_logs_for_date(sb_client, today, ui_states)
        if ok:
            engine.finalize_today()
        else:
            st.warning("Today is already finalized (possibly in another tab). Reloading.")
        time.sleep(0.2)
        st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)

    # Discipline Index
    st.markdown("")
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown("## Discipline Index")
    st.markdown(
        f'<div class="smallnote">Computed on finalized days only (through {processed_through.isoformat()}). Today is excluded until finalized.</div>',
        unsafe_allow_html=True,
    )


    di7_pct = round(di7["di"] * 100, 1) if di7["days"] > 0 else 0.0
    di30_pct = round(di30["di"] * 100, 1) if di30["days"] > 0 else 0.0

    c1, c2 = st.columns(2, gap="small")
    with c1:
        st.markdown(
            '<div class="kpi"><div class="label">DI (7-day)</div>'
            f'<div class="value">{di7_pct}%</div>'
            f'<div class="sub">Days used: {di7["days"]} • {di7["start_date"].isoformat()} → {di7["end_date"].isoformat()}</div></div>',
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            '<div class="kpi"><div class="label">DI (30-day)</div>'
            f'<div class="value">{di30_pct}%</div>'
            f'<div class="sub">Days used: {di30["days"]} • {di30["start_date"].isoformat()} → {di30["end_date"].isoformat()}</div></div>',
            unsafe_allow_html=True,
        )

    st.markdown("</div>", unsafe_allow_html=True)

    # Buffers (as-of processed_through)
    st.markdown("")
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown("## Buffers")

    if processed_through < s_start:
        st.markdown(
            f'<div class="smallnote">Counts as of {processed_through.isoformat()} (pending days excluded). Rules shown as of streak start {s_start.isoformat()}.</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f'<div class="smallnote">As of {processed_through.isoformat()} (pending days excluded).</div>',
            unsafe_allow_html=True,
        )


    display_date = s_start if processed_through < s_start else processed_through
    rules_asof_pt = load_active_rules_for_date(sb_client, display_date)

    buffer_rows = compute_buffer_view(
        rules=rules_asof_pt,
        rule_state_json=open_streak.get("rule_state_json") or {},
        streak_start=s_start,
        processed_through=processed_through,
    )

    h1, h2, h3, h4 = st.columns([2.2, 0.8, 1.0, 1.6])
    with h1:
        st.caption("Rule")
    with h2:
        st.caption("Remaining buffer")
    with h3:
        st.caption("Window length")
    with h4:
        st.caption("Buffer resets in")

    for row in buffer_rows:
        c1, c2, c3, c4 = st.columns([2.2, 0.8, 1.0, 1.6])
        with c1:
            st.write(row["name"])
        with c2:
            rn = int(row.get("remaining_n", 0))
            if rn < 0:
                st.markdown(f'<span style="color:#ff0000; font-weight:700;">{row["remaining"]}</span>', unsafe_allow_html=True)
            elif rn == 0:
                st.markdown(f'<span style="color:#ff8c00; font-weight:700;">{row["remaining"]}</span>', unsafe_allow_html=True)
            else:
                st.write(row["remaining"])
        with c3:
            st.write(f'{row["window_days"]} days')
        with c4:
            st.write(f'{row["resets_in"]} days')

    st.markdown("</div>", unsafe_allow_html=True)

with tab_trend:
    st.header("Trend (DI)")
    st.markdown("## Trend (last 14 finalized days)")
    st.caption(f"Computed through finalized day {processed_through.isoformat()}. Today excluded until finalized.")

    if processed_through < engine.get_app_start_date(sb_client):
        st.info("No finalized days yet.")
    else:
        ts = engine.compute_di_timeseries(sb_client, processed_through, plot_days=14, windows=(7, 30))
        rows = ts["rows"]

        if not rows:
            st.info("No data to plot yet.")
        else:
            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")

            # rename for display and plot as percent
            df = df.rename(columns={"di1": "DI1", "di7": "DI7", "di30": "DI30"}) * 100.0

            st.line_chart(df[["DI1", "DI7", "DI30"]])

            st.caption(f"Range: {ts['plot_start'].isoformat()} → {ts['end_date'].isoformat()}")

with tab_stats:
    st.header("Stats")

    win = st.selectbox(
        "Consistency window (days)",
        options=[7, 30],
        index=1,  # default 30
    )
    stats_pack = engine.compute_statistics(sb_client, processed_through, consistency_window_days=win)

    g = stats_pack["global"]
    st.markdown("## Global streaks")
    c1, c2, c3, c4, c5, c6 = st.columns(6, gap="small")
    with c1:
        st.markdown(
            '<div class="kpi"><div class="label">Total streaks</div>'
            f'<div class="value">{g["count"]}</div></div>',
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            '<div class="kpi"><div class="label">Avg length</div>'
            f'<div class="value">{g["mean"]:.2f}</div></div>',
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            '<div class="kpi"><div class="label">Median length</div>'
            f'<div class="value">{g["median"]:.2f}</div></div>',
            unsafe_allow_html=True,
        )
    with c4:
        st.markdown(
            '<div class="kpi"><div class="label">Variability (stdev)</div>'
            f'<div class="value">{g["stdev"]:.2f}</div></div>',
            unsafe_allow_html=True,
        )
    with c5:
        st.markdown(
            '<div class="kpi"><div class="label">Min</div>'
            f'<div class="value">{g["min"]}</div></div>',
            unsafe_allow_html=True,
        )
    with c6:
        st.markdown(
            '<div class="kpi"><div class="label">Max</div>'
            f'<div class="value">{g["max"]}</div></div>',
            unsafe_allow_html=True,
        )

    st.markdown("")
    st.markdown("## Rule consistency (top 3 / bottom 3)")
    st.caption(
        f"Window: last {stats_pack['rule_consistency_window_days']} finalized days "
        f"(through {processed_through.isoformat()})."
    )

    cons = pd.DataFrame(stats_pack["consistency"])
    if cons.empty:
        st.info("No rules found.")
    else:
        cons = cons[cons["pass_rate"].notna()].copy()
        cons["pass_rate_pct"] = (cons["pass_rate"] * 100.0).round(1)

        cons = cons.sort_values(["pass_rate", "applicable_days"], ascending=[False, False])

        top3 = cons.head(3)[["name", "pass_rate_pct", "pass_days", "applicable_days"]]
        bot3 = cons.tail(3).sort_values(["pass_rate", "applicable_days"], ascending=[True, False])[
            ["name", "pass_rate_pct", "pass_days", "applicable_days"]
        ]

        a, b = st.columns(2, gap="small")
        with a:
            st.markdown("### Most consistent")
            st.dataframe(top3, use_container_width=True, hide_index=True)
        with b:
            st.markdown("### Least consistent")
            st.dataframe(bot3, use_container_width=True, hide_index=True)

    st.markdown("")
    st.markdown("## Individual rule streaks (PASS runs)")
    rs = pd.DataFrame(stats_pack["rule_streaks"])
    if rs.empty:
        st.info("No rule streak stats.")
    else:
        rs = rs.sort_values(["current_streak", "mean"], ascending=[False, False]).copy()
        rs["mean"] = rs["mean"].round(2)
        rs["median"] = rs["median"].round(2)
        rs["stdev"] = rs["stdev"].round(2)
        st.dataframe(
            rs[["name", "current_streak", "mean", "median", "stdev", "max", "streak_count", "applicable_days"]],
            use_container_width=True,
            hide_index=True,
        )

with tab_admin:
    st.header("Admin")
    # Admin: Rule management
    st.markdown("## Admin")
    ok = _admin_gate()
    if ok:
        
        st.markdown("### Rule Management")
        st.caption(f"Rule changes are scheduled for tomorrow only: {tomorrow.isoformat()} (cannot affect today).")
        st.info("Today’s evaluation uses today’s already-effective rule versions. New versions apply starting tomorrow.")


        rule_keys = engine.admin_list_rule_keys(sb_client)

        action = st.radio(
            "Select action",
            ["Add new rule (new rule_key)", "Add new version (existing rule_key)", "Deactivate rule_key"],
            horizontal=True,
        )

        if action == "Add new rule (new rule_key)":
            with st.form("add_rule"):
                rk = st.text_input("rule_key").strip()
                st.date_input("effective_from", value=tomorrow, disabled=True)
                name = st.text_input("name")
                desc = st.text_area("description / comments", height=80)
                wdays = st.number_input("window_days", min_value=1, step=1, value=7)
                buf = st.number_input("buffer_misses", min_value=0, step=1, value=1)
                wt = st.number_input("weight", value=1.0, step=0.1, format="%.2f")
                submit = st.form_submit_button("Create rule")
                if submit:
                    try:
                        if not rk:
                            raise ValueError("rule_key required")
                        engine.admin_add_new_rule(
                            sb_client, rk, name, desc, int(wdays), int(buf), float(wt)
                        )
                        time.sleep(0.2)
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))

        elif action == "Add new version (existing rule_key)":
            if not rule_keys:
                st.info("No rules exist yet.")
            else:
                with st.form("add_version"):
                    rk2 = st.selectbox("rule_key", rule_keys, key="rk2")

                    latest_today = engine.admin_get_version_applicable_on(sb_client, rk2, today)
                    fallback = (engine.admin_get_versions(sb_client, rk2) or [{}])[-1]

                    base = latest_today if latest_today else fallback

                    st.date_input("effective_from", value=tomorrow, disabled=True, key="eff2")

                    name2 = st.text_input("name", value=(base.get("name") or ""), key="name2")
                    desc2 = st.text_area("description / comments", value="", height=80, key="desc2")
                    wdays2 = st.number_input(
                        "window_days", min_value=1, step=1, value=int(base.get("window_days") or 7), key="wdays2"
                    )
                    buf2 = st.number_input(
                        "buffer_misses", min_value=0, step=1, value=int(base.get("buffer_misses") or 1), key="buf2"
                    )
                    wt2 = st.number_input(
                        "weight", value=float(base.get("weight") or 1.0), step=0.1, format="%.2f", key="wt2"
                    )

                    submit2 = st.form_submit_button("Create version")
                    if submit2:
                        try:
                            engine.admin_add_new_version(
                                sb_client, rk2, name2, desc2, int(wdays2), int(buf2), float(wt2)
                            )
                            time.sleep(0.2)
                            st.rerun()
                        except Exception as e:
                            st.error(str(e))

        else:  # Deactivate rule_key
            if not rule_keys:
                st.info("No rules exist yet.")
            else:
                with st.form("deactivate"):
                    rk3 = st.selectbox("rule_key", rule_keys, key="rk3")
                    st.date_input("effective_from", value=tomorrow, disabled=True, key="eff3")
                    submit3 = st.form_submit_button("Deactivate (effective tomorrow)")
                    if submit3:
                        try:
                            engine.admin_deactivate_rule_key(sb_client, rk3)
                            time.sleep(0.2)
                            st.rerun()
                        except Exception as e:
                            st.error(str(e))

        st.markdown("---")
        st.markdown("### View rule versions")
        if not rule_keys:
            st.info("No rules yet.")
        else:
            sel = st.selectbox("Select rule_key to view versions", rule_keys, key="view_rule_key")
            versions = engine.admin_get_versions(sb_client, sel)
            st.dataframe(versions, use_container_width=True, hide_index=True)
    else:
        st.info("Admin panel locked.")