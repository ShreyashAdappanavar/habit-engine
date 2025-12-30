import datetime as dt
import os
import time

import streamlit as st
from supabase import create_client

import engine


def sb():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])


def load_active_rules_for_date(sb_client, d: dt.date):
    rows = (
        sb_client.table("rule_defs")
        .select("*")
        .lte("effective_from", d.isoformat())
        .order("rule_key", desc=False)
        .order("effective_from", desc=True)
        .execute()
        .data
    )
    latest = {}
    for r in rows:
        k = r["rule_key"]
        if k not in latest and r.get("is_active", True):
            latest[k] = r

    return sorted(latest.values(), key=lambda r: (-float(r.get("weight", 1)), r.get("name", "")))


def load_logs_for_date(sb_client, d: dt.date):
    rows = (
        sb_client.table("rule_logs")
        .select("rule_key,state")
        .eq("log_date", d.isoformat())
        .execute()
        .data
    )
    return {r["rule_key"]: r["state"] for r in rows}


def upsert_logs_for_date(sb_client, d: dt.date, states: dict):
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

/* compress form widget spacing */
div[data-testid="stForm"] div[data-testid="stVerticalBlock"]{ gap: 0.0rem !important; }
div[data-testid="stForm"] [data-testid="stWidget"]{ margin-bottom: -0.70rem !important; }
div[data-testid="stForm"] [data-testid="stWidget"] > div{ padding-top: 0 !important; padding-bottom: 0 !important; }
div[data-testid="stToggle"] label{ margin: 0 !important; padding: 0 !important; }
</style>
""",
    unsafe_allow_html=True,
)

engine.auto_process_until_yesterday()

sb_client = sb()
today = dt.date.today()

engine.ensure_app_start_date(sb_client)
open_streak = engine.get_open_streak(sb_client)

s_start = dt.date.fromisoformat(open_streak["start_date"])
processed_through = dt.date.fromisoformat(open_streak["processed_through_date"])
today_locked = processed_through >= today

di7 = engine.compute_discipline_index(sb_client, processed_through, 7)
di30 = engine.compute_discipline_index(sb_client, processed_through, 30)
di7_pct = round(di7["di"] * 100, 1)
di30_pct = round(di30["di"] * 100, 1)


finalized_len = streak_len_days(s_start, processed_through)
pending_from = processed_through + dt.timedelta(days=1)
pending_days = (today - pending_from).days + 1 if pending_from <= today else 0

rules = load_active_rules_for_date(sb_client, today)
logs = load_logs_for_date(sb_client, today)

st.markdown("# Discipline Engine")

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
    total = len(rules)
    saved_pass = sum(1 for r in rules if logs.get(r["rule_key"], "UNKNOWN") == "PASS")
    st.markdown(
        '<div class="kpi"><div class="label">Progress (saved)</div>'
        f'<div class="value">{saved_pass}/{total}</div>'
        '<div class="sub">Updates apply only on Save/Finalize.</div></div>',
        unsafe_allow_html=True,
    )

st.markdown("")

# Rules section full width, 2-column grid
st.markdown('<div class="panel">', unsafe_allow_html=True)
st.markdown("## Today")
st.markdown('<div class="smallnote">Hover rule label for description. Ordered by weight.</div>', unsafe_allow_html=True)

with st.form("today_form", clear_on_submit=False):
    ui_states = {}

    for i in range(0, len(rules), 2):
        cols = st.columns(2, gap="small")
        pair = rules[i : i + 2]
        for j, r in enumerate(pair):
            with cols[j]:
                rule_key = r["rule_key"]
                desc = (r.get("description") or "").strip()
                default_checked = (logs.get(rule_key, "UNKNOWN") == "PASS")
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
            '<div class="smallnote">Finalize locks today and evaluates immediately. Otherwise auto at 00:00 tomorrow.</div>',
            unsafe_allow_html=True,
        )

if save_pressed:
    upsert_logs_for_date(sb_client, today, ui_states)
    st.success("Saved.")
    time.sleep(1.0)
    st.rerun()

if finalize_pressed:
    upsert_logs_for_date(sb_client, today, ui_states)
    engine.finalize_today()
    st.success("Finalized.")
    time.sleep(1.0)
    st.rerun()

st.markdown("</div>", unsafe_allow_html=True)

# Discipline Index section (between Rules and Buffers)
st.markdown("")
st.markdown('<div class="panel">', unsafe_allow_html=True)
st.markdown("## Discipline Index")
st.markdown(
    f'<div class="smallnote">Computed on finalized days only (through {processed_through.isoformat()}).</div>',
    unsafe_allow_html=True,
)

di7 = engine.compute_discipline_index(sb_client, processed_through, 7)
di30 = engine.compute_discipline_index(sb_client, processed_through, 30)

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


# Buffers section at bottom
st.markdown("")
st.markdown('<div class="panel">', unsafe_allow_html=True)
st.markdown("## Buffers")
st.markdown(
    f'<div class="smallnote">As of {processed_through.isoformat()} (pending days excluded).</div>',
    unsafe_allow_html=True,
)
st.markdown("")

buffer_rows = compute_buffer_view(
    rules=rules,
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
