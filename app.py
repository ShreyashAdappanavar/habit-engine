import streamlit as st
import datetime
from auditor import HabitAuditor

# --- CONFIGURATION ---
st.set_page_config(page_title="Discipline Engine", layout="wide")

# --- INITIALIZATION ---
try:
    auditor = HabitAuditor()
    today = auditor.get_today_ist()
    yesterday = today - datetime.timedelta(days=1)
    sleep_id, groom_id = auditor.get_special_rule_ids()
except Exception as e:
    st.error(f"System Error: {e}")
    st.stop()

# --- SIDEBAR ---
st.sidebar.header("Temporal Controls")
date_map = {f"Today ({today})": today, f"Yesterday ({yesterday})": yesterday}
selection = st.sidebar.radio("Log Target:", list(date_map.keys()))
target_date = date_map[selection]

# --- DATA FETCHING ---
# We fetch rules and current logs for the target date to determine UI state
rules = auditor.supabase.table("rules").select("*").order("id").execute().data
logs_res = auditor.supabase.table("logs").select("*").eq("log_date", target_date.isoformat()).execute()
current_logs = {l['rule_id']: l['satisfied'] for l in logs_res.data}

# --- MAIN UI ---
st.title("🛡️ Protocol Interface")
st.markdown(f"**Target Date:** `{target_date}`")

# --- FORM SECTION ---
with st.form("daily_log_form"):
    new_entries = []
    
    for rule in rules:
        rule_id = rule['id']
        rule_name = rule['name']
        
        # 1. Determine Permission (Yesterday Logging)
        can_log_yesterday = rule_id in [sleep_id, groom_id] if sleep_id else False
        if target_date == yesterday and not can_log_yesterday:
            st.text(f"🔒 {rule_name} (Same-day only)")
            continue

        # 2. Determine State (Locking History)
        # If a log exists and is FALSE, it is locked. You cannot change History Fail -> Pass.
        is_prev_true = current_logs.get(rule_id, False)
        entry_exists = rule_id in current_logs
        
        is_locked = False
        lock_msg = ""
        
        if entry_exists and not is_prev_true:
            is_locked = True
            lock_msg = "History Locked (Fail cannot become Pass)"


        desc_text = rule.get('description', '') or ""
        final_help = f"🔒 {lock_msg}\n\n{desc_text}" if is_locked else desc_text

        val = st.checkbox(
            rule_name, 
            value=is_prev_true, 
            disabled=is_locked,
            key=f"chk_{rule_id}_{target_date}", 
            help=final_help  # <--- NEW SURGICAL ADDITION
        )
        
        new_entries.append({
            "rule_id": rule_id, 
            "log_date": target_date.isoformat(), 
            "satisfied": val
        })

    # Submit Button
    submitted = st.form_submit_button("Synchronize Protocol")
    if submitted:
        if new_entries:
            try:
                auditor.supabase.table("logs").upsert(new_entries).execute()
                st.success("Protocol Updated.")
                st.rerun()
            except Exception as e:
                st.error(f"Sync Failed: {e}")

# --- DASHBOARD SECTION ---
st.divider()

# Compute Logic
global_streak, rule_stats = auditor.get_global_status()

# 1. Global Status Header
if global_streak > 0:
    st.success(f"### 🔥 GLOBAL STREAK: {global_streak} DAYS")
else:
    # If 0, it might be Day 1 or a Reset.
    st.warning("### ⚠️ GLOBAL STREAK: 0 DAYS")

# 2. Detailed Table
st.subheader("Compliance Matrix")

# Custom Table Layout
col_h1, col_h2, col_h3, col_h4 = st.columns([3, 1, 1, 1])
col_h1.markdown("**Rule**")
col_h2.markdown("**Buffer**")
col_h3.markdown("**Streak**")
col_h4.markdown("**Status**")

for stat in rule_stats:
    c1, c2, c3, c4 = st.columns([3, 1, 1, 1])
    
    # Name
    c1.write(stat['name'])
    
    # Buffer Calculation for Display
    # Find original rule to get max buffer
    orig_rule = next((r for r in rules if r['id'] == stat['id']), None)
    max_buf = orig_rule['buffer'] if orig_rule else "?"
    buf_left = stat['buffer_left']
    
    # Red text if buffer is empty
    if buf_left == 0:
        c2.markdown(f":red[{buf_left} / {max_buf}]")
    else:
        c2.write(f"{buf_left} / {max_buf}")
        
    # Streak
    c3.write(f"{stat['rule_streak']}")
    
    # Status Icon
    c4.write("✅" if stat['is_valid'] else "❌")