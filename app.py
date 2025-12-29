import streamlit as st
import datetime
from auditor import HabitAuditor

# --- CONFIGURATION ---
st.set_page_config(page_title="Discipline Engine", layout="wide")

# --- SECURITY LAYER ---
def check_password():
    """Returns `True` if the user had the correct password."""
    def password_entered():
        if st.session_state["password"] == st.secrets["APP_PASSWORD"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # clean up
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input
        st.text_input(
            "Enter Access PIN", type="password", on_change=password_entered, key="password"
        )
        return False
    elif not st.session_state["password_correct"]:
        # Password incorrect, show input again + error
        st.text_input(
            "Enter Access PIN", type="password", on_change=password_entered, key="password"
        )
        st.error("⛔ Access Denied")
        return False
    else:
        # Password correct
        return True

if not check_password():
    st.stop()  # SDE Note: This kills the script execution here. Nothing below runs.

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

tab1, tab2 = st.tabs(["🚀 Dashboard", "📈 Analytics"])

with tab1:
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
            
            # FIX: Only apply the lock if we are editing the PAST
            if target_date < today: # or specifically '== yesterday'
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

    # --- DISCIPLINE INDICES ---
    st.subheader("Discipline Index (Weighted)")
    col_di1, col_di2, col_di3 = st.columns(3)

    # 7-Day Index
    di_7 = auditor.calculate_discipline_index(7)
    col_di1.metric("7-Day Form", f"{di_7}%", help="Weighted average of the last 7 days")

    # 30-Day Index
    di_30 = auditor.calculate_discipline_index(30)
    col_di2.metric("30-Day Consistency", f"{di_30}%", help="Weighted average of the last 30 days")

    # Custom Analysis (Collapsed by default)
    with col_di3.expander("Custom Range"):
        custom_days = st.number_input("Days", min_value=1, max_value=365, value=90)
        if st.button("Calculate"):
            custom_di = auditor.calculate_discipline_index(custom_days)
            st.write(f"**{custom_days}-Day Index:** {custom_di}%")

    st.divider()

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

with tab2:
    st.header("Performance Visualizations")
    
    # 1. Line Chart
    st.subheader("Discipline Trend (Last 14 Days)")
    try:
        chart_data = auditor.get_trend_data(view_days=14)
        if not chart_data.empty:
            # Streamlit Line Chart handles the legend automatically based on columns
            st.line_chart(chart_data[['Daily Score', '7-Day Avg', '30-Day Avg']], color=["#FF4B4B", "#1f77b4", "#2ca02c"])
        else:
            st.info("Not enough data to generate trends.")
    except Exception as e:
        st.error(f"Visualization Error: {e}")

    st.divider()

    # 2. Consistency Ranking
    st.subheader("Rule Consistency (All Time)")
    
    rankings = auditor.get_consistency_ranking()
    
    col_best, col_worst = st.columns(2)
    
    with col_best:
        st.markdown("### 🏆 Most Consistent")
        # Top 3
        for i, r in enumerate(rankings[:3]):
            st.write(f"**{i+1}. {r['name']}**")
            st.progress(r['score'] / 100)
            st.caption(f"{r['score']:.1f}% Compliance")

    with col_worst:
        st.markdown("### ⚠️ Needs Improvement")
        # Bottom 3 (reversed)
        for i, r in enumerate(reversed(rankings[-3:])):
            st.write(f"**{i+1}. {r['name']}**")
            st.progress(r['score'] / 100)
            st.caption(f"{r['score']:.1f}% Compliance")