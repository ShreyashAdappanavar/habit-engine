import pandas as pd
from auditor import HabitAuditor
import datetime
import sys

# --- SETUP ---
try:
    auditor = HabitAuditor()
    print("✅ Successfully connected to Supabase and initialized Auditor.")
except Exception as e:
    print(f"❌ CRITICAL ERROR: Could not connect. Check .env file.\n{e}")
    sys.exit(1)

def header(text):
    print(f"\n{'='*60}\n {text} \n{'='*60}")

def subheader(text):
    print(f"\n--- {text} ---")

# --- 1. SYSTEM CONSTANTS ---
header("1. SYSTEM CONFIGURATION")
today_ist = auditor.get_today_ist()
app_start = auditor.get_app_start_date()
print(f"Current Date (IST) : {today_ist}")
print(f"App Start Date     : {app_start}")
print(f"System Age         : {(today_ist - app_start).days + 1} days")

# --- 2. RAW DATA TABLES ---
header("2. DATABASE SNAPSHOTS")

# Rules
subheader("Table: rules")
rules = auditor.supabase.table("rules").select("*").order("id").execute().data
if rules:
    df_rules = pd.DataFrame(rules)
    # Select key columns for cleaner display
    print(df_rules[["id", "name", "buffer", "window_days", "weight"]].to_string(index=False))
else:
    print("⚠️ Rules table is EMPTY.")

# Streak Anchors
subheader("Table: streak_anchors")
anchors = auditor.supabase.table("streak_anchors").select("*").execute().data
if anchors:
    print(pd.DataFrame(anchors).to_string(index=False))
else:
    print("⚠️ No Anchors found (System might be uninitialized).")

# Logs (Last 14 Days)
subheader("Table: logs (Last 14 Days)")
start_log_view = today_ist - datetime.timedelta(days=14)
logs = auditor.supabase.table("logs").select("*")\
    .gte("log_date", start_log_view.isoformat())\
    .order("log_date", desc=True)\
    .execute().data

if logs:
    df_logs = pd.DataFrame(logs)
    # Pivot for readability: Rows=Date, Cols=Rule ID
    try:
        pivot = df_logs.pivot(index="log_date", columns="rule_id", values="satisfied")
        print(pivot.fillna(".").to_string())
    except Exception as e:
        print(f"Could not pivot logs: {e}")
        print(df_logs.head())
else:
    print("⚠️ No logs found in the last 14 days.")


# --- 3. LOGIC DIAGNOSTICS (THE AUDITOR BRAIN) ---
header("3. LOGIC DIAGNOSTICS")

print(f"{'ID':<4} {'Rule Name':<25} {'Anchor':<12} {'Window':<8} {'Logs(Win)':<10} {'Misses':<8} {'Buffer':<8} {'Streak':<8} {'Status'}")
print("-" * 105)

global_streak, results = auditor.get_global_status()

for res in results:
    rule_id = res['id']
    rule_data = next(r for r in rules if r['id'] == rule_id)
    
    # Re-run logic components to expose hidden math
    anchor = auditor.get_anchor(rule_id)
    
    # 1. Check logged today status for "Immunity" logic
    today_log = auditor.supabase.table("logs").select("satisfied")\
        .eq("rule_id", rule_id).eq("log_date", today_ist.isoformat()).execute()
    has_logged_today = len(today_log.data) > 0

    # 2. Calculate Effective Window
    end_search_date = today_ist if has_logged_today else (today_ist - datetime.timedelta(days=1))
    
    if end_search_date < anchor:
        days_active = 0
    else:
        days_active = (end_search_date - anchor).days + 1
        
    effective_window = min(rule_data['window_days'], days_active)
    
    # 3. Count Actual Successes in Window
    start_search = (end_search_date - datetime.timedelta(days=effective_window - 1)).isoformat() if effective_window > 0 else today_ist.isoformat()
    
    success_logs = auditor.supabase.table("logs").select("id")\
        .eq("rule_id", rule_id).eq("satisfied", True)\
        .gte("log_date", start_search)\
        .lte("log_date", end_search_date.isoformat())\
        .execute().data
    success_count = len(success_logs)
    
    # 4. Misses
    misses = effective_window - success_count
    
    # Print Row
    status_icon = "✅" if res['is_valid'] else "❌"
    print(f"{rule_id:<4} {rule_data['name'][:25]:<25} {str(anchor):<12} {effective_window:<8} {success_count:<10} {misses:<8} {res['buffer_left']:<8} {res['rule_streak']:<8} {status_icon}")

print("-" * 105)
print(f"🔥 GLOBAL STREAK CALCULATION: {global_streak}")


# --- 4. DISCIPLINE INDEX DEBUG ---
header("4. DISCIPLINE INDEX (ELASTIC WINDOW)")

def debug_di(n_days):
    print(f"\n--- Analyzing {n_days}-Day Index ---")
    
    lookback_start = today_ist - datetime.timedelta(days=n_days - 1)
    effective_start = max(lookback_start, app_start)
    
    print(f"Requested Window : {lookback_start} to {today_ist}")
    print(f"App Start Date   : {app_start}")
    print(f"Effective Window : {effective_start} to {today_ist}")
    
    score = auditor.calculate_discipline_index(n_days)
    print(f">> CALCULATED SCORE: {score}%")

debug_di(7)
debug_di(30)
debug_di(90)

print("\n\n✅ DIAGNOSTIC COMPLETE")