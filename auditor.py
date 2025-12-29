import os
import datetime
import pytz
from dotenv import load_dotenv
from supabase import create_client, Client
import pandas as pd

load_dotenv()

class HabitAuditor:
    def __init__(self):
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        if not url or not key:
            raise ValueError("Missing Supabase credentials in .env")
        self.supabase: Client = create_client(url, key)
        self.ist = pytz.timezone('Asia/Kolkata')

    def get_today_ist(self):
        """Returns current date in IST."""
        return datetime.datetime.now(self.ist).date()

    def get_special_rule_ids(self):
        """Dynamically finds IDs for Sleep and Grooming."""
        try:
            rules = self.supabase.table("rules").select("id, name").execute().data
            sleep_id = next((r['id'] for r in rules if "Sleep" in r['name']), None)
            groom_id = next((r['id'] for r in rules if "Grooming" in r['name']), None)
            return sleep_id, groom_id
        except Exception:
            return None, None

    def get_anchor(self, rule_id=None):
        """
        Fetches the anchor date (start of streak). 
        If missing, creates one set to TODAY (Day 1).
        """
        query = self.supabase.table("streak_anchors").select("anchor_date")
        if rule_id:
            res = query.eq("rule_id", rule_id).execute()
        else:
            res = query.is_("rule_id", "null").execute()

        if not res.data:
            self.reset_anchor(rule_id)
            return self.get_today_ist()
            
        return datetime.date.fromisoformat(res.data[0]['anchor_date'])

    def reset_anchor(self, rule_id=None):
        """Resets the streak anchor to Today."""
        today = self.get_today_ist().isoformat()
        data = {"rule_id": rule_id, "anchor_date": today}
        # Using upsert to handle both insert and update
        self.supabase.table("streak_anchors").upsert(data, on_conflict="rule_id").execute()

    def check_rule_compliance(self, rule):
        today = self.get_today_ist()
        anchor = self.get_anchor(rule['id'])
        
        # 1. Check if we have logged today
        today_log = self.supabase.table("logs")\
            .select("satisfied")\
            .eq("rule_id", rule['id'])\
            .eq("log_date", today.isoformat()).execute()
        
        has_logged_today = len(today_log.data) > 0
        
        # 2. Define the Evaluation Window
        # The window starts from the Anchor Date.
        # IF we haven't logged today, we don't count today in the window logic yet.
        # This prevents "Buffer - 1" on Day 1 before you've had a chance to log.
        end_search_date = today if has_logged_today else (today - datetime.timedelta(days=1))
        
        # If end_search_date is before anchor (e.g., Day 1 morning), window is 0.
        if end_search_date < anchor:
            days_active = 0
        else:
            days_active = (end_search_date - anchor).days + 1
            
        # 3. Calculate Misses
        # We only check logs within the "Active" window relative to the anchor
        # But we cap it at the rule's rolling window (e.g. last 7 days)
        effective_window = min(rule['window_days'], days_active)
        
        if effective_window == 0:
            # Special Case: Day 1, no logs yet.
            return True, rule['buffer'], 0 # Valid, Full Buffer, 0 Streak

        start_search = (end_search_date - datetime.timedelta(days=effective_window - 1)).isoformat()
        
        # Fetch successes
        res = self.supabase.table("logs").select("id")\
            .eq("rule_id", rule['id'])\
            .eq("satisfied", True)\
            .gte("log_date", start_search)\
            .lte("log_date", end_search_date.isoformat())\
            .execute()
            
        success_count = len(res.data)
        
        # Logic: Misses = Expected Days - Actual Successes
        misses = effective_window - success_count
        buffer_left = rule['buffer'] - misses
        is_valid = buffer_left >= 0
        
        # 4. Streak Calculation
        # Streak is simply total successes since the anchor date
        # This allows the streak to go up by 1 immediately when you log today
        total_streak_res = self.supabase.table("logs").select("id")\
            .eq("rule_id", rule['id'])\
            .eq("satisfied", True)\
            .gte("log_date", anchor.isoformat())\
            .execute()
        
        current_streak = len(total_streak_res.data) if is_valid else 0
        
        return is_valid, buffer_left, current_streak

    def get_global_status(self):
        rules = self.supabase.table("rules").select("*").eq("is_active", True).order("id").execute().data
        
        results = []
        global_fail = False
        
        # 1. Check all rules
        for rule in rules:
            is_valid, buf_left, r_streak = self.check_rule_compliance(rule)
            results.append({
                "id": rule['id'],
                "name": rule['name'],
                "is_valid": is_valid,
                "buffer_left": buf_left,
                "rule_streak": r_streak
            })
            if not is_valid:
                self.reset_anchor(rule['id']) 
                global_fail = True

        # 2. Global Streak Logic
        if global_fail:
            self.reset_anchor(None)
            global_streak = 0
        else:
            global_anchor = self.get_anchor(None)
            app_start = self.get_app_start_date()
            today = self.get_today_ist()
            
            days_elapsed = (today - global_anchor).days
            
            # Check for activity today
            today_logs = self.supabase.table("logs").select("id").eq("log_date", today.isoformat()).execute()
            has_logged_today = len(today_logs.data) > 0
            
            if global_anchor == app_start:
                # SCENARIO A: First Ever Run (Day 1 counts)
                bonus = 1 if has_logged_today else 0
                global_streak = days_elapsed + bonus
            else:
                # SCENARIO B: Reset (Day 0 is DEAD)
                if days_elapsed == 0:
                    # We are ON the day of failure. 
                    # Even if you logged stuff, it's a fail day.
                    global_streak = 0 
                else:
                    # We are past the day of failure (Day 1+).
                    # We subtract the Dead Day (1 day penalty).
                    # We ONLY add the bonus if you logged on this NEW day.
                    bonus = 1 if has_logged_today else 0
                    global_streak = (days_elapsed - 1) + bonus

        return global_streak, results

    def get_app_start_date(self):
        """Fetches the immutable start date of the entire system."""
        try:
            res = self.supabase.table("global_config").select("value").eq("key", "app_start_date").single().execute()
            return datetime.date.fromisoformat(res.data['value'])
        except Exception:
            # Fallback for safety: treat today as start if DB is missing row
            return self.get_today_ist()
    
    def calculate_discipline_index(self, n_days):
        """
        Calculates Weighted Moving Average using an Elastic Window.
        If app age < n_days, it calculates average over the app age (M-day avg).
        """
        today = self.get_today_ist()
        app_start = self.get_app_start_date()
        
        # 1. Determine the Elastic Window
        # The lookback cannot go before the app started.
        lookback_start = today - datetime.timedelta(days=n_days - 1)
        effective_start_date = max(lookback_start, app_start)
        
        # 2. Fetch Weights
        rules = self.supabase.table("rules").select("id, weight").execute().data
        if not rules: return 0.0
        
        weights = {r['id']: r['weight'] for r in rules}
        total_weight = sum(weights.values())
        if total_weight == 0: return 0.0

        # 3. Fetch Logs (Only within the effective window)
        logs = self.supabase.table("logs").select("rule_id, log_date, satisfied")\
            .gte("log_date", effective_start_date.isoformat())\
            .lte("log_date", today.isoformat())\
            .execute().data
            
        # 4. Create DataFrame
        df = pd.DataFrame(logs)
        
        # 5. Vectorized Calculation
        if not df.empty:
            df['log_date'] = pd.to_datetime(df['log_date']).dt.date
            df['weight'] = df['rule_id'].map(weights)
            # Sum satisfied weights per day
            daily_sums = df[df['satisfied'] == True].groupby('log_date')['weight'].sum()
        else:
            daily_sums = pd.Series(dtype=float)

        # 6. Reindex over the EFFECTIVE range
        # If today is Day 5 and we want a 30-day index, full_range is just 5 days.
        # This ensures we don't divide by 30 (which would artificially lower the score).
        full_range = pd.date_range(start=effective_start_date, end=today).date
        
        # Fill missing days with 0.0
        daily_scores = daily_sums.reindex(full_range, fill_value=0.0)
        
        # 7. Normalize
        normalized_scores = (daily_scores / total_weight) * 100
        
        # 8. Average
        # If start date is in the future (edge case), return 0
        if len(normalized_scores) == 0: return 0.0
        
        return round(normalized_scores.mean(), 1)
    
    def get_trend_data(self, view_days=14):
        """
        Generates a DataFrame with Daily Score, 7-Day MA, and 30-Day MA.
        Fetches extra history to ensure the Moving Averages are accurate for the viewed dates.
        """
        # We need 30 days of buffer data to calculate the 30-Day MA for the oldest point in our view
        fetch_days = view_days + 30
        today = self.get_today_ist()
        start_date = today - datetime.timedelta(days=fetch_days)
        
        # 1. Fetch all weights
        rules = self.supabase.table("rules").select("id, weight").execute().data
        weights = {r['id']: r['weight'] for r in rules}
        total_weight = sum(weights.values())
        
        # 2. Fetch logs
        logs = self.supabase.table("logs").select("rule_id, log_date, satisfied")\
            .gte("log_date", start_date.isoformat())\
            .lte("log_date", today.isoformat())\
            .execute().data
            
        if not logs or total_weight == 0:
            return pd.DataFrame()

        # 3. Create DataFrame
        df = pd.DataFrame(logs)
        df['log_date'] = pd.to_datetime(df['log_date']).dt.date
        
        # 4. Pivot to get Score per Day
        # Group by Date -> Sum (Satisfied * Weight)
        def calculate_day_score(group):
            score = 0
            for _, row in group.iterrows():
                if row['satisfied']:
                    score += weights.get(row['rule_id'], 0)
            return round((score / total_weight) * 100, 1)

        daily_scores = df.groupby('log_date').apply(calculate_day_score, include_groups=False).reset_index(name='Daily Score')
        
        # 5. Reindex to fill missing dates with 0 (The Passive Fail Logic)
        full_range = pd.date_range(start=start_date, end=today)
        daily_scores.set_index('log_date', inplace=True)
        daily_scores = daily_scores.reindex(full_range.date, fill_value=0.0)
        daily_scores.index.name = 'Date'
        
        # 6. Calculate Moving Averages
        daily_scores['7-Day Avg'] = daily_scores['Daily Score'].rolling(window=7, min_periods=1).mean()
        daily_scores['30-Day Avg'] = daily_scores['Daily Score'].rolling(window=30, min_periods=1).mean()
        
        # 7. Return only the requested view_days
        return daily_scores.tail(view_days)

    def get_consistency_ranking(self):
        """
        Returns a sorted list of rules by Consistency % (Successes / Total Active Days).
        Uses App Start Date to ensure 'All Time' accuracy even after streak resets.
        """
        today = self.get_today_ist()
        rules = self.supabase.table("rules").select("id, name").execute().data
        
        # FIX: Use Immutable App Start Date, not the mutable Global Anchor
        app_start = self.get_app_start_date()
        total_days = (today - app_start).days + 1
        if total_days < 1: total_days = 1
        
        ranking = []
        for r in rules:
            # Count Successes
            res = self.supabase.table("logs").select("id", count="exact")\
                .eq("rule_id", r['id']).eq("satisfied", True).execute()
            success_count = res.count
            
            consistency = (success_count / total_days) * 100
            ranking.append({"name": r['name'], "score": consistency})
            
        # Sort High to Low
        return sorted(ranking, key=lambda x: x['score'], reverse=True)