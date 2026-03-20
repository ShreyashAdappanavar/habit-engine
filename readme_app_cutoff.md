# README.md

## Discipline Engine: Temporal Logic

This application does not follow a standard midnight-to-midnight calendar. It uses a **Logical Day** boundary to align with human sleep cycles and late-night productivity.

---

### The 04:30 AM Cutoff
The day officially "ends" and resets at **04:30 AM IST**. 

Events occurring between 12:00 AM and 04:29 AM are logically attributed to the **previous** calendar day. This ensures that late-night work sessions are counted toward the correct discipline cycle and do not cause premature streak failures.

### Implementation
The system calculates the current logical date by shifting the wall-clock time backward by 4 hours and 30 minutes.

$$LogicalDate = (CurrentTime_{IST} - DayCutoffDelta).date()$$

* **Timezone:** Locked to `Asia/Kolkata` (IST).
* **Delta:** 04:30:00 (4.5 hours).

### Usage Notes
* **Check-in:** You cannot log or view the dashboard for a new day until the wall-clock passes 04:30 AM.
* **Finalization:** The engine automatically processes "yesterday" (anything before the current 04:30 AM boundary) upon the first run of the new logical day.
* **Data Integrity:** All tables (`rule_logs`, `streaks`, `daily_checkins`) use this calculated date as the primary key.

---

Would you like me to generate a simple shell script to automate the Streamlit launch?