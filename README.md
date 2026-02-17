# Kaplan-Meier Survival Analysis on Snowflake

This project demonstrates two approaches for generating KM survival curves using Snowflake.

## The Problem

Customers want to ask a Cortex Agent: *"Show me survival curves by MRD status"* and get a visual chart in response.

**Challenge:** Snowflake Intelligence (SI) cannot render matplotlib images or execute Python visualization code. It can only create native charts (line, bar, etc.) from SQL query results.

---

## Two Approaches

### Approach 1: Snowflake Intelligence (Two-Step Process)

**Why two steps?** SI's `data_to_chart` only accepts SQL query results from tables in the semantic view. It cannot:
- Render images from SPROCs
- Chart inline JSON data returned by tools
- Execute Python visualization code

**Flow:**
```
Step 1: User manually runs SPROC → Writes KM data to KM_RESULTS table
Step 2: User asks agent → Agent queries KM_RESULTS → SI renders line chart
```

**Limitations:**
- ❌ No confidence interval bands (SI line charts don't support shaded areas)
- ❌ Two-step process (SPROC must run before asking agent)
- ❌ KM_RESULTS table must be in semantic view
- ✅ Works within SI ecosystem
- ✅ No additional infrastructure needed

**Files:**
- `SPROC_COMPUTE_KM_SURVIVAL.sql` - Creates KM data and saves to table
- `cortex_agent_instructions.txt` - Agent orchestration (SI section)

**Usage:**
```sql
-- Step 1: Generate KM data
CALL COMPUTE_KM_SURVIVAL(
    'NATERA_DUMMY.NATERA_SCHEMA.SURVIVAL_ANALYSIS_DATA',
    'STATUS', 'TIME_DAYS', 'MRD_STATUS',
    'NATERA_DUMMY.NATERA_SCHEMA.KM_RESULTS'
);

-- Step 2: Ask agent
-- "Show me the survival curves"
```

---

### Approach 2: Streamlit in Snowflake (Single-Step Process)

**Why this works:** Streamlit can execute Python code (matplotlib) directly, so we don't need the intermediate table.

**Flow:**
```
User asks in chat → SPROC returns JSON → Streamlit renders matplotlib chart
```

**Advantages:**
- ✅ Single step - no manual SPROC call needed
- ✅ Full matplotlib rendering (step functions, confidence bands)
- ✅ Real-time computation
- ❌ Requires Streamlit app (separate from SI)

**Files:**
- `km_survival_chat_app.py` - Streamlit app with chat interface

**Usage:**
Deploy to Streamlit in Snowflake, then ask:
- "Show me the survival curves"
- "Generate a KM plot"

---

## Data Requirements

**Input table** (`survival_analysis_data.csv`) must have:
| Column | Description |
|--------|-------------|
| STATUS | Event occurred (0=censored, 1=event) |
| TIME_DAYS | Time to event or last follow-up (days) |
| GROUP_COLUMN | (Optional) Column to split curves by (e.g., MRD_STATUS) |

---

## SPROC Details

Uses `scikit-survival` package (`kaplan_meier_estimator`) which returns:
- `time` - unique time points
- `survival_prob` - survival probability at each time
- `conf_int` - 95% confidence intervals (lower, upper)

All three are stored in the output table/JSON.

---

## Summary

| | Snowflake Intelligence | Streamlit |
|---|---|---|
| Steps | 2 (SPROC → Agent) | 1 (Chat → Chart) |
| Confidence Bands | ❌ No | ✅ Yes |
| Infrastructure | SI only | Streamlit app |
| Real-time | ❌ Pre-compute needed | ✅ Yes |
| Native SI Experience | ✅ Yes | ❌ Separate app |

**Choose SI** if you want to stay within the SI ecosystem and don't need confidence bands.

**Choose Streamlit** if you need proper KM plots with confidence intervals and single-step execution.
