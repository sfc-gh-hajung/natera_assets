CREATE OR REPLACE PROCEDURE NATERA_DUMMY.NATERA_SCHEMA.COMPUTE_KM_SURVIVAL("SOURCE_TABLE" VARCHAR, "STATUS_COLUMN" VARCHAR, "TIME_COLUMN" VARCHAR, "GROUP_COLUMN" VARCHAR DEFAULT null, "OUTPUT_TABLE" VARCHAR DEFAULT 'NATERA_DUMMY.NATERA_SCHEMA.KM_RESULTS')
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','scikit-survival','pandas','numpy')
HANDLER = 'run'
EXECUTE AS OWNER
AS '
import pandas as pd
import numpy as np
from sksurv.nonparametric import kaplan_meier_estimator

def run(session, source_table, status_column, time_column, group_column, output_table):
    
    # Load data from source table
    try:
        df = session.table(source_table).to_pandas()
    except Exception as e:
        return {"error": f"Failed to load table {source_table}: {str(e)}"}
    
    # Validate columns exist
    if status_column not in df.columns:
        return {"error": f"STATUS_COLUMN ''{status_column}'' not found. Available: {list(df.columns)}"}
    if time_column not in df.columns:
        return {"error": f"TIME_COLUMN ''{time_column}'' not found. Available: {list(df.columns)}"}
    if group_column and group_column.strip() and group_column not in df.columns:
        return {"error": f"GROUP_COLUMN ''{group_column}'' not found. Available: {list(df.columns)}"}
    
    # Clean data
    df[status_column] = pd.to_numeric(df[status_column], errors=''coerce'')
    df[time_column] = pd.to_numeric(df[time_column], errors=''coerce'')
    df = df.dropna(subset=[status_column, time_column])
    df = df[df[time_column] > 0]
    
    if len(df) == 0:
        return {"error": "No valid data after cleaning (need TIME > 0 and non-null STATUS)"}
    
    results = []
    stats = {}
    
    # Determine if we''re doing grouped or single analysis
    if not group_column or not group_column.strip():
        # Single curve - no grouping
        group_values = [(''All'', df)]
    else:
        # Multiple curves - group by column
        unique_groups = df[group_column].dropna().unique()
        group_values = [(str(g), df[df[group_column] == g]) for g in unique_groups]
    
    for group_name, group_df in group_values:
        if len(group_df) < 2:
            continue
            
        event = group_df[status_column].astype(bool).values
        time = group_df[time_column].astype(float).values
        
        try:
            # Compute KM with confidence intervals
            time_pts, surv_prob, conf_int = kaplan_meier_estimator(
                event,
                time,
                conf_type="log-log"
            )
            
            # Store each time point
            for i, (t, s) in enumerate(zip(time_pts, surv_prob)):
                results.append({
                    "TIME_DAYS": int(t),
                    "SURVIVAL_PROBABILITY": float(np.round(s, 4)),
                    "CI_LOWER": float(np.round(conf_int[0][i], 4)),
                    "CI_UPPER": float(np.round(conf_int[1][i], 4)),
                    "PATIENT_GROUP": group_name
                })
            
            # Summary stats
            n_pts = int(len(group_df))
            n_events = int(event.sum())
            stats[group_name] = {
                "n_patients": n_pts,
                "n_events": n_events,
                "event_rate_pct": float(np.round(100.0 * n_events / max(1, n_pts), 1)),
                "median_time": float(np.round(np.median(time), 1))
            }
            
        except Exception as e:
            stats[group_name] = {"error": str(e)}
    
    if not results:
        return {"error": "No KM curves could be computed. Check your data."}
    
    # Save to output table
    result_df = pd.DataFrame(results)
    result_df[''UPDATED_AT''] = pd.Timestamp.now()
    
    try:
        session.create_dataframe(result_df).write.mode("overwrite").save_as_table(output_table)
    except Exception as e:
        return {"error": f"Failed to write to {output_table}: {str(e)}"}
    
    # Build summary
    summary_lines = ["**Kaplan-Meier Survival Analysis**"]
    for grp, st in stats.items():
        if "error" in st:
            summary_lines.append(f"- {grp}: Error - {st[''error'']}")
        else:
            summary_lines.append(f"- {grp}: {st[''n_patients'']} pts, {st[''n_events'']} events ({st[''event_rate_pct'']}%), median time {st[''median_time'']} days")
    
    return {
        "status": "success",
        "summary": "\\n".join(summary_lines),
        "statistics": stats,
        "output_table": output_table,
        "rows_written": len(results),
        "groups": list(stats.keys())
    }
';