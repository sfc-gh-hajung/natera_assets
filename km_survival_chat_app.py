import streamlit as st
import pandas as pd
import json
import matplotlib.pyplot as plt
from snowflake.snowpark.context import get_active_session

# Page config
st.set_page_config(page_title="KM Survival Analysis Chat", layout="wide")
st.title("Kaplan-Meier Survival Analysis")

SOURCE_TABLE = "NATERA_DUMMY.NATERA_SCHEMA.SURVIVAL_ANALYSIS_DATA"

# Get Snowflake session
session = get_active_session()

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Sidebar for configuration
with st.sidebar:
    st.header("Configuration")
    
    source_table = st.text_input(
        "Source Table", 
        value=SOURCE_TABLE
    )
    status_column = st.text_input("Status Column (0/1)", value="STATUS")
    time_column = st.text_input("Time Column (days)", value="TIME_DAYS")
    group_column = st.text_input("Group Column (optional)", value="MRD_STATUS")
    
    show_ci = st.checkbox("Show Confidence Intervals", value=True)

def call_km_sproc(source_table, status_col, time_col, group_col):
    """Call the KM SPROC and return results"""
    group_param = f"'{group_col}'" if group_col else "NULL"
    
    query = f"""
    CALL NATERA_DUMMY.NATERA_SCHEMA.GET_KM_SURVIVAL_DATA(
        '{source_table}',
        '{status_col}',
        '{time_col}',
        {group_param}
    )
    """
    
    result = session.sql(query).collect()
    return json.loads(result[0][0])

def create_km_plot(data, show_confidence_intervals=True):
    """Create a Kaplan-Meier plot"""
    df = pd.DataFrame(data)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    colors = plt.cm.tab10.colors
    
    for i, group in enumerate(df['group'].unique()):
        group_data = df[df['group'] == group].sort_values('time')
        color = colors[i % len(colors)]
        
        ax.step(group_data['time'], group_data['survival_prob'], 
                where='post', label=group, color=color, linewidth=2)
        
        if show_confidence_intervals:
            ax.fill_between(group_data['time'], 
                          group_data['ci_lower'], 
                          group_data['ci_upper'],
                          alpha=0.2, step='post', color=color)
    
    ax.set_xlabel('Time (days)', fontsize=12)
    ax.set_ylabel('Survival Probability', fontsize=12)
    ax.set_title('Kaplan-Meier Survival Curves', fontsize=14)
    ax.set_ylim(0, 1.05)
    ax.set_xlim(0, None)
    ax.legend(loc='lower left', fontsize=10)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    return fig

def format_statistics(statistics):
    """Format statistics as markdown"""
    lines = ["**Summary Statistics:**"]
    for group, stats in statistics.items():
        if "error" not in stats:
            lines.append(f"- **{group}**: {stats['n_patients']} patients, {stats['n_events']} events ({stats['event_rate_pct']}%), median time {stats['median_time']} days")
    return "\n".join(lines)

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        if message["type"] == "text":
            st.markdown(message["content"])
        elif message["type"] == "plot":
            st.pyplot(message["content"])

# Chat input
if prompt := st.chat_input("Ask for survival analysis (e.g., 'Show me the survival curves')"):
    st.session_state.messages.append({"role": "user", "type": "text", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    survival_keywords = ['survival', 'km', 'kaplan', 'meier', 'curve', 'plot', 'analysis', 'show', 'generate']
    
    with st.chat_message("assistant"):
        if any(keyword in prompt.lower() for keyword in survival_keywords):
            with st.spinner("Computing Kaplan-Meier survival curves..."):
                try:
                    result = call_km_sproc(
                        source_table, 
                        status_column, 
                        time_column, 
                        group_column if group_column else None
                    )
                    
                    if result.get("status") == "success":
                        fig = create_km_plot(result["data"], show_ci)
                        st.pyplot(fig)
                        
                        stats_text = format_statistics(result["statistics"])
                        st.markdown(stats_text)
                        
                        st.session_state.messages.append({"role": "assistant", "type": "plot", "content": fig})
                        st.session_state.messages.append({"role": "assistant", "type": "text", "content": stats_text})
                    else:
                        st.error(f"Error: {result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    st.error(f"Error: {str(e)}")
        else:
            response = "I can help with Kaplan-Meier survival analysis. Try:\n- 'Show me the survival curves'\n- 'Generate a KM plot'\n- 'Analyze survival by group'"
            st.markdown(response)
            st.session_state.messages.append({"role": "assistant", "type": "text", "content": response})
