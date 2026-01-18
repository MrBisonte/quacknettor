import streamlit as st
import traceback
import os
import time
from duckel.config import load_config
from duckel.runner import PipelineRunner, PipelineExecutionError
from duckel.models import PipelineConfig
from duckel.logger import logger
from duckel.jules import JulesClient

st.set_page_config(
    page_title="DuckEL - Data Pipeline Orchestration",
    page_icon="🦆",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for premium feel
st.markdown("""
<style>
    .stMetric {
        background-color: #f8f9fa;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .success-box {
        padding: 1.5rem;
        border-radius: 0.8rem;
        background-color: #effaf3;
        border: 1px solid #c3e6cb;
        border-left: 5px solid #28a745;
        margin: 1rem 0;
    }
    .error-box {
        padding: 1.5rem;
        border-radius: 0.8rem;
        background-color: #fdf2f2;
        border: 1px solid #f5c6cb;
        border-left: 5px solid #dc3545;
        margin: 1rem 0;
    }
    .info-box {
        padding: 1.5rem;
        border-radius: 0.8rem;
        background-color: #f0f7ff;
        border: 1px solid #b8daff;
        border-left: 5px solid #007bff;
        margin: 1rem 0;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: transparent;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
</style>
""", unsafe_allow_html=True)

# Header
col_title, col_logo = st.columns([0.8, 0.2])
with col_title:
    st.title("🦆 DuckEL Pipeline Orchestration")
    st.markdown("*Enterprise-grade data movement powered by DuckDB*")

# Sidebar
with st.sidebar:
    st.header("⚙️ Configuration")
    
    try:
        pipelines = load_config("pipelines.yml")
    except Exception as e:
        st.error(f"❌ Failed to load configuration: {e}")
        st.stop()
    
    pipeline_name = st.selectbox(
        "Select Pipeline",
        options=list(pipelines.keys()),
        help="Choose a configured pipeline to execute"
    )
    
    pipeline_config = pipelines[pipeline_name]
    
    st.divider()
    
    # Refresh & Evolution Options
    st.subheader("🔄 Pipeline Controls")
    
    is_incremental = pipeline_config.source.incremental_key is not None
    full_refresh = False
    if is_incremental:
        st.info(f"Incremental Key: `{pipeline_config.source.incremental_key}`")
        try:
            temp_runner = PipelineRunner(pipeline_config, pipeline_name=pipeline_name)
            watermark = temp_runner._get_watermark()
            if watermark:
                st.write(f"Current Watermark: **{watermark}**")
            else:
                st.write("Current Watermark: *None*")
        except:
            pass
        full_refresh = st.checkbox("Full Refresh", value=False)
    
    schema_evolution = st.selectbox(
        "Schema Evolution",
        options=["ignore", "fail", "evolve"],
        index=0
    )

    st.divider()

    # AI Assistant Key
    st.subheader("🤖 AI Assistant")
    env_key = os.environ.get("JULES_API_KEY")
    if not env_key:
        jules_api_key = st.text_input(
            "Jules API Key",
            type="password",
            help="Enter your JULES_API_KEY to enable AI features"
        )
        if jules_api_key:
            os.environ["JULES_API_KEY"] = jules_api_key
    else:
        st.success("✅ AI Key detected")

    st.divider()
    
    # Runtime toggles
    st.subheader("Runtime Stages")
    compute_counts = st.checkbox("Compute Row Counts", value=True)
    sample_data = st.checkbox("Sample Data Preview", value=True)
    compute_summary = st.checkbox("Generate Summary Stats", value=False)
    
    with st.expander("⚡ Advanced Settings"):
        threads = st.number_input("Threads", min_value=1, max_value=64, value=4)
        memory_limit = st.selectbox("Memory Limit", ["1GB", "2GB", "4GB", "8GB", "16GB"], index=1)

# --- MAIN TABS ---
tab_run, tab_obs, tab_ai = st.tabs(["🚀 Run Pipeline", "👁️ Pipeline Observation", "🤖 AI Assistant (Jules)"])

with tab_run:
    # Display config snippet
    with st.expander("📋 Pipeline Blueprint", expanded=False):
        c1, c2 = st.columns(2)
        c1.json(pipeline_config.source.model_dump())
        c2.json(pipeline_config.target.model_dump())

    if st.button("▶️ Execute Pipeline", type="primary", use_container_width=True):
        overrides = {
            "compute_counts": compute_counts,
            "sample_data": sample_data,
            "threads": threads,
            "memory_limit": memory_limit,
            "full_refresh": full_refresh,
            "schema_evolution": schema_evolution,
            "compute_summary": compute_summary
        }
        
        progress_text = "Operation in progress. Please wait."
        my_bar = st.progress(0, text=progress_text)
        
        try:
            # Stage 1: Initialize
            my_bar.progress(10, text="🔧 Initializing adapters...")
            runner = PipelineRunner(pipeline_config, overrides, pipeline_name=pipeline_name)
            
            # Stage 2: Execute
            my_bar.progress(40, text="⚙️ Moving data...")
            result = runner.run()
            
            my_bar.progress(100, text="✅ Done!")
            time.sleep(1)
            my_bar.empty()
            
            # Results
            st.markdown(f"""
            <div class="success-box">
                ✅ <strong>Success!</strong> Processed <strong>{result['rows']:,}</strong> rows in <strong>{result['timings']['total_s']}s</strong>.
            </div>
            """, unsafe_allow_html=True)
            
            # Metrics
            cols = st.columns(5)
            metrics_map = {
                "Count": "count_s",
                "Sample": "sample_s",
                "Summary": "summary_s",
                "Write": "write_s",
                "Total": "total_s"
            }
            for i, (label, key) in enumerate(metrics_map.items()):
                cols[i].metric(label, f"{result['timings'][key]}s")
            
            # Previews
            if result.get("sample") is not None:
                st.subheader("🔍 Data Preview")
                st.dataframe(result["sample"], use_container_width=True)
                
            if result.get("summary") is not None:
                st.subheader("📊 Summary Statistics")
                st.dataframe(result["summary"], use_container_width=True)
            
            with st.expander("📝 SQL Audit"):
                st.code(result["write_sql"], language="sql")

        except Exception as e:
            my_bar.empty()
            st.markdown(f'<div class="error-box">❌ <strong>Error:</strong> {str(e)}</div>', unsafe_allow_html=True)
            with st.expander("Traceback"):
                st.code(traceback.format_exc())

with tab_obs:
    st.header("📊 Logs & Observability")
    st.markdown("Real-time pipeline execution logs with status tracking.")
    
    log_file = "duckel.log"
    
    # Auto-refresh toggle
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        auto_refresh = st.checkbox("Auto-refresh", value=False)
    with col2:
        if st.button("🔄 Refresh Now"):
            st.rerun()
    with col3:
        if st.button("🗑️ Clear Logs"):
            if os.path.exists(log_file):
                open(log_file, "w").close()
            st.rerun()
    
    if auto_refresh:
        import time as _time
        _time.sleep(2)
        st.rerun()
    
    st.divider()
    
    if os.path.exists(log_file):
        with open(log_file, "r") as f:
            lines = f.readlines()
        
        if lines:
            # Summary metrics
            total_lines = len(lines)
            error_count = sum(1 for l in lines if "ERROR" in l)
            warning_count = sum(1 for l in lines if "WARNING" in l)
            info_count = sum(1 for l in lines if "INFO" in l)
            
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Entries", total_lines)
            m2.metric("Errors", error_count, delta=None if error_count == 0 else f"{error_count}", delta_color="inverse")
            m3.metric("Warnings", warning_count)
            m4.metric("Info", info_count)
            
            st.divider()
            
            # Filter options
            filter_col1, filter_col2 = st.columns([1, 3])
            with filter_col1:
                level_filter = st.selectbox("Filter by Level", ["ALL", "ERROR", "WARNING", "INFO", "DEBUG"])
            with filter_col2:
                search_term = st.text_input("Search logs", placeholder="Enter search term...")
            
            # Filter logs
            filtered_lines = lines
            if level_filter != "ALL":
                filtered_lines = [l for l in filtered_lines if level_filter in l]
            if search_term:
                filtered_lines = [l for l in filtered_lines if search_term.lower() in l.lower()]
            
            # Display logs with syntax highlighting
            st.subheader(f"Log Entries ({len(filtered_lines)} shown)")
            
            # Show last 100 filtered lines, newest first
            display_lines = filtered_lines[-100:][::-1]
            
            for line in display_lines:
                line = line.strip()
                if not line:
                    continue
                
                # Color code by level
                if "ERROR" in line:
                    st.markdown(f'<div class="error-box" style="padding:0.5rem;margin:0.2rem 0;font-family:monospace;font-size:0.85rem;">❌ {line}</div>', unsafe_allow_html=True)
                elif "WARNING" in line:
                    st.markdown(f'<div style="padding:0.5rem;margin:0.2rem 0;background:#fff3cd;border-left:3px solid #ffc107;font-family:monospace;font-size:0.85rem;">⚠️ {line}</div>', unsafe_allow_html=True)
                elif "INFO" in line:
                    st.markdown(f'<div class="info-box" style="padding:0.5rem;margin:0.2rem 0;font-family:monospace;font-size:0.85rem;">ℹ️ {line}</div>', unsafe_allow_html=True)
                else:
                    st.markdown(f'<div style="padding:0.5rem;margin:0.2rem 0;background:#f8f9fa;border-left:3px solid #6c757d;font-family:monospace;font-size:0.85rem;">🔍 {line}</div>', unsafe_allow_html=True)
        else:
            st.info("Log file exists but is empty. Run a pipeline to generate logs.")
    else:
        st.warning("⚠️ No log file found. Run a pipeline to generate logs.")
        st.markdown("""
        **Expected log file**: `duckel.log`
        
        Logs will appear here after you execute a pipeline. Each entry includes:
        - **Timestamp** - When the event occurred
        - **Level** - ERROR, WARNING, INFO, or DEBUG
        - **Component** - Which module generated the log
        - **Message** - Details about the event
        """)

with tab_ai:
    st.header("Ask Jules 🤖")
    st.markdown("Use Google's AI Agent to help you generate pipelines or debug issues.")
    
    jules = JulesClient()
    if not jules.is_configured():
        st.warning("⚠️ `JULES_API_KEY` not found. Please set it to enable AI features.")
        st.code("export JULES_API_KEY=...", language="bash")
    else:
        prompt = st.text_area("How can Jules help you today?", placeholder="e.g., Create a pipeline to load CSV data from /data to Postgres.")
        if st.button("Ask Jules"):
            with st.spinner("Jules is processing..."):
                resp = jules.create_session(prompt)
                if "error" in resp:
                    st.error(resp["error"])
                else:
                    st.success("Session initiated!")
                    st.json(resp)
                    st.balloons()

# Footer
st.divider()
st.caption("Powered by DuckDB & Pydantic. Dedicated to fast data engineering.")
