import streamlit as st
import traceback
from duckel.config import load_config
from duckel.runner import PipelineRunner, PipelineExecutionError
from duckel.models import PipelineConfig
from duckel.logger import logger

st.set_page_config(
    page_title="DuckEL - Data Pipeline Orchestration",
    page_icon="🦆",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .stMetric {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 5px;
    }
    .success-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d4edda;
        border-left: 4px solid #28a745;
        margin: 1rem 0;
    }
    .error-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #f8d7da;
        border-left: 4px solid #dc3545;
        margin: 1rem 0;
    }
    .info-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d1ecf1;
        border-left: 4px solid #17a2b8;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Header with logo/branding
st.title("🦆 DuckEL Pipeline Orchestration")
st.markdown("*High-performance data movement powered by DuckDB*")

# Sidebar for configuration
with st.sidebar:
    st.header("⚙️ Configuration")
    
    # Pipeline selection
    try:
        pipelines = load_config("pipelines.yml")
    except FileNotFoundError as e:
        st.error(f"❌ Configuration file not found: {e}")
        st.stop()
    except ValueError as e:
        st.error(f"❌ Invalid configuration: {e}")
        st.stop()
    except Exception as e:
        st.error(f"❌ Failed to load configuration: {e}")
        st.stop()
    
    pipeline_name = st.selectbox(
        "Select Pipeline",
        options=list(pipelines.keys()),
        help="Choose a configured pipeline to execute"
    )
    
    st.divider()
    
    # Runtime options
    st.subheader("Runtime Options")
    compute_counts = st.checkbox("Compute Row Counts", value=True, help="Count total rows in source")
    sample_data = st.checkbox("Sample Data Preview", value=True, help="Show sample of data")
    
    sample_rows = 50
    if sample_data:
        sample_rows = st.number_input(
            "Sample Size",
            min_value=1,
            max_value=10000,
            value=50,
            help="Number of rows to sample"
        )
    
    compute_summary = st.checkbox("Generate Summary Statistics", value=False, help="Generate statistical summary")
    
    st.divider()
    
    # Performance settings
    with st.expander("⚡ Performance Settings"):
        threads = st.number_input("Threads", min_value=1, max_value=32, value=4, help="Number of threads for DuckDB")
        memory_limit = st.selectbox(
            "Memory Limit",
            ["1GB", "2GB", "4GB", "8GB", "16GB"],
            index=1,
            help="Maximum memory for DuckDB"
        )

# Main content area
pipeline_config = pipelines[pipeline_name]

# Display pipeline configuration
with st.expander("📋 Pipeline Configuration", expanded=False):
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Source")
        st.json(pipeline_config.source.model_dump())
    with col2:
        st.subheader("Target")
        st.json(pipeline_config.target.model_dump())

# Execution
if st.button("▶️ Run Pipeline", type="primary", use_container_width=True):
    overrides = {
        "compute_counts": compute_counts,
        "sample_data": sample_data,
        "sample_rows": sample_rows,
        "compute_summary": compute_summary,
        "threads": threads,
        "memory_limit": memory_limit
    }
    
    # Progress indicators
    progress_container = st.empty()
    status_container = st.empty()
    
    try:
        # Stage 1: Initialize
        with progress_container:
            with st.spinner("🔧 Initializing pipeline..."):
                runner = PipelineRunner(pipeline_config, overrides)
        
        # Stage 2: Execute
        with progress_container:
            with st.spinner("⚙️ Executing pipeline..."):
                result = runner.run()
        
        progress_container.empty()
        
        # Success message
        st.markdown(f"""
        <div class="success-box">
            ✅ <strong>Pipeline executed successfully!</strong><br>
            Processed <strong>{result['rows']:,}</strong> rows in <strong>{result['timings']['total_s']}</strong> seconds
        </div>
        """, unsafe_allow_html=True)
        
        # Metrics
        st.subheader("📊 Execution Metrics")
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                "Count Time",
                f"{result['timings']['count_s']}s",
                help="Time to count rows"
            )
        with col2:
            st.metric(
                "Sample Time",
                f"{result['timings']['sample_s']}s",
                help="Time to sample data"
            )
        with col3:
            st.metric(
                "Summary Time",
                f"{result['timings']['summary_s']}s",
                help="Time to generate summary"
            )
        with col4:
            st.metric(
                "Write Time",
                f"{result['timings']['write_s']}s",
                help="Time to write to target"
            )
        with col5:
            st.metric(
                "Total Time",
                f"{result['timings']['total_s']}s",
                delta=None,
                help="Total execution time"
            )
        
        # Data preview
        if result.get("sample") is not None and len(result["sample"]) > 0:
            st.subheader("🔍 Data Preview")
            st.dataframe(
                result["sample"],
                use_container_width=True,
                height=300
            )
        
        # Summary statistics
        if result.get("summary") is not None and len(result["summary"]) > 0:
            st.subheader("📈 Summary Statistics")
            st.dataframe(
                result["summary"],
                use_container_width=True
            )
        
        # SQL
        with st.expander("📝 Generated SQL", expanded=False):
            st.code(result["write_sql"], language="sql")
        
    except PipelineExecutionError as e:
        progress_container.empty()
        st.markdown(f"""
        <div class="error-box">
            ❌ <strong>Pipeline execution failed</strong><br>
            {str(e)}
        </div>
        """, unsafe_allow_html=True)
        
        with st.expander("🐛 Error Details"):
            st.code(traceback.format_exc())
        
        logger.error(f"Pipeline failed: {e}", exc_info=True)
    
    except Exception as e:
        progress_container.empty()
        st.markdown(f"""
        <div class="error-box">
            ❌ <strong>Unexpected error occurred</strong><br>
            {str(e)}
        </div>
        """, unsafe_allow_html=True)
        
        with st.expander("🐛 Debug Information"):
            st.code(traceback.format_exc())
        
        logger.error(f"Unexpected error: {e}", exc_info=True)

# Footer
st.divider()
st.markdown("""
<div class="info-box">
    ℹ️ <strong>Tips:</strong><br>
    • Enable "Compute Row Counts" for data validation<br>
    • Use "Sample Data Preview" to verify data quality<br>
    • Increase threads and memory for large datasets<br>
    • Check the logs for detailed execution information
</div>
""", unsafe_allow_html=True)
