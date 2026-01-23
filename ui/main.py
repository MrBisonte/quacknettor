import streamlit as st
import traceback
import os
import time
import sys
from pathlib import Path

# Add project root to path to find duckel package
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from duckel.config import load_config, save_pipeline_config
from duckel.config import load_config, save_pipeline_config
from duckel.runner import PipelineRunner, PipelineExecutionError, run_pipeline
from duckel.models import PipelineConfig
from duckel.logger import logger
from duckel.scheduler import SchedulerManager
from datetime import datetime, timedelta
from duckel.jules import JulesClient
import duckdb
from duckel.adapters import create_source_adapter, create_target_adapter

st.set_page_config(
    page_title="DuckEL - Data Pipeline Orchestration",
    page_icon="🦆",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for premium feel
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    
    /* Global Background refresh */
    .stApp {
        background-color: #FAFAFA;
    }
    
    /* Cards */
    .card-container, .stMetric {
        background-color: #FFFFFF;
        padding: 24px;
        border-radius: 12px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
        border: 1px solid #E5E7EB;
        margin-bottom: 20px;
    }

    /* Headings */
    h1, h2, h3 {
        color: #111827;
        font-weight: 700;
        letter-spacing: -0.025em;
    }
    
    /* Metrics */
    .stMetric label {
        color: #6B7280;
        font-size: 0.875rem;
        font-weight: 500;
    }
    .stMetric [data-testid="stMetricValue"] {
        color: #111827;
        font-weight: 600;
        font-size: 1.5rem;
    }

    /* Custom Alert Boxes */
    .success-box {
        padding: 1rem 1.5rem;
        border-radius: 8px;
        background-color: #ECFDF5;
        border: 1px solid #A7F3D0;
        color: #065F46;
        margin: 1rem 0;
        display: flex;
        align-items: center;
        gap: 12px;
        font-weight: 500;
    }
    .error-box {
        padding: 1rem 1.5rem;
        border-radius: 8px;
        background-color: #FFF1F2;
        border: 1px solid #FECDD3;
        color: #9F1239;
        margin: 1rem 0;
        display: flex;
        align-items: center;
        gap: 12px;
        font-weight: 500;
    }
    .info-box {
        padding: 1rem 1.5rem;
        border-radius: 8px;
        background-color: #EFF6FF;
        border: 1px solid #BFDBFE;
        color: #1E40AF;
        margin: 1rem 0;
        display: flex;
        align-items: center;
        gap: 12px;
        font-weight: 500;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: transparent;
        border-bottom: 2px solid #E5E7EB;
        padding-bottom: 10px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 40px;
        white-space: pre-wrap;
        background-color: transparent;
        border: none;
        color: #6B7280;
        font-weight: 500;
        border-radius: 6px;
        padding: 0 16px;
    }
    .stTabs [aria-selected="true"] {
        color: #4F46E5;
        background-color: #EEF2FF;
    }
    
    /* Header Styling */
    .main-header {
        padding: 1rem 0 2rem 0;
        border-bottom: 1px solid #E5E7EB;
        margin-bottom: 2rem;
    }
    .main-header h1 {
        margin: 0;
        font-size: 2.25rem;
        background: linear-gradient(90deg, #4F46E5 0%, #7C3AED 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        padding-bottom: 0.5rem;
    }
    .main-header p {
        color: #6B7280;
        font-size: 1.1rem;
        margin: 0;
    }
</style>
""", unsafe_allow_html=True)

# --- LANDING PAGE ---
if "app_started" not in st.session_state:
    # Custom Start Button Style
    st.markdown("""
    <style>
        .stButton button[kind="primary"] {
            padding: 0.75rem 2rem;
            font-size: 1.1rem;
            font-weight: 600;
            background: linear-gradient(90deg, #4F46E5 0%, #7C3AED 100%);
            border: none;
            box-shadow: 0 4px 6px -1px rgba(79, 70, 229, 0.2);
            transition: all 0.2s;
        }
        .stButton button[kind="primary"]:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 15px -3px rgba(79, 70, 229, 0.3);
        }
        .feature-card {
            background: white;
            padding: 1.5rem;
            border-radius: 12px;
            border: 1px solid #E5E7EB;
            height: 100%;
            text-align: center;
        }
        .feature-icon {
            font-size: 2rem;
            margin-bottom: 1rem;
            display: inline-block;
            padding: 12px;
            background: #EEF2FF;
            border-radius: 12px;
        }
    </style>
    """, unsafe_allow_html=True)

    # Hero Section
    st.markdown("""
    <div style="text-align: center; padding: 4rem 0 3rem 0;">
        <h1 style="text-align: center; font-size: 3.5rem; margin-bottom: 1rem; background: linear-gradient(90deg, #4F46E5 0%, #7C3AED 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">
            Welcome to DuckEL
        </h1>
        <p style="text-align: center; font-size: 1.25rem; color: #6B7280; max-width: 600px; margin: 0 auto;">
            The enterprise-grade orchestration engine for modern data teams. 
            Move data between Parquet, Postgres, and Snowflake with lightning speed.
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Call to Action
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("🚀 Start New Pipeline", type="primary", use_container_width=True, key="btn_start_pipeline"):
            st.session_state["app_started"] = True
            st.rerun()

    st.markdown("<div style='height: 3rem;'></div>", unsafe_allow_html=True)

    # Feature Grid
    c1, c2, c3 = st.columns(3)
    
    with c1:
        st.markdown("""
        <div class="feature-card">
            <div class="feature-icon" style="background: transparent;">
                <img src="https://duckdb.org/images/logo-dl/DuckDB_Logo-stacked.svg" width="60" alt="DuckDB Logo">
            </div>
            <h3>Powered by DuckDB</h3>
            <p style="color: #6B7280;">Leverage the world's fastest in-process SQL OLAP DBMS for lightning-fast transformations.</p>
        </div>
        """, unsafe_allow_html=True)
        
    with c2:
        st.markdown("""
        <div class="feature-card">
            <div class="feature-icon">🔌</div>
            <h3>Universal Connectors</h3>
            <p style="color: #6B7280;">Seamlessly move data between local Parquet storage, PostgreSQL, and Snowflake warehouses.</p>
        </div>
        """, unsafe_allow_html=True)
        
    with c3:
        st.markdown("""
        <div class="feature-card">
            <div class="feature-icon">🤖</div>
            <h3>AI Assisted</h3>
            <p style="color: #6B7280;">Integrated Jules AI agent to help you generate pipelines and debug complex data flows.</p>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='height: 4rem;'></div>", unsafe_allow_html=True)

    # Recent Activity
    st.subheader("🕑 Recent Activity")
    try:
        import pandas as pd
        hist_path = os.path.join("logs", "history.csv")
        if os.path.exists(hist_path):
            df = pd.read_csv(hist_path)
            # Show last 5, newest first
            st.dataframe(
                df.tail(5).iloc[::-1], 
                use_container_width=True,
                hide_index=True,
                column_config={
                    "timestamp": "Execution Time",
                    "pipeline": "Pipeline Name", 
                    "rows": st.column_config.NumberColumn("Rows Processed"),
                    "duration_s": st.column_config.NumberColumn("Duration (s)", format="%.2f"),
                    "status": st.column_config.TextColumn("Status")
                }
            )
        else:
            st.info("No recent pipeline executions found. Start your first job today!")
    except Exception as e:
        st.warning("Could not load recent history.")

    # Stop execution here so we don't render the rest of the app
    st.stop()

# Header
st.markdown("""
<div class="main-header" style="display: flex; align-items: center; gap: 20px;">
    <img src="https://duckdb.org/images/logo-dl/DuckDB_Logo-horizontal.svg" height="40" alt="DuckDB Logo">
    <div style="border-left: 1px solid #E5E7EB; padding-left: 20px;">
        <h1 style="font-size: 1.5rem; margin: 0;">Orchestrator</h1>
        <p style="font-size: 0.875rem; color: #6B7280; margin: 0;">Enterprise-grade data movement</p>
    </div>
</div>
""", unsafe_allow_html=True)

@st.cache_resource
def get_scheduler():
    return SchedulerManager()

scheduler = get_scheduler()

# Sidebar
with st.sidebar:
    st.header("🛠️ Pipeline Setup")
    
    # Mode selector
    config_mode = st.radio(
        "Configuration Mode",
        ["📋 Preset Pipeline", "🔧 Custom Source/Target"],
        horizontal=True
    )
    
    st.divider()
    
    if config_mode == "📋 Preset Pipeline":
        # Traditional pipeline selection
        try:
            pipelines = load_config(os.path.join("configs", "pipelines.yml"))
        except Exception as e:
            st.error(f"❌ Failed to load configuration: {e}")
            st.stop()
        
        pipeline_name = st.selectbox(
            "Select Pipeline",
            options=list(pipelines.keys()),
            help="Choose a configured pipeline to execute"
        )
        pipeline_config = pipelines[pipeline_name]
    
    else:
        # Custom source/target configuration
        st.subheader("📥 Source Configuration")
        
        source_type = st.selectbox(
            "Source Type",
            ["parquet", "postgres", "snowflake"],
            key="src_type"
        )
        
        source_config = {"type": source_type}
        
        if source_type == "parquet":
            source_path = st.text_input(
                "Source Path",
                value="./data/inbound/",
                help="Local path or s3://bucket/path",
                key="src_path"
            )
            source_config["path"] = source_path
        
        elif source_type == "postgres":
            source_config["name"] = st.text_input("Attachment Name", value="pgsrc", key="src_name")
            
            with st.expander("🔐 Connection Details", expanded=True):
                c1, c2 = st.columns(2)
                with c1:
                    pg_host = st.text_input("Host", value="127.0.0.1", key="src_pg_host")
                    pg_user = st.text_input("User", value="myuser", key="src_pg_user")
                    pg_db = st.text_input("Database", value="mydb", key="src_pg_db")
                with c2:
                    pg_port = st.text_input("Port", value="5432", key="src_pg_port")
                    pg_pass = st.text_input("Password", value="__ENV:DUCKEL_PG_PASSWORD", type="password", key="src_pg_pass")

            # Construct connection string internally
            source_config["conn"] = f"dbname={pg_db} user={pg_user} host={pg_host} port={pg_port} password={pg_pass}"
            
            c_test, c_disc = st.columns([1, 1])
            with c_test:
                if st.button("🔌 Test Connection", key="test_src_pg", use_container_width=True):
                    try:
                        with st.spinner("Testing connection..."):
                            t_con = duckdb.connect()
                            adapter = create_source_adapter(source_config)
                            adapter.attach(t_con)
                            st.success("✅ Connection Successful!")
                    except Exception as e:
                        st.error(f"❌ Connection Failed: {e}")

            with c_disc:
                if st.button("🔍 Fetch Tables", key="fetch_src_pg", type="primary", use_container_width=True):
                    try:
                        with st.spinner("Fetching tables..."):
                            t_con = duckdb.connect()
                            adapter = create_source_adapter(source_config)
                            adapter.attach(t_con)
                            # Fetch tables
                            q = f"SELECT table_schema || '.' || table_name FROM {source_config['name']}.information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')"
                            res = t_con.execute(q).fetchall()
                            st.session_state["src_pg_tables"] = sorted([r[0] for r in res])
                            st.success(f"Found {len(res)} tables")
                    except Exception as e:
                        st.error(f"Fetch failed: {e}")

            if st.session_state.get("src_pg_tables"):
                source_config["object"] = st.selectbox("Select Table/View", options=st.session_state["src_pg_tables"], key="src_obj_sel")
            else:
                source_config["object"] = st.text_input("Table/View", value="public.my_table", key="src_obj")
        
        elif source_type == "snowflake":
            source_config["name"] = st.text_input("Attachment Name", value="sfsrc", key="src_name")
            
            with st.expander("🔐 Connection Details", expanded=True):
                c1, c2 = st.columns(2)
                with c1:
                    sf_account = st.text_input("Account", value="__ENV:DUCKEL_SF_ACCOUNT", key="src_sf_acc")
                    sf_user = st.text_input("User", value="__ENV:DUCKEL_SF_USER", key="src_sf_user")
                    sf_warehouse = st.text_input("Warehouse", value="__ENV:DUCKEL_SF_WAREHOUSE", key="src_sf_wh")
                with c2:
                    sf_database = st.text_input("Database", value="__ENV:DUCKEL_SF_DATABASE", key="src_sf_db")
                    sf_schema = st.text_input("Schema", value="__ENV:DUCKEL_SF_SCHEMA", key="src_sf_sch")
                    sf_pass = st.text_input("Password", value="__ENV:DUCKEL_SF_PASSWORD", type="password", key="src_sf_pass")

            source_config["conn"] = f"user={sf_user} password={sf_pass} account={sf_account} warehouse={sf_warehouse} database={sf_database} schema={sf_schema}"
            
            c_test, c_disc = st.columns([1, 1])
            with c_test:
                if st.button("🔌 Test Connection", key="test_src_sf", use_container_width=True):
                    try:
                        with st.spinner("Testing connection..."):
                            t_con = duckdb.connect()
                            adapter = create_source_adapter(source_config)
                            adapter.attach(t_con)
                            st.success("✅ Connection Successful!")
                    except Exception as e:
                        st.error(f"❌ Connection Failed: {e}")
            
            with c_disc:
                if st.button("🔍 Fetch Tables", key="fetch_src_sf", type="primary", use_container_width=True):
                    try:
                        with st.spinner("Fetching tables..."):
                            t_con = duckdb.connect()
                            adapter = create_source_adapter(source_config)
                            adapter.attach(t_con)
                            q = f"SELECT table_schema || '.' || table_name FROM {source_config['name']}.information_schema.tables WHERE table_schema != 'INFORMATION_SCHEMA'"
                            res = t_con.execute(q).fetchall()
                            st.session_state["src_sf_tables"] = sorted([r[0] for r in res])
                            st.success(f"Found {len(res)} tables")
                    except Exception as e:
                        st.error(f"Fetch failed: {e}")

            if st.session_state.get("src_sf_tables"):
                 source_config["object"] = st.selectbox("Select Table/View", options=st.session_state["src_sf_tables"], key="src_sf_sel")
            else:
                 source_config["object"] = st.text_input("Table/View", value="PUBLIC.my_table", key="src_obj")
        
        # Incremental key (optional)
        with st.expander("⏱️ Incremental Options"):
            inc_key = st.text_input("Incremental Key Column", value="", placeholder="e.g., updated_at")
            if inc_key:
                source_config["incremental_key"] = inc_key
        
        st.divider()
        st.subheader("📤 Target Configuration")
        
        target_type = st.selectbox(
            "Target Type",
            ["parquet", "postgres", "snowflake"],
            key="tgt_type"
        )
        
        target_config = {"type": target_type}
        
        if target_type == "parquet":
            target_path = st.text_input(
                "Target Path",
                value="./data/outbound/output.parquet",
                help="Local path or s3://bucket/path",
                key="tgt_path"
            )
            target_config["path"] = target_path
            target_config["compression"] = st.selectbox("Compression", ["zstd", "snappy", "gzip", "none"], key="tgt_comp")
        
        elif target_type == "postgres":
            target_config["name"] = st.text_input("Attachment Name", value="pgtgt", key="tgt_name")
            
            with st.expander("🔐 Connection Details", expanded=True):
                c1, c2 = st.columns(2)
                with c1:
                    pg_host = st.text_input("Host", value="127.0.0.1", key="tgt_pg_host")
                    pg_user = st.text_input("User", value="myuser", key="tgt_pg_user")
                    pg_db = st.text_input("Database", value="mydb", key="tgt_pg_db")
                with c2:
                    pg_port = st.text_input("Port", value="5432", key="tgt_pg_port")
                    pg_pass = st.text_input("Password", value="__ENV:DUCKEL_PG_PASSWORD", type="password", key="tgt_pg_pass")

            target_config["conn"] = f"dbname={pg_db} user={pg_user} host={pg_host} port={pg_port} password={pg_pass}"
            
            c_test, c_disc = st.columns([1, 1])
            with c_test:
                if st.button("🔌 Test Connection", key="test_tgt_pg", use_container_width=True):
                    try:
                        with st.spinner("Testing connection..."):
                            t_con = duckdb.connect()
                            adapter = create_target_adapter(target_config)
                            adapter.attach(t_con)
                            st.success("✅ Connection Successful!")
                    except Exception as e:
                        st.error(f"❌ Connection Failed: {e}")
            
            with c_disc:
                if st.button("🔍 Fetch Tables", key="fetch_tgt_pg", type="primary", use_container_width=True):
                    try:
                        with st.spinner("Fetching tables..."):
                            t_con = duckdb.connect()
                            adapter = create_target_adapter(target_config)
                            adapter.attach(t_con)
                            q = f"SELECT table_schema || '.' || table_name FROM {target_config['name']}.information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')"
                            res = t_con.execute(q).fetchall()
                            st.session_state["tgt_pg_tables"] = sorted([r[0] for r in res])
                            st.success(f"Found {len(res)} tables")
                    except Exception as e:
                        st.error(f"Fetch failed: {e}")

            if st.session_state.get("tgt_pg_tables"):
                target_config["table"] = st.selectbox("Target Table", options=st.session_state["tgt_pg_tables"], key="tgt_pg_sel")
            else:
                target_config["table"] = st.text_input("Target Table", value="public.my_output_table", key="tgt_table")
        
        elif target_type == "snowflake":
            target_config["name"] = st.text_input("Attachment Name", value="sftgt", key="tgt_name")

            with st.expander("🔐 Connection Details", expanded=True):
                c1, c2 = st.columns(2)
                with c1:
                    sf_account = st.text_input("Account", value="__ENV:DUCKEL_SF_ACCOUNT", key="tgt_sf_acc")
                    sf_user = st.text_input("User", value="__ENV:DUCKEL_SF_USER", key="tgt_sf_user")
                    sf_warehouse = st.text_input("Warehouse", value="__ENV:DUCKEL_SF_WAREHOUSE", key="tgt_sf_wh")
                with c2:
                    sf_database = st.text_input("Database", value="__ENV:DUCKEL_SF_DATABASE", key="tgt_sf_db")
                    sf_schema = st.text_input("Schema", value="__ENV:DUCKEL_SF_SCHEMA", key="tgt_sf_sch")
                    sf_pass = st.text_input("Password", value="__ENV:DUCKEL_SF_PASSWORD", type="password", key="tgt_sf_pass")

            target_config["conn"] = f"user={sf_user} password={sf_pass} account={sf_account} warehouse={sf_warehouse} database={sf_database} schema={sf_schema}"
            
            c_test, c_disc = st.columns([1, 1])
            with c_test:
                if st.button("🔌 Test Connection", key="test_tgt_sf", use_container_width=True):
                    try:
                        with st.spinner("Testing connection..."):
                            t_con = duckdb.connect()
                            adapter = create_target_adapter(target_config)
                            adapter.attach(t_con)
                            st.success("✅ Connection Successful!")
                    except Exception as e:
                        st.error(f"❌ Connection Failed: {e}")
            
            with c_disc:
                if st.button("🔍 Fetch Tables", key="fetch_tgt_sf", type="primary", use_container_width=True):
                    try:
                        with st.spinner("Fetching tables..."):
                            t_con = duckdb.connect()
                            adapter = create_target_adapter(target_config)
                            adapter.attach(t_con)
                            q = f"SELECT table_schema || '.' || table_name FROM {target_config['name']}.information_schema.tables WHERE table_schema != 'INFORMATION_SCHEMA'"
                            res = t_con.execute(q).fetchall()
                            st.session_state["tgt_sf_tables"] = sorted([r[0] for r in res])
                            st.success(f"Found {len(res)} tables")
                    except Exception as e:
                        st.error(f"Fetch failed: {e}")

            if st.session_state.get("tgt_sf_tables"):
                target_config["table"] = st.selectbox("Target Table", options=st.session_state["tgt_sf_tables"], key="tgt_sf_sel")
            else:
                target_config["table"] = st.text_input("Target Table", value="PUBLIC.my_output_table", key="tgt_table")
        
        # Common target options
        target_config["mode"] = st.selectbox("Write Mode", ["overwrite", "append", "upsert"], key="tgt_mode")
        
        if target_config["mode"] == "upsert":
            target_config["unique_key"] = st.text_input("Unique Key", placeholder="e.g., id", key="tgt_ukey")
        
        # Build pipeline config dynamically
        from duckel.models import SourceConfig, TargetConfig, PipelineConfig
        try:
            pipeline_config = PipelineConfig(
                source=SourceConfig(**source_config),
                target=TargetConfig(**target_config)
            )
            pipeline_name = f"custom_{source_type}_to_{target_type}"
            
            # Save Preset UI
            with st.expander("💾 Save as Preset"):
                preset_name = st.text_input("Preset Name", value=pipeline_name, help="Name for pipelines.yml")
                if st.button("Save to pipelines.yml", key="btn_save_preset"):
                    try:
                        save_pipeline_config(os.path.join("configs", "pipelines.yml"), preset_name, pipeline_config)
                        st.success(f"✅ Saved as '{preset_name}'!")
                        st.info("💡 Restart app to see new preset.")
                    except Exception as e:
                        st.error(f"❌ Save failed: {e}")

        except Exception as e:
            st.error(f"❌ Invalid configuration: {e}")
            st.stop()
    
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
tab_run, tab_sched, tab_obs, tab_ai = st.tabs(["🚀 Run Pipeline", "📅 Schedule", "👁️ Pipeline Observation", "🤖 AI Assistant (Jules)"])

with tab_run:
    # Visual Lineage
    st.subheader("🔗 Pipeline Flow")
    
    import os
    src_info = f"{pipeline_config.source.type.upper()}"
    if hasattr(pipeline_config.source, "object") and pipeline_config.source.object:
        src_info += f"\\n{pipeline_config.source.object}"
    elif hasattr(pipeline_config.source, "path") and pipeline_config.source.path:
        # Show filename if available, else FILE
        name = os.path.basename(pipeline_config.source.path)
        src_info += f"\\n{name if name else 'FILE'}"
    else:
        src_info += "\\nFILE"

    tgt_info = f"{pipeline_config.target.type.upper()}"
    if hasattr(pipeline_config.target, "table") and pipeline_config.target.table:
        tgt_info += f"\\n{pipeline_config.target.table}"
    elif hasattr(pipeline_config.target, "path") and pipeline_config.target.path:
        name = os.path.basename(pipeline_config.target.path)
        tgt_info += f"\\n{name if name else 'FILE'}"
    else:
        tgt_info += "\\nFILE"
        
    mermaid_graph = f"""
    graph LR
        S["{src_info}"] --> D((DuckEL Engine))
        D --> T["{tgt_info}"]
        style S fill:#EEF2FF,stroke:#4F46E5,stroke-width:2px,color:#1E1B4B
        style D fill:#FFFBEB,stroke:#F59E0B,stroke-width:2px,color:#451A03
        style T fill:#ECFDF5,stroke:#10B981,stroke-width:2px,color:#064E3B
    """
    
    # Render using html component since st.markdown doesn't support mermaid locally
    import streamlit.components.v1 as components
    components.html(
        f"""
        <script type="module">
            import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
            mermaid.initialize({{ startOnLoad: true }});
        </script>
        <div class="mermaid">
            {mermaid_graph}
        </div>
        """,
        height=200,
    )

    # Display config snippet
    with st.expander("📋 Pipeline Blueprint", expanded=False):
        c1, c2 = st.columns(2)
        c1.json(pipeline_config.source.model_dump())
        c2.json(pipeline_config.target.model_dump())

    if st.button("🚀 Run Pipeline", type="primary", use_container_width=True):
        overrides = {
            "compute_counts": compute_counts,
            "sample_data": sample_data,
            "threads": threads,
            "memory_limit": memory_limit,
            "full_refresh": full_refresh,
            "schema_evolution": schema_evolution,
            "compute_summary": compute_summary
        }
        

        
        try:

            # Smart Progress Callback
            class SmartProgress:
                def __init__(self, placeholder, threshold_s=10):
                    self.placeholder = placeholder
                    self.threshold_s = threshold_s
                    self.start_time = time.time()
                    self.shown = False

                def update(self, percent, message):
                    elapsed = time.time() - self.start_time
                    if elapsed > self.threshold_s:
                        if not self.shown:
                            self.shown = True
                        self.placeholder.progress(percent, text=message)

            prog_bg = st.empty()
            smart_prog = SmartProgress(prog_bg, threshold_s=10.0)

            def progress_handler(pct, msg):
                smart_prog.update(pct, msg)

            runner = PipelineRunner(
                pipeline_config, 
                overrides, 
                pipeline_name=pipeline_name,
                progress_callback=progress_handler
            )
            
            # Execute
            result = runner.run()
            
            # Always show completion
            prog_bg.progress(100, text="✅ Done!")
            
            # Results
            st.markdown(f"""
            <div class="success-box">
                <span style="font-size: 1.5rem;">✅</span>
                <div>
                    <strong style="display:block; margin-bottom:4px;">Pipeline Completed Successfully</strong>
                    <span style="opacity: 0.9">Processed <strong>{result['rows']:,}</strong> rows in <strong>{result['timings']['total_s']}s</strong></span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Log history
            try:
                import csv
                from datetime import datetime
                hist_dir = "logs"
                if not os.path.exists(hist_dir):
                    os.makedirs(hist_dir)
                hist_file = os.path.join(hist_dir, "history.csv")
                file_exists = os.path.exists(hist_file)
                with open(hist_file, "a", newline="") as f:
                    writer = csv.writer(f)
                    if not file_exists:
                        writer.writerow(["timestamp", "pipeline", "rows", "duration_s", "status"])
                    writer.writerow([datetime.now().isoformat(), pipeline_name, result['rows'], result['timings']['total_s'], "SUCCESS"])
            except Exception as e:
                logger.error(f"Failed to log history: {e}")
            
            # Metrics
            cols = st.columns(5)
            metrics_map = {
                "rows": ("Rows", result['rows']),
                "total_s": ("Total Time", f"{result['timings']['total_s']}s"),
                "write_s": ("Write Time", f"{result['timings']['write_s']}s"),
                "count_s": ("Count Time", f"{result['timings']['count_s']}s"),
                "summary_s": ("Summary Time", f"{result['timings']['summary_s']}s"),
            }
            
            for k, (label, val) in metrics_map.items():
                with cols[list(metrics_map.keys()).index(k)]:
                    st.metric(label, val)
                    
            if result.get("sample") is not None:
                st.subheader("🔍 Data Preview")
                st.dataframe(result["sample"], use_container_width=True)
                
            if result.get("summary") is not None:
                st.subheader("📊 Summary Statistics")
                summary_df = result["summary"]
                st.dataframe(summary_df, use_container_width=True)
                
                # visualiza distinct counts if available
                if "approx_unique" in summary_df.columns:
                    st.caption("Approximate Unique Values per Column")
                    st.bar_chart(summary_df.set_index("column_name")["approx_unique"])
            
            with st.expander("📝 SQL Audit"):
                st.code(result["write_sql"], language="sql")

        except Exception as e:
            prog_bg.empty() # Use prog_bg instead of my_bar
            st.markdown(f'''
            <div class="error-box">
                <span style="font-size: 1.5rem;">❌</span>
                <div>
                    <strong style="display:block; margin-bottom:4px;">Pipeline Execution Failed</strong>
                    <span style="opacity: 0.9">{str(e)}</span>
                </div>
            </div>
            ''', unsafe_allow_html=True)
            with st.expander("Traceback"):
                st.code(traceback.format_exc())

with tab_sched:
    st.header("📅 Schedule Pipeline Pipeline")
    st.info("⚠️ Note: The application must remain running for scheduled jobs to execute.")
    
    c1, c2 = st.columns(2)
    with c1:
        sched_type = st.radio("Schedule Type", ["One-off", "Recurring (Cron)"])
    
    with c2:
        if sched_type == "One-off":
            run_date = st.date_input("Date", value=datetime.now())
            run_time = st.time_input("Time", value=(datetime.now() + timedelta(minutes=5)).time())
            run_dt = datetime.combine(run_date, run_time)
        else:
            cron_expr = st.text_input("Cron Expression", value="*/30 * * * *", help="Standard cron format")
            
    if st.button("➕ Schedule Job"):
        try:
            # We must pass the raw config dict, not the pydantic model, 
            # because run_pipeline constructs the model.
            # However, pipeline_config is a Pydantic model here.
            # Let's dump it.
            job_name = f"{pipeline_name}_{int(time.time())}"
            cfg_dict = pipeline_config.model_dump()
            
            if sched_type == "One-off":
                if run_dt < datetime.now():
                    st.error("Cannot schedule in the past!")
                else:
                    scheduler.schedule_pipeline_run(run_pipeline, cfg_dict, run_at=run_dt, job_id=job_name)
                    st.success(f"Scheduled '{job_name}' for {run_dt}")
            else:
                try:
                    scheduler.schedule_pipeline_run(run_pipeline, cfg_dict, cron_expr=cron_expr, job_id=job_name)
                    st.success(f"Scheduled recurring job '{job_name}' with cron '{cron_expr}'")
                except Exception as e:
                    st.error(f"Invalid cron expression: {e}")
                    
        except Exception as e:
            st.error(f"Scheduling failed: {e}")

    st.divider()
    st.subheader("📋 Active Jobs")
    jobs = scheduler.get_jobs()
    if not jobs:
        st.write("No active jobs.")
    else:
        for job in jobs:
            c1, c2, c3 = st.columns([2, 2, 1])
            c1.write(f"**ID:** {job.id}")
            c2.write(f"**Next Run:** {job.next_run_time}")
            if c3.button("🗑️", key=f"del_{job.id}"):
                scheduler.remove_job(job.id)
                st.rerun()

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
