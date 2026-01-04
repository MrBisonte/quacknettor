import streamlit as st
from duckel.config import load_config
from duckel.runner import run_pipeline

st.set_page_config(page_title="DuckEL POC", layout="wide")
st.title("DuckEL, DuckDB-powered Extract + Load POC")

try:
    import os
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "pipelines.yml")
    cfg = load_config(config_path)
    
    # Helper to resolve relative paths in config
    def resolve_paths(d):
        if isinstance(d, dict):
            for k, v in d.items():
                if k == "path" and isinstance(v, str) and (v.startswith("./") or v.startswith("../")):
                    d[k] = os.path.join(base_dir, v)
                else:
                    resolve_paths(v)
        elif isinstance(d, list):
            for item in d:
                resolve_paths(item)

    sources = cfg["sources"]
    targets = cfg["targets"]
    resolve_paths(sources)
    resolve_paths(targets)
except Exception as e:
    st.error(f"Failed to load config: {e}")
    st.stop()

# Source & Target Selection
c_sel1, c_sel2 = st.columns(2)

with c_sel1:
    src_name = st.selectbox("Source", list(sources.keys()))
    source_cfg = sources[src_name]
    with st.expander(f"Source Config ({src_name})", expanded=False):
        st.json(source_cfg)

with c_sel2:
    tgt_name = st.selectbox("Target", list(targets.keys()))
    target_cfg = targets[tgt_name]
    with st.expander(f"Target Config ({tgt_name})", expanded=False):
        st.json(target_cfg)

# Runtime Options
st.subheader("Runtime Options")
c_run1, c_run2, c_run3 = st.columns(3)

with c_run1:
    compute_counts = st.checkbox("Compute Counts", value=True)

with c_run2:
    sample_data = st.checkbox("Sample Data", value=True)
    sample_rows = 50
    if sample_data:
        sample_rows = st.number_input("rows", min_value=1, value=50, label_visibility="collapsed")
        st.caption("Sample Rows")

with c_run3:
    compute_summary = st.checkbox("Summarize Data", value=False)
    
st.write("") # Spacer

# Dynamic overrides based on target type
target_table_override = None
if target_cfg["type"] == "postgres":
    target_table_override = st.text_input("Target Table Override (Optional)", 
                                        placeholder="e.g. public.test_table_v2",
                                        help="Override the destination table name defined in config.")

# --- TABS ---
tab1, tab2 = st.tabs(["Run Pipeline", "AI Assistant (Jules)"])

with tab1:
    if st.button("Run Pipeline", type="primary"):
        # Construct runtime pipeline definition
        p = {
            "source": source_cfg,
            "target": target_cfg,
            "options": {
                 "threads": 4, 
                 "memory_limit": "2GB",
                 "sample_rows": sample_rows
            }
        }

        overrides = {
            "compute_counts": compute_counts,
            "sample_data": sample_data,
            "sample_rows": sample_rows,
            "compute_summary": compute_summary
        }
        
        if target_table_override:
            overrides["target_table"] = target_table_override
        
        with st.spinner("Running pipeline..."):
            try:
                result = run_pipeline(p, overrides=overrides)
            except Exception as e:
                st.error(f"Pipeline failed: {e}")
                result = None

        if result:
            st.success(f"Done, {result['rows']:,} rows")

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Count (s)", result["timings"]["count_s"])
            c2.metric("Sample (s)", result["timings"]["sample_s"])
            c3.metric("Write (s)", result["timings"]["write_s"])
            c4.metric("Total (s)", result["timings"]["total_s"])

            if result.get("sample") is not None:
                st.subheader("Sample preview")
                st.dataframe(result["sample"], width="stretch")

            if result.get("summary") is not None:
                st.subheader("Data Summary")
                st.dataframe(result["summary"], width="stretch")

            st.subheader("Write SQL")
            st.code(result["write_sql"], language="sql")

with tab2:
    st.header("Ask Jules 🤖")
    st.markdown("Use Google's AI Agent to help you generate pipelines or debug issues.")
    
    # Import here to avoid hard dependency at top
    from duckel.jules import JulesClient
    jules = JulesClient()
    
    if not jules.is_configured():
        st.warning("⚠️ JULES_API_KEY not found. Please export it to start a session.")
        st.code("export JULES_API_KEY=AQ.Ab8RN6LMCi8s6pmlHxHK09nQ6HPGboxddpI6KA0cnfTn3CoQlQ", language="bash")
    else:
        prompt = st.text_area("What do you want to do?", placeholder="e.g., Create a pipeline to read from S3 bucket 'my-data' and load into Postgres.")
        
        if st.button("Ask Jules"):
            with st.spinner("Jules is thinking..."):
                session = jules.create_session(prompt)
                
                if "error" in session:
                    st.error(f"Error: {session['error']}")
                else:
                    st.success(f"Session Started: {session['name']}")
                    st.json(session)
                    st.balloons()
