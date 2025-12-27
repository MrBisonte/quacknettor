import streamlit as st
from duckel.config import load_config
from duckel.runner import run_pipeline

st.set_page_config(page_title="DuckEL POC", layout="wide")
st.title("DuckEL, DuckDB-powered Extract + Load POC")

cfg = load_config("pipelines.yml")
pipelines = cfg["pipelines"]

name = st.pills("Choose pipeline", list(pipelines.keys()), default=list(pipelines.keys())[0])
p = pipelines[name]

with st.expander("Pipeline config", expanded=False):
    st.json(p)

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

if st.button("Run"):
    overrides = {
        "compute_counts": compute_counts,
        "sample_data": sample_data,
        "sample_rows": sample_rows,
        "compute_summary": compute_summary
    }
    
    with st.spinner("Running pipeline..."):
        result = run_pipeline(p, overrides=overrides)

    st.success(f"Done, {result['rows']} rows")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Count (s)", result["timings"]["count_s"])
    c2.metric("Sample (s)", result["timings"]["sample_s"])
    c3.metric("Summarize (s)", result["timings"]["summary_s"])
    c4.metric("Write (s)", result["timings"]["write_s"])
    c5.metric("Total (s)", result["timings"]["total_s"])

    if result.get("sample") is not None:
        st.subheader("Sample preview")
        st.dataframe(result["sample"], use_container_width=True)

    if result.get("summary") is not None:
        st.subheader("Data Summary")
        st.dataframe(result["summary"], use_container_width=True)

    st.subheader("Write SQL")
    st.code(result["write_sql"], language="sql")
