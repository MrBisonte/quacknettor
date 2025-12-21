import streamlit as st
from duckel.config import load_config
from duckel.runner import run_pipeline

st.set_page_config(page_title="DuckEL POC", layout="wide")
st.title("DuckEL, DuckDB-powered Extract + Load POC")

cfg = load_config("pipelines.yml")
pipelines = cfg["pipelines"]

name = st.selectbox("Choose pipeline", list(pipelines.keys()))
p = pipelines[name]

with st.expander("Pipeline config", expanded=False):
    st.json(p)

if st.button("Run"):
    with st.spinner("Running pipeline..."):
        result = run_pipeline(p)

    st.success(f"Done, {result['rows']:,} rows")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Count time (s)", result["timings"]["count_s"])
    c2.metric("Sample time (s)", result["timings"]["sample_s"])
    c3.metric("Write time (s)", result["timings"]["write_s"])
    c4.metric("Total time (s)", result["timings"]["total_s"])

    st.subheader("Sample preview")
    st.dataframe(result["sample"], use_container_width=True)

    st.subheader("Write SQL")
    st.code(result["write_sql"], language="sql")
