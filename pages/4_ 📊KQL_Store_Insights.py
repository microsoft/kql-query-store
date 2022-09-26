import streamlit as st
import pandas as pd
import sys

import altair as alt

if ".." not in sys.path:
    sys.path.append("..")

from src.data_store import DataStore

_TEST_JSON = "test_runs/kql_query_db-2022-09-24-02-51-49.json"
ds = DataStore(json_path=_TEST_JSON)


@st.cache(suppress_st_warning=True)
def load_data(nrows):
    data = ds.to_df()
    data = data.head(nrows)
    return data


def main() -> None:
    st.title(":bar_chart: KQL Store Insights")

    data = load_data(5000)

    st.subheader("KQL Query Store Summary")
    st.metric("Total No of Queries", f"{len(data)}")

    data_sentinel = data[data["repo_name"] == "Azure/Azure-Sentinel"]
    st.metric("Total No of Queries in Azure Sentinel Github", f"{len(data_sentinel)}")

    st.subheader("Source Type Ditribution")

    df_source_type = (
        data.groupby("source_type")["query"]
        .count()
        .sort_values(ascending=False)
        .reset_index()
    )

    chart = (
        alt.Chart(df_source_type)
        .mark_bar()
        .encode(x="source_type", y="query")
        .properties(height=400)
    )

    st.altair_chart(chart, use_container_width=True)

    st.subheader(f"Top 5 Community Repos")
    repo_count = (
        data.groupby("repo_name")["query"]
        .count()
        .sort_values(ascending=False)
        .reset_index()
    )
    repo_top = repo_count[repo_count["repo_name"] != "Azure/Azure-Sentinel"].head(5)
    st.write(repo_top)


if __name__ == "__main__":
    st.set_page_config(
        "KQL Store Insights",
        "🛡️",
        initial_sidebar_state="expanded",
        layout="wide",
    )
    main()
