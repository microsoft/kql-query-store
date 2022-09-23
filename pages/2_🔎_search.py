import streamlit as st
import pandas as pd

from pathlib import Path


@st.cache(suppress_st_warning=True)
def load_data(nrows):
    data = pd.read_json("..\data\kql_queries.json")
    return data


@st.cache
def convert_df(df, file_type):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    if file_type == "csv":
        data = df.to_csv().encode("utf-8")
    if file_type == "json":
        data = df.to_json().encode("utf-8")

    return data


def main() -> None:
    st.title(":mag_right: Interactive KQL Query Store")

    data_load_state = st.text("Loading data...")
    data = load_data(10000)
    data_load_state.text("Done Loaded and cached !!")
    csv_export = convert_df(data, "csv")
    json_export = convert_df(data, "json")

    with st.expander("Raw Dataframe"):
        if st.checkbox("Show raw data"):
            st.subheader("Raw data")
            st.write(data)
            st.download_button(
                label="Download data as CSV",
                data=csv_export,
                file_name="kql_query_store-export.csv",
                mime="text/csv",
            )

            st.download_button(
                label="Download data as JSON",
                data=json_export,
                file_name="kql_query_store-export.json",
                mime="json",
            )
    st.sidebar.subheader("Filter by Table Names")
    tables = ["SigninLogs", "CommonSecurityLogs", "AWSCloudTrail"]
    table_selections = st.sidebar.multiselect(
        "Select Tables to View", options=tables, default=tables
    )

    st.sidebar.subheader("Filter by KQL Operators")

    operators = ["mv-expand", "parse_json", "parse_xml", "matches regex"]
    symbol_selections = st.sidebar.multiselect(
        "Select KQL operators to View", options=operators, default=operators
    )

    st.subheader(f"KQL Query Store Summary")

    st.metric("Total No of Queries", f"{len(data)}")


if __name__ == "__main__":
    st.set_page_config(
        "Interactive KQL Query Store by MSTIC",
        "🔎",
        initial_sidebar_state="expanded",
        layout="wide",
    )
    main()
