import streamlit as st
import pandas as pd
import sys

from pathlib import Path
from st_aggrid import AgGrid
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode

if ".." not in sys.path:
    sys.path.append("..")

from src.data_store import DataStore

_TEST_JSON = "test_runs/kql_query_db-2022-09-24-02-51-49.json"
ds = DataStore(json_path=_TEST_JSON)


@st.cache(suppress_st_warning=True)
def load_data(nrows):
    data = ds.to_df().head(nrows)
    return data


@st.cache
def convert_df(df, file_type):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    if file_type == "csv":
        data = df.to_csv().encode("utf-8")
    if file_type == "json":
        data = df.to_json().encode("utf-8")

    return data


def aggrid_interactive_table(df: pd.DataFrame):
    """Source : https://github.com/streamlit/example-app-interactive-table
    Creates an st-aggrid interactive table based on a dataframe.
    Args:
        df (pd.DataFrame]): Source dataframe
    Returns:
        dict: The selected row
    """
    options = GridOptionsBuilder.from_dataframe(
        df, enableRowGroup=True, enableValue=True, enablePivot=True
    )

    options.configure_side_bar()

    options.configure_selection("single")
    selection = AgGrid(
        df,
        enable_enterprise_modules=True,
        gridOptions=options.build(),
        theme="balham",
        update_mode=GridUpdateMode.MODEL_CHANGED,
        allow_unsafe_jscode=True,
    )

    return selection


def main() -> None:
    st.title(":mag_right: Interactive KQL Query Store")

    data_load_state = st.text("Loading data...")
    data = load_data(5000)
    data_disp = load_data(50)
    data_load_state.text("Data Loaded and cached !!")
    json_export = convert_df(data, "json")

    with st.expander("Raw Dataframe"):
        if st.checkbox("Show raw data"):
            st.subheader("Raw data")
            st.write("Go ahead, click on a row in the table below!")

            selection = aggrid_interactive_table(df=data_disp)

            if selection:
                st.write("You selected:")
                st.json(selection["selected_rows"])

            st.download_button(
                label="Download data as JSON",
                data=json_export,
                file_name="kql_query_store-export.json",
                mime="json",
            )

    st.sidebar.subheader("Filter by Table Names")
    tables = ds.get_filter_lists()["tables"]
    table_selections = st.sidebar.multiselect(
        "Select Tables to View", options=tables, default="CommonSecurityLog"
    )

    st.sidebar.subheader("Filter by KQL Operators")

    operators = ds.get_filter_lists()["operators"]
    operator_selections = st.sidebar.multiselect(
        "Select KQL operators to filter by", options=operators, default="mv-expand"
    )

    st.sidebar.subheader("Filter by KQL Function Calls")

    func_calls = ds.get_filter_lists()["functioncalls"]
    func_calls_selections = st.sidebar.multiselect(
        "Select KQL function calls to filter by",
        options=func_calls,
        default="series_decompose_anomalies",
    )

    result = ds.find_queries(
        # query_name={"contains": "time series"},
        tables=table_selections,  # the list values are OR'd - so will return UNION
        operators=operator_selections,  # the list values are OR'd - so will return UNION
        functioncalls=func_calls_selections,
    )

    st.subheader("Filtered Results matching criteria")
    selection = aggrid_interactive_table(df=result)

    if selection:
        st.write("You selected:")
        st.json(selection["selected_rows"])


if __name__ == "__main__":
    st.set_page_config(
        "Interactive KQL Query Store by MSTIC",
        "🔎",
        initial_sidebar_state="expanded",
        layout="wide",
    )
    main()
