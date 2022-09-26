import streamlit as st
from typing import Dict
import pandas as pd
import requests

import bs4
from tqdm.auto import tqdm

SCHEMA_CATS_URL = (
    "https://learn.microsoft.com/azure/azure-monitor/reference/tables/tables-category"
)


def fetch_az_mon_categories() -> requests.models.Response:
    """Return the AzMonitor reference page."""
    return requests.get(SCHEMA_CATS_URL)


def get_security_category_list(resp: requests.models.Response) -> bs4.element.Tag:
    """Extract the list after the security header."""
    soup = bs4.BeautifulSoup(resp.text, "html.parser")

    result = soup.find("div", class_="content")
    sec_header = result.find("h2", id="security")
    return sec_header.find_next_sibling()


def build_table_index(security_cat_list: bs4.element.Tag) -> Dict[str, Dict[str, str]]:
    """From the html list, build an index of URLs."""
    table_prefix = (
        "https://learn.microsoft.com/azure/azure-monitor/reference/tables/{href}"
    )
    return {
        item.a.contents[0]: {
            "href": item.a.attrs.get("href"),
            "url": table_prefix.format(**(item.a.attrs)),
        }
        for item in security_cat_list.find_all("li")
    }


def read_table_from_url(table: str, ref: Dict[str, str]) -> pd.DataFrame:
    """Read table schema from a URL."""
    table_data = pd.read_html(ref["url"])[0]
    table_data["Table"] = table
    table_data["Url"] = ref["url"]
    print(table, table_data.columns)
    return table_data


def fetch_table_schemas(sec_url_dict: Dict[str, Dict[str, str]]) -> pd.DataFrame:
    """Combine schema tables into single DF."""
    print(f"Reading schemas for {len(sec_url_dict)} tables...")
    all_tables = [
        read_table_from_url(table, ref) for table, ref in tqdm(sec_url_dict.items())
    ]
    return pd.concat(all_tables, ignore_index=True)


def main() -> None:
    st.title(":shield: Schema Browser")
    sec_cat_list = get_security_category_list(fetch_az_mon_categories())
    sec_url_dict = build_table_index(sec_cat_list)
    sec_url_dict = {
        key: val for key, val in sec_url_dict.items() if key.startswith("S")
    }
    comb_tables = fetch_table_schemas(sec_url_dict)

    # st.sidebar.subheader("Filter by Table Names")
    # tables = tuple(comb_tables["Table"].unique())
    # st.write("Tables:", tables)

    # TODO : Recursion error - need to troubleshoot - hardcoded table names
    table_selection = st.selectbox(
        "Select a Table name to view schema ?",
        (
            "SecurityAlert",
            "SecurityBaseline",
            "SecurityBaselineSummary",
            "SecurityDetection",
            "SecurityEvent",
            "SecurityIoTRawEvent",
            "SecurityRecommendation",
            "SentinelAudit",
            "SentinelHealth",
            "SigninLogs",
            "Syslog",
        ),
    )

    df_schema = comb_tables[comb_tables["Table"] == table_selection]

    st.subheader("Schema for the filtered table name")
    st.write(df_schema)


if __name__ == "__main__":
    st.set_page_config(
        "Schema Browser",
        "🛡️",
        initial_sidebar_state="expanded",
        layout="wide",
    )
    main()
