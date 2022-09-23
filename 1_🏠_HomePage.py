import streamlit as st
import pandas as pd

from pathlib import Path


def main() -> None:
    st.title(":mag_right: Interactive KQL Query Store")

    with st.expander("Expand to Read more about the Project"):
        st.write(Path(".\README.md").read_text())

    st.success("Select a page on side bar to naviagate")


if __name__ == "__main__":
    st.set_page_config(
        "Interactive KQL Query Store by MSTIC",
        "🔎",
        initial_sidebar_state="expanded",
        layout="wide",
    )
    main()
