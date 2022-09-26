import streamlit as st


def main() -> None:
    st.subheader("Reach out to Project team via Github")
    st.subheader("Github: https://github.com/microsoft/kql-query-store")

    st.write(
        "If you would like to add new Github repositories as source, open a issue on Github"
    )


if __name__ == "__main__":
    st.set_page_config("Contact Us !!", "💬")
    main()
