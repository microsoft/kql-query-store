# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""Azure Monitor Schema creation."""
__author__ = "Ian Hellen"
import json
from pathlib import Path
from typing import Any, Dict, Optional, Union

import bs4
import pandas as pd
import requests
from tqdm.auto import tqdm

SCHEMA_CATS_URL = (
    "https://learn.microsoft.com/azure/azure-monitor/reference/tables/tables-category"
)


class AzMonitorSchemas:
    """Class to download and store Azure Monitor table schemas."""

    def __init__(
        self, json_path: Union[None, str, Path] = None, json_text: Optional[str] = None
    ):
        """Initialize the schema class."""
        self.schemas: Optional[pd.DataFrame] = None
        if json_path or json_text:
            self.schemas = self._df_from_json(json_path=json_path, json_text=json_text)

    def get_az_mon_schemas(self):
        """Retrieve Azure monitor schemas"""
        sec_cat_list = _get_security_category_list(_fetch_az_mon_categories())
        sec_url_dict = _build_table_index(sec_cat_list)
        self.schemas = _fetch_table_schemas(sec_url_dict).reindex(
            columns=["Table", "Column", "Type", "Description", "Url"]
        )

    @property
    def schema_dict(self) -> Dict[str, Dict[str, Any]]:
        """Return the schema as a dictionary."""
        if self.schemas is None:
            return {}
        table_dict = {}
        for table, df in self.schemas.groupby("Table"):
            url = df.iloc[0]["Url"]
            table_dict[table.casefold()] = {
                "url": url,
                "table": table,
                "schema": df.drop(columns=["Table", "Url"]).to_dict(orient="records"),
            }
        return table_dict

    def to_json(self):
        """Return schemas as JSON string."""
        return json.dumps(self.schema_dict)

    @staticmethod
    def _df_from_json(
        json_path: Union[None, str, Path] = None, json_text: Optional[str] = None
    ) -> pd.DataFrame:
        """Create DataFrame from JSON representation."""
        if json_path:
            json_text = Path(json_path).read_text(encoding="utf-8")
        schema_dict = json.loads(json_text)
        rows = []
        for item in schema_dict.values():
            rows.extend(
                {
                    "Table": item["table"],
                    "Column": schema.get("Column"),
                    "Type": schema.get("Type"),
                    "Description": schema.get("Description"),
                    "Url": item["url"],
                }
                for schema in item.get("schema", [])
            )
        return pd.DataFrame(rows).sort_values(["Table", "Column"])

    def find_tables(self, tables: Union[str, list]) -> pd.DataFrame:
        """
        Return schema entries matching `tables`.

        Parameters
        ----------
        tables : Union[str, list]
            A table name/regex pattern or a list
            of table names to match.

        Returns
        -------
        pd.DataFrame
            DataFrame of matching schema entries.

        """
        if isinstance(tables, list):
            tables = [table.casefold() for table in tables]
            return self.schemas[self.schemas["Table"].str.casefold().isin(tables)]
        return self.schemas[self.schemas["Table"].str.match(tables, case=False)]

    def find_columns(self, columns: Union[str, list]) -> pd.DataFrame:
        """
        Return schema entries matching `columns`.

        Parameters
        ----------
        columns : Union[str, list]
            A column name/regex pattern or a list
            of column names to match.

        Returns
        -------
        pd.DataFrame
            DataFrame of matching schema entries.

        """
        if isinstance(columns, list):
            columns = [column.casefold() for column in columns]
            return self.schemas[self.schemas["Column"].str.casefold().isin(columns)]
        return self.schemas[self.schemas["Column"].str.match(columns, case=False)]


def _fetch_az_mon_categories() -> requests.models.Response:
    """Return the AzMonitor reference page."""
    return requests.get(SCHEMA_CATS_URL)


def _get_security_category_list(resp: requests.models.Response) -> bs4.element.Tag:
    """Extract the list after the security header."""
    soup = bs4.BeautifulSoup(resp.text, "html.parser")

    result = soup.find("div", class_="content")
    sec_header = result.find("h2", id="security")
    return sec_header.find_next_sibling()


def _build_table_index(security_cat_list: bs4.element.Tag) -> Dict[str, Dict[str, str]]:
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


def _read_table_from_url(table: str, ref: Dict[str, str]) -> pd.DataFrame:
    """Read table schema from a URL."""
    table_data = pd.read_html(ref["url"])[0]
    table_data["Table"] = table
    table_data["Url"] = ref["url"]
    return table_data


def _fetch_table_schemas(sec_url_dict: Dict[str, Dict[str, str]]) -> pd.DataFrame:
    """Combine schema tables into single DF."""
    print(f"Reading Azure monitor schemas for {len(sec_url_dict)} tables...")
    all_tables = [
        _read_table_from_url(table, ref)
        for table, ref in tqdm(sec_url_dict.items(), unit="schemas")
    ]
    return pd.concat(all_tables, ignore_index=True)
