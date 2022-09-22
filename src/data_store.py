# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""DataStore class."""
from hashlib import sha256
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Union
import json
import pandas as pd

from .kql_query import KqlQuery


__author__ = "Ian Hellen"


# interface
# get_query_ids()
# returns a DF of source_path, query_id, query_hash - the idea here is that you
# (or someone) can check for existing queries based on path. I guess I could also
# do that in the store - i.e. don't add a new one if the hash is the same,
# just overwrite with the new details. Hmm. Maybe you don't need to create a query_id.
# I could just do this checking in the data layer comparing source_path and
# source_index with existing values. LMK what you think.
#
# add_queries(queries: List[Dict[as described above]])
# add_kql_properties(query_id, properties: Dict[Liam's dict])
# get_filter_lists() - will return a dictionary of lists of unique values of various properties for the UI filtering
#     I could also return lists of unique query names and paths
# find_queries(**kwargs) - this is going to be an interesting one given that we have a flexible set of properties to search on.
#     kwargs lets us specify a flexible list of conditions, examples:
#         source_path="/some/path - exact string match (prob case insensitive)
#         query_name={matches: regex} - match based on a pandas operator like regex, startswith, contains
#         table=["table1", "table2"] - intersection of queries that use both these tables
#     it will return a DF of query_id + basic properties.
# get_query(query_id) - find_queries will return a list, to get all the props for a query, you'd need to call this.
# get_schema(table)

QueryDict = Dict[str, Union[str, int, Dict[str, Any]]]
QueryList = List[QueryDict]

KqlQueryList = List[KqlQuery]


class DataStore:
    """DataStore class for KqlQuery store."""

    _ATTRIB_INDEXES = {"tactics": list, "techniques": list}
    _KQL_INDEXES = {
        "tables": list,
        "operators": list,
        "fields": list,
        "functions": list,
        "tactics": list,
        "techniques": list,
    }

    _OPERATOR = {
        "startswith": "^{expr}.*",
        "endswith": ".*{expr}$",
        "contains": ".*{expr}.*",
        "matches": "{expr}",
    }

    def __init__(
        self,
        kql_queries: Union[None, KqlQueryList, QueryList] = None,
        json_path: Optional[str] = None,
    ):
        self._json_path = json_path
        if json_path:
            self._data = {
                query.query_id: KqlQuery(**query)
                for query in self._read_json_data(json_path)
            }
        elif kql_queries:
            self._data = {query.query_id: query.asdict() for query in kql_queries}
        # self.attributes = self._extract_attributes()
        self._indexes = {}
        self._create_indexes("attributes")

    @property
    def _data(self):
        """Return internal data."""
        return self._internal_data

    @_data.setter
    def _data(self, value):
        """Set internal data to `value`."""
        self._internal_data = value
        self._data_df = pd.DataFrame(self.queries).set_index("query_id")

    @property
    def queries(self) -> List[KqlQuery]:
        """Get the list of current queries."""
        return [KqlQuery(**query) for query in self._data.values()]

    @property
    def queries_dict(self) -> List[KqlQuery]:
        """Get the list of current queries."""
        return list(self._data.values())

    def to_json(self, file_path: Optional[str] = None) -> Optional[str]:
        """Return the queries as JSON or save to `file_path`, if specified."""
        if file_path:
            Path(file_path).write_text(self.to_json())
        return json.dumps(self.queries_dict)

    def to_df(self) -> pd.DataFrame:
        """Return queries as a pandas DataFrame."""
        return pd.DataFrame(self.queries)

    def get_query_ids(self) -> pd.DataFrame:
        """Return subset of query columns."""
        return self._data_df[["source_path", "query_name" "query_hash"]]

    def add_queries(self, queries: KqlQueryList):
        """Add a list of queries to the store."""
        self._data.update({query.query_id: query.asdict() for query in queries})

    def add_query(self, query: KqlQuery):
        """Add a single query to the store"""
        self._data[query.query_id] = query

    def add_kql_properties(self, query_id: str, kql_properties: Dict[str, Any]):
        self._data[query_id]["kql_properties"] = kql_properties

    def get_filter_lists(
        self, categories: Optional[List[str]] = None
    ) -> Dict[str, List[str]]:
        """Return unique lists of values for each category."""
        return {
            attrib: sorted(self._indexes[attrib].index.unique())
            for attrib in {**self._ATTRIB_INDEXES, **self._KQL_INDEXES}
            if attrib in self._indexes and (categories is None or attrib in categories)
        }

    def find_queries(self, case: bool = False, **kwargs) -> pd.DataFrame:
        """
        Return matching values as a pandas DataFrame.

        Parameters
        ----------
        case : bool, optional
            Use case-sensitive matching, by default False

        Other Parameters
        ----------------
        kwargs :
            You can specify search criteria in the general form attrib_name=expression.
            You can specify multiple criteria - all will be ANDed together.
            attrib=value - exact match (case sensitive for strings)
            attrib={operator: value} - match based on a string operator (matches,
            contains, startswith, endswith)
            attrib=["value1", "value2"] - intersection of items that have
            matches for ALL items in the list.

        Returns
        -------
        pd.DataFrame
            DataFrame of matching queries

        Examples
        --------
        Some examples of expressions:

        - source_path="/some/path" - exact string match (case insensitive)
        - query_name={matches: "AAD.*"} - match based on a  operator like regex, startswith, contains
        - table=["table1", "table2"] - the queries that use both these tables

        """
        criteria = True
        for arg_name, arg_expr in kwargs.items():

            if isinstance(arg_expr, str):
                criteria &= self._data_df[arg_name] == arg_expr
            if isinstance(arg_expr, dict):
                operator, expr = next(iter(arg_expr.items()))
                crit_expr = self._OPERATOR.get(operator)
                if crit_expr:
                    criteria &= self._data_df[arg_name].str.match(
                        crit_expr.format(expr=expr), case=case
                    )
            if isinstance(arg_expr, list) and arg_name in self._indexes:
                query_ids = None
                # we're looking for queries in the indexes that have a matching value
                for match_value in arg_expr:
                    # matched_ids == all query_ids with this property
                    matched_ids = set(
                        self._indexes[arg_name][
                            self._indexes[arg_name].index == match_value
                        ]["query_id"].values
                    )
                    # AND this with query_ids (unless None, then just use this as the
                    # first criterion)
                    query_ids = (
                        matched_ids if query_ids is None else matched_ids & query_ids
                    )
                # Add the matched query IDs to criteria
                criteria &= self._data_df.index.isin(query_ids)
        # return the data subset
        return self._data_df[criteria]

    @staticmethod
    def _read_json_data(json_path: str):
        return json.loads(Path(json_path).read_text(encoding="utf-8"))

    def _create_indexes(self, sub_key: str):
        """Create indexes for child items in queries."""
        # create DF with attributes expanded to columns

        exp_df = self._data_df[[sub_key]].apply(
            lambda x: pd.Series(x[sub_key]), result_type="expand", axis=1
        )
        for key, data_type in self._ATTRIB_INDEXES.items():
            if data_type == list:
                self._indexes[key] = self._create_list_index(
                    data=exp_df,
                    key_col=key,
                )
            if data_type == dict:
                self._indexes[key] = self._create_dict_index(
                    data=exp_df,
                    key_col=key,
                )

    @staticmethod
    def _create_list_index(data, key_col):
        return data[[key_col]].explode(key_col).reset_index().set_index([key_col])

    @staticmethod
    def _extract_dict_keys(row, col_name):
        if isinstance(row[col_name], dict):
            return {
                col_name: [
                    inner_val
                    for val in row[col_name].values()
                    for inner_val in val.keys()
                    if isinstance(val, dict)
                ]
            }
        return row

    def _create_dict_index(self, data, key_col):
        df_dict_keys = data[[key_col]].apply(
            lambda x: self._extract_dict_keys(x, key_col), result_type="expand", axis=1
        )
        return self._create_list_index(df_dict_keys, key_col)
