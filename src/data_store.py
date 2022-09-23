# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""DataStore class."""
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union
import json
import numpy as np
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

    _ATTRIB_INDEXES: Dict[str, type] = {"tactics": list, "techniques": list}
    _KQL_INDEXES: Dict[str, type] = {
        "tables": list,
        "operators": list,
        "fields": list,
        "functioncalls": list,
        "joins": dict,
    }
    _ALL_INDEXES: Dict[str, type] = {**_ATTRIB_INDEXES, **_KQL_INDEXES}

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
                query.get("query_id"): KqlQuery(**query)
                for query in self._read_json_data(json_path)
            }
        elif kql_queries:
            if isinstance(kql_queries[0], KqlQuery):
                self._data = {query.query_id: query for query in kql_queries}
            else:
                self._data = {
                    query["query_id"]: KqlQuery(**query) for query in kql_queries
                }
        else:
            self._data = {}
        # self.attributes = self._extract_attributes()
        if self._data:
            self._data_df = pd.DataFrame(self.queries).set_index("query_id")
        else:
            self._data_df = pd.DataFrame(
                self.queries, columns=KqlQuery.field_names()
            ).set_index("query_id")
        self._indexes: Dict[str, pd.DataFrame] = {}
        self._create_indexes("attributes")
        self._create_indexes("kql_properties")

    @property
    def queries(self) -> List[KqlQuery]:
        """Get the list of current queries."""
        return list(self._data.values())

    @property
    def queries_dict(self) -> List[KqlQuery]:
        """Get the list of current queries."""
        return [query.asdict() for query in self._data.values()]

    def to_json(self, file_path: Optional[str] = None) -> Optional[str]:
        """Return the queries as JSON or save to `file_path`, if specified."""
        if file_path is not None:
            Path(file_path).write_text(self.to_json(), encoding="utf-8")
        return json.dumps(self.queries_dict)

    def to_df(self) -> pd.DataFrame:
        """Return queries as a pandas DataFrame."""
        return pd.DataFrame(self.queries)

    def get_query_ids(self) -> pd.DataFrame:
        """Return subset of query columns."""
        columns = ["source_path", "query_name", "query_hash"]
        if self._data_df is None:
            return pd.DataFrame(columns=columns)
        return self._data_df[columns]

    def add_queries(self, queries: KqlQueryList):
        """Add a list of queries to the store."""
        self._data.update({query.query_id: query for query in queries})
        self._create_indexes("attributes")
        self._create_indexes("kql_properties")
        self._data_df = pd.DataFrame(self.queries).set_index("query_id")

    def add_query(self, query: KqlQuery):
        """Add a single query to the store."""
        self._data[query.query_id] = query
        self._add_item_to_indexes(query)
        self._data_df = pd.concat(
            [self._data_df, pd.DataFrame(query).set_index("query_id")]
        )

    def add_kql_properties(self, query_id: str, kql_properties: Dict[str, Any]):
        """Add Kql properties to a query."""
        kql_props = {key.casefold(): value for key, value in kql_properties.items()}
        self._data[query_id].kql_properties = kql_props
        # update indexes
        self._add_item_to_indexes(self._data[query_id])

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

        ds.find_queries(query_name={"contains": "AAD"}, tables=["table1", "table2"], operations=[...])
        """
        if self._data_df is None:
            return pd.DataFrame()
        criteria = True
        debug = kwargs.pop("debug", False)
        valid_fields = KqlQuery.field_names() + list(self._indexes.keys())

        for arg_name, arg_expr in kwargs.items():
            if arg_name not in valid_fields:
                raise ValueError(
                    f"Unknown attribute name {arg_name}",
                    f"Search expression: {arg_expr}."
                )
            if isinstance(arg_expr, str):
                criteria &= self._data_df[arg_name] == arg_expr
            if isinstance(arg_expr, dict):
                operator, expr = next(iter(arg_expr.items()))
                crit_expr = self._OPERATOR.get(operator)
                if crit_expr:
                    criteria &= self._data_df[arg_name].str.match(
                        crit_expr.format(expr=expr), case=case
                    )
                    if debug:
                        print(arg_expr, criteria.value_counts())
            if isinstance(arg_expr, list) and arg_name in self._indexes:
                query_ids: Optional[Set] = None
                # we're looking for queries in the indexes that have a matching value
                for match_value in arg_expr:
                    # matched_ids == all query_ids with this property
                    matched_ids = set(
                        self._indexes[arg_name][
                            self._indexes[arg_name].index == match_value
                        ]["query_id"].values
                    )
                    if debug:
                        print(len(matched_ids))
                    # AND this with query_ids (unless None, then just use this as the
                    # first criterion)
                    query_ids = (
                        matched_ids if query_ids is None else matched_ids | query_ids
                    )

                # Add the matched query IDs to criteria
                criteria &= self._data_df.index.isin(query_ids)
                if debug:
                    print(arg_expr, criteria.value_counts())
        # return the data subset
        if debug:
            print("final criteria:", criteria.value_counts())
        return self._data_df[criteria]

    @staticmethod
    def _read_json_data(json_path: str):
        return json.loads(Path(json_path).read_text(encoding="utf-8"))

    def _create_indexes(self, sub_key: str):
        """Create indexes for child items in queries."""
        # create DF with attributes expanded to columns
        if self._data_df is None:
            return
        exp_df = (
            # avoid rows with null or empty dictionaries
            self._data_df[
                ~((self._data_df[sub_key] == {}) | (self._data_df[sub_key].isna()))
            ][[sub_key]].apply(
                lambda x: pd.Series(x[sub_key]), result_type="expand", axis=1
            )
        )
        for key, data_type in self._ALL_INDEXES.items():
            if key not in exp_df.columns:
                continue
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

    def _add_item_to_indexes(self, query: KqlQuery):
        """Add attributes and kql_properties to indexes."""
        index_attribs = {**(query.attributes), **(query.kql_properties)}
        for key in self._ALL_INDEXES:
            if key not in index_attribs:
                continue
            df_index = (
                list(index_attribs[key])
                if isinstance(index_attribs[key], (list, dict))
                else None
            )
            if df_index:
                current_index = self._indexes.get(key)
                new_index_items = pd.DataFrame(
                    data=[{"query_id": query.query_id} for _ in df_index],
                    index=df_index,
                )
                if current_index is None:
                    self._indexes[key] = new_index_items
                else:
                    self._indexes[key] = pd.concat(
                        [self._indexes[key], new_index_items]
                    )

    @staticmethod
    def _create_list_index(data, key_col):
        return data[[key_col]].explode(key_col).dropna().reset_index().set_index([key_col])

    @staticmethod
    def _extract_dict_keys(row, col_name):
        if isinstance(row[col_name], dict):
            return {
                col_name: [
                    inner_val
                    for val in row[col_name].values()
                    for inner_val in val
                    if isinstance(val, dict)
                    and inner_val != np.nan
                ]
            }
        return row

    def _create_dict_index(self, data, key_col):
        df_dict_keys = data[[key_col]].apply(
            lambda x: self._extract_dict_keys(x, key_col), result_type="expand", axis=1
        )
        return self._create_list_index(df_dict_keys, key_col)
