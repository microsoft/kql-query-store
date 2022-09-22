# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""Test kql extraction integration."""
from .data_store import DataStore
from .kql_extract import extract_kql
from .test_data_store import get_queries_with_kql, get_kqlquery_list

__author__ = "Ian Hellen"

# pylint: disable=redefined-outer-name

def test_extract_from_ds_query(get_queries_with_kql):
    """Function_docstring."""

    queries = get_queries_with_kql
    assert len(queries) > 0
    ds = DataStore(queries)
    assert len(ds.queries) == len(get_queries_with_kql)

    for query in ds.queries:
        result = extract_kql(query.query)
        ds.add_kql_properties(query_id=query.query_id, kql_properties=result)

    assert all(len(query.kql_properties) for query in ds.queries)
    assert len(ds._indexes) >= 6
    assert all(item in ds._indexes for item in ["tactics", "tables", "operators"])
    assert len(ds._indexes["tables"]) >= len(ds.queries)
    assert len(ds._indexes["operators"]) >= len(ds.queries)
