# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""Test kql extraction integration."""
from pathlib import Path
from .data_store import DataStore
from .kql_extract import extract_kql
from .kql_query import KqlQuery

from .test_data_store import get_random_query

import pytest

__author__ = "Ian Hellen"

# pylint: disable=redefined-outer-name, protected-access


_TEST_KQL = Path(__file__).parent.joinpath("test_data")

@pytest.fixture
def get_queries_with_kql():
    queries = []
    for file in Path(_TEST_KQL).glob("*.kql"):

        query_text = file.read_text(encoding="utf-8")
        for query in [KqlQuery(**get_random_query(i)) for i in range(2)]:
            query.query = query_text
            queries.append(query)
    return queries


def test_extract_from_ds_query(get_queries_with_kql):
    """Function_docstring."""

    queries = get_queries_with_kql
    assert len(queries) > 0
    ds = DataStore(queries)
    assert len(ds.queries) == len(get_queries_with_kql)

    for query in ds.queries:
        result = extract_kql(query.query, query_id=query.query_id)
        ds.add_kql_properties(query_id=query.query_id, kql_properties=result)

    print([len(query.kql_properties) for query in ds.queries])
    assert all(len(query.kql_properties) for query in ds.queries)
    assert len(ds._indexes) >= 6
    assert all(item in ds._indexes for item in ["tactics", "tables", "operators"])
    assert len(ds._indexes["tables"]) >= len(ds.queries)
    assert len(ds._indexes["operators"]) >= len(ds.queries)
