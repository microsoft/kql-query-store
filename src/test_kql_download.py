# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""Test Query downloader."""
import random
from pathlib import Path

from .data_store import DataStore
from .kql_download import get_community_queries, get_sentinel_queries

__author__ = "Ian Hellen"

# pylint: disable=protected-access


def test_get_sentinel_queries(tmp_path):
    """Test downloading sentinel queries."""
    queries = get_sentinel_queries(tmp_path)
    ds = DataStore(queries)
    assert ds is not None
    assert len(ds.queries) > 2000
    assert len(ds._indexes["tactics"]) > 1000
    assert len(ds._indexes["techniques"]) > 1000

    indexes = [random.randint(0, len(ds.queries)) for _ in range(10)]
    for attrib in ["source_path", "query", "query_id", "attributes"]:
        for idx in indexes:
            assert hasattr(ds.queries[idx], attrib)


def test_get_community_queries(tmp_path):
    """Test downloading sentinel queries."""
    conf_path = Path(__file__).parent.joinpath("repos.yaml")
    queries = get_community_queries(tmp_path, config=conf_path)
    ds = DataStore(queries)
    assert ds is not None
    assert len(ds.queries) > 100

    indexes = [random.randint(0, len(ds.queries)) for _ in range(10)]
    for attrib in ["source_path", "query", "query_id"]:
        for idx in indexes:
            assert hasattr(ds.queries[idx], attrib)
