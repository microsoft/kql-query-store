# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""KqlQuery data class."""
import hashlib
import json
import uuid
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Literal, Optional

import pandas as pd

__author__ = "Ian Hellen"


_SOURCE_TYPES = ["text", "markdown", "sentinel_yaml", "api", "other"]

SourceType = Literal["text", "markdown", "sentinel_yaml", "api", "other"]


def _uuid_str():
    return str(uuid.uuid4())


@dataclass
class KqlQuery:
    """
    Data format for KqlQuery record.

    Attributes
    ----------
    source_path : str
        The path to the original file or API identifier.
    query : str
        The raw query string
    source_type : SourceType, optional
        String - the source file/data type. Valid types are:
        text, markdown, sentinel_yaml, api, other
    source_index : int, optional
        The index (0-based) if the query is one of several in the
        file pointed to by source_path. The default is 0.
    query_name : Optional[str]
        The name of the query. If None this will be derived from
        the last element of source_path
    attributes: Dict[str, Any], optional
        Dictionary of any metadata attributes read from the source
        file.
    kql_properties: Dict[str, Any], optional
        Dictionary of properties derived from the KQL query
    query_id: Optional[str], optional
        UUID used to identify the query
    query_hash: int, optional
        Hash of the query text
    query_version: int, optional
        Query version, not currently used. Default is 0

    Examples
    --------
    Create a KqlQuery instance
    >>>> kql = KqlQuery(
    ...     source_path="https://github.com/a/b/file.kql",
    ...     query="SecurityAlert | take 1"
    ... )

    Create a KqlQuery instance from a dict
    >>>> attribs = {
    ...     "source_path": "https://github.com/a/b/file.kql",
    ...     "query": "SecurityAlert | take 1",
    ... }
    ... kql = KqlQuery(**attribs)

    Different default representation
    >>>> kql
    KqlQuery(source_path='https://github.com/a/b/file.kql', query='SecurityAlert... query_version=0)

    As a dict
    >>>> print(kql.asdict())
    {'source_path': 'https://github.com/a/b/file.kql', 'query': 'SecurityAlert... 'query_version': 0}

    As JSON
    print(kql.to_json())
    {"source_path": "https://github.com/a/b/file.kql", "query": "SecurityAlert... "query_version": 0}

    Class method to convert a list of KqlQuery instances to a list of dicts
    >>>> KqlQuery.kql_list_to_pylist([kql, kql])

    Class method to convert a list of KqlQuery instances to JSON
    >>>> KqlQuery.kql_list_to_json([kql, kql])
    '[{"source_path": "https://github.com/a/b/file.kql", "query": "SecurityAlert... "query_version": 0}]'

    Class method to convert list of KqlQuery instances to a DataFrame
    """

    source_path: str
    query: str
    source_type: SourceType = "text"
    source_index: int = 0
    query_name: Optional[str] = None
    attributes: Dict[str, Any] = field(default_factory=dict)
    kql_properties: Dict[str, Any] = field(default_factory=dict)
    query_id: str = field(default_factory=_uuid_str)
    query_hash: int = 0
    query_version: int = 0

    def __post_init__(self):
        if self.source_path is not None:
            self.query_name = self.source_path.rsplit("/", maxsplit=1)[-1]
        if self.query:
            self.query_hash = hashlib.sha256(
                bytes(self.query, encoding="utf-8"),
                # usedforsecurity=False
            ).hexdigest()

    def asdict(self):
        """Return a dictionary of attributes."""
        return asdict(self)

    def to_json(self):
        """Return JSON representation of attributes."""
        return json.dumps(self.asdict())

    # helper methods and properties
    @property
    def source_types(self):
        """Return list of acceptable source_types."""
        del self
        return _SOURCE_TYPES

    @staticmethod
    def kql_list_to_pylist(kql_queries: List["KqlQuery"]):
        """Return a list of Python dicts from a list of KqlQuery instances."""
        return [
            kql.asdict() for kql in kql_queries
        ]

    @classmethod
    def kql_list_to_json(cls, kql_queries: List["KqlQuery"]):
        """Return JSON from a list of KqlQuery instances."""
        return json.dumps(cls.kql_list_to_pylist(kql_queries))

    @classmethod
    def kql_list_to_df(cls, kql_queries: List["KqlQuery"]):
        """Return a pandas DataFrame from a list of KqlQuery instances."""
        return pd.DataFrame(cls.kql_list_to_pylist(kql_queries))
