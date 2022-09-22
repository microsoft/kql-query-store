# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""Main script to fetch KQL queries and create JSON Database."""
import argparse
import logging
from pathlib import Path

import sys
sys.path.append(".")
from kql_query import KqlQuery
# from .kql_ingest import fetch_queries
# from .kql_db_store import DataStore
# from .kql_parser import parse_kql

########## MOCK Stuff for stubbing code
from unittest.mock import MagicMock
# from .kql_ingest import fetch_queries
fetch_queries = MagicMock()
_MOCK_QUERY = "SecurityAlert | take 1"
_MOCK_RESULTS = [KqlQuery(source_path=f"/x/y/{src}.kql", query=_MOCK_QUERY) for src in range(3)]
fetch_queries.return_value = _MOCK_RESULTS
# # from .kql_db_store import DataStore
DataStore = MagicMock()
store_instance = MagicMock()
DataStore.return_value = store_instance
store_instance.queries = _MOCK_RESULTS

_MOCK_KQL_PARSE = {"FunctionCalls":["count","tostring","make_list","toreal"],"Joins":["rightsemi","leftouter"],"Operators":["where","extend","summarize","mv-expand","project-away","project"],"Tables":["SigninLogs"]}
parse_kql = MagicMock()
parse_kql.return_value = _MOCK_KQL_PARSE
########## End Mocks


__author__ = "Ian Hellen"


def _add_script_args():
    parser = argparse.ArgumentParser(description="Module static call tree analyer.")
    parser.add_argument(
        "--conf", "-c", required=True, help="Path to query source config file."
    )
    parser.add_argument(
        "--out",
        "-o",
        default="kql_query_db.json",
        help="Path to output file.",
    )
    parser.add_argument(
        "--df",
        "-d",
        action="store_const",
        const="kql_query_df.pkl",
        help="Path to save output as pickled pandas file. (default is kql_query_df.pkl",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        default=False,
        help="Show verbose output of execution.",
    )
    return parser


def main(args):
    """Main entrypoint for fetching queries and writing to store."""
    conf_text = Path(args.conf).read_text(encoding="utf-8")
    results = []
    # fetch and parse queries
    query_srcs = [
        query_src for query_src in conf_text.split("\n")
        if not query_src.strip() or query_src.strip().startswith("#")
    ]
    logging.info(
        "Fetching queries from %d locations", len(query_srcs)
    )
    for query_src in query_srcs:
        try:
            results.extend(fetch_queries(query_src))
        except Exception as err:
            logging.exception(
                "Failed to fetch results from %s. Exception: %s",
                query_src,
                err,
            )

    # add queries to store
    logging.info(
        "Adding %d queries to store.", len(results)
    )
    store = DataStore()
    try:
        store.add_queries(results)
    except Exception as err:
        logging.exception(
            "Failed to queries to store. Exception: %s",
            err,
        )

    # parse Kql for query properties
    logging.info(
        "Parsing %d queries.", len(results)
    )
    for query in store.queries:
        kql_properties = parse_kql(query)
        query.kql_properties = kql_properties
        try:
            store.update(query)
            raise ValueError("test")
        except Exception as err:
            logging.exception(
                "Failed to parse query '%s'.",
                query.query_id,
            )

    # get table schema
    # write JSON and DF
    store.to_json(args.out)
    if args.df:
        store.to_df(args.df)


def _configure_logging(args):
    logging_args = {"format": "%(asctime)s: %(funcName)s #%(lineno)d %(filename)s %(message)s"}
    if args.verbose:
        logging_args["level"] = logging.DEBUG
    logging.basicConfig(**logging_args)


# pylint: disable=invalid-name
if __name__ == "__main__":
    arg_parser = _add_script_args()
    args = arg_parser.parse_args()

    _configure_logging(args)
    main(args)
