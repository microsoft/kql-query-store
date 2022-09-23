# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""Main script to fetch KQL queries and create JSON Database."""
import argparse
import logging
from pathlib import Path
from typing import Any, Dict

import sys
sys.path.append(str(Path(__file__).parent))

from tqdm.auto import tqdm

# from .kql_query import KqlQuery
from .kql_download import get_sentinel_queries, get_community_queries
from .data_store import DataStore
from .kql_extract import extract_kql

# ######### MOCK Stuff for stubbing code
# from unittest.mock import MagicMock
# # from .kql_ingest import fetch_queries
# fetch_queries = MagicMock()
# _MOCK_QUERY = "SecurityAlert | take 1"
# _MOCK_RESULTS = [KqlQuery(source_path=f"/x/y/{src}.kql", query=_MOCK_QUERY) for src in range(3)]
# fetch_queries.return_value = _MOCK_RESULTS
# # # from .kql_db_store import DataStore
# DataStore = MagicMock()
# store_instance = MagicMock()
# DataStore.return_value = store_instance
# store_instance.queries = _MOCK_RESULTS

# _MOCK_KQL_PARSE = {"FunctionCalls":["count","tostring","make_list","toreal"],"Joins":["rightsemi","leftouter"],"Operators":["where","extend","summarize","mv-expand","project-away","project"],"Tables":["SigninLogs"]}
# parse_kql = MagicMock()
# parse_kql.return_value = _MOCK_KQL_PARSE
########## End Mocks


__author__ = "Ian Hellen"


def _add_script_args():
    parser = argparse.ArgumentParser(description="Kql Query download and build script.")
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
    results = []
    # fetch and parse queries
    logging.info(
        "Fetching queries"
    )
    try:
        results.extend(get_sentinel_queries())
    except Exception as err:  # pylint: disable=broad-except
        logging.exception(
            "Failed to fetch Sentinel queries.",
            exc_info=err,
        )
    try:
        results.extend(get_community_queries(config=args.conf))
    except Exception as err:  # pylint: disable=broad-except
        logging.exception(
            "Failed to fetch community queries.",
            exc_info=err,
        )

    # add queries to store
    logging.info(
        "Adding %d queries to store.", len(results)
    )

    try:
        store = DataStore(results)
    except Exception as err:  # pylint: disable=broad-except
        logging.exception(
            "Failed to queries to store.",
            exc_info=err,
        )

    # parse Kql for query properties
    logging.info(
        "Getting KQL properties for %d kql queries.", len(results)
    )
    for query in tqdm(store.queries):
        try:
            kql_properties = extract_kql(kqlquery=query.query, query_id=query.query_id)
        except Exception as err:  # pylint: disable=broad-except
            logging.exception(
                "Failed to parse query '%s'.\n %s",
                query.query_id,
                query.source_path,
                exc_info=err
            )
            continue
        try:
            store.add_kql_properties(query.query_id, kql_properties)
        except Exception as err:  # pylint: disable=broad-except
            logging.exception(
                "Failed to update kql properties for query '%s'.",
                query.query_id,
                exc_info=err,
            )

    # get table schema
    # write JSON and DF
    store.to_json(args.out)
    if args.df:
        query_df = store.to_df()
        query_df.to_pickle(args.df)


def _configure_logging(verbose: bool = False):
    logging_args: Dict[str, Any] = {"format": "%(asctime)s: %(funcName)s #%(lineno)d %(filename)s %(message)s"}
    if verbose:
        logging_args["level"] = logging.DEBUG
    logging.basicConfig(**logging_args)


# pylint: disable=invalid-name
if __name__ == "__main__":

    arg_parser = _add_script_args()
    args = arg_parser.parse_args()

    _configure_logging(args.verbose)
    main(args)
