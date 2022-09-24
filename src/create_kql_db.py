# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""Main script to fetch KQL queries and create JSON Database."""

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import sys

sys.path.append(str(Path(__file__).parent))

from tqdm.auto import tqdm

# from .kql_query import KqlQuery
from .kql_download import get_sentinel_queries, get_community_queries
from .data_store import DataStore
from . import kql_extract as extract
from .az_mon_schema import AzMonitorSchemas

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

_OUTPUT_FILE = "kql_query_db"


def _add_script_args():
    parser = argparse.ArgumentParser(description="Kql Query download and build script.")
    parser.add_argument(
        "--conf", "-c", required=True, help="Path to query source config file."
    )
    parser.add_argument(
        "--out",
        "-o",
        default="output",
        help="Path to output folder.",
    )
    parser.add_argument(
        "--df",
        "-d",
        action="store_true",
        default=False,
        help="Write a pickled dataframe.",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        default=False,
        help="Show less output of the execution.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        default=False,
        help="Show debug logging of execution.",
    )
    parser.add_argument(
        "--timestamp",
        "-t",
        action="store_true",
        default=False,
        help="Add UTC timestamps to output file.",
    )
    parser.add_argument(
        "--save-stages",
        "-s",
        action="store_true",
        default=False,
        help="Save outputs after initial query load/parsing.",
    )
    parser.add_argument(
        "--az-schemas",
        "-a",
        action="store_true",
        default=False,
        help="Download and store Azure monitor schema.",
    )
    return parser


def main(args):
    """Main entrypoint for fetching queries and writing to store."""
    results = []
    if not Path(args.out).is_dir():
        if Path(args.out).exists():
            logging.error("Cannot find or create output folder %s", args.out)
            return
        Path.mkdir(args.out, parents=True, exist_ok=True)

    # fetch and parse queries
    logging.info("Fetching queries")
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
    logging.info("Adding %d queries to store.", len(results))

    try:
        store = DataStore(results)
    except Exception as err:  # pylint: disable=broad-except
        logging.exception(
            "Failed to queries to store.",
            exc_info=err,
        )

    if args.save_stages:
        store.to_json(_get_output_file(args, file_type="p1.json"))

    # parse Kql for query properties
    logging.info("Getting KQL properties for %d kql queries.", len(results))
    try:
        extract.start()
        for query in tqdm(store.queries):
            try:
                kql_properties = extract.extract_kql(
                    kql_query=query.query, query_id=query.query_id
                )
            except Exception as err:  # pylint: disable=broad-except
                logging.exception(
                    "Failed to parse query '%s'.\n %s",
                    query.query_id,
                    query.source_path,
                    exc_info=err,
                )
                continue
            try:
                if not kql_properties.get("Valid_Query", True):
                    logging.error(
                        "Invalid KQL for query %s (%s)",
                        query.query_id,
                        query.source_path,
                    )
                store.add_kql_properties(
                    query_id=query.query_id, kql_properties=kql_properties
                )
            except Exception as err:  # pylint: disable=broad-except
                logging.exception(
                    "Failed to update kql properties for query '%s'.",
                    query.query_id,
                    exc_info=err,
                )
    finally:
        extract.stop()
    logging.info("Finished getting KQL properties for %d kql queries.", len(results))

    if args.az_schemas:
        logging.info("Getting Azure Monitor schema data.")
        az_schemas = AzMonitorSchemas()
        az_schemas.get_az_mon_schemas()
        schema_json = Path(args.out).joinpath("az_mon_schemas.json")
        schema_df = Path(args.out).joinpath("az_mon_schemas.json")
        schema_json.write_text(az_schemas.to_json(), encoding="utf-8")
        az_schemas.schemas.to_pickle(schema_df)
        logging.info(
            "Saved schema data to %s and %s.", str(schema_json), str(schema_df)
        )

    # get table schema
    # write JSON and DF
    out_json_path = _get_output_file(args, "json")
    store.to_json(out_json_path)
    logging.info("Writing JSON output to %s", out_json_path)
    if args.df:
        query_df = store.to_df()
        out_df_path = _get_output_file(args, "pkl")
        query_df.to_pickle(out_df_path)
        logging.info("Writing Pickled dataframe output to %s", out_df_path)
    logging.info("Job completed")
    logging.info("============================================")


def _get_output_file(args, file_type):
    """Return formatted path for output files."""
    if args.timestamp:
        time_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H-%M-%S")
        return Path(args.out).joinpath(f"{_OUTPUT_FILE}-{time_stamp}.{file_type}")
    return Path(args.out).joinpath(f"{_OUTPUT_FILE}.{file_type}")


def _configure_logging(args):
    logging_args: Dict[str, Any] = {
        "format": "%(asctime)s: %(funcName)s #%(lineno)d %(filename)s %(message)s"
    }
    if args.quiet:
        logging_args["level"] = logging.WARNING
    elif args.verbose:
        logging_args["level"] = logging.DEBUG
    else:
        logging_args["level"] = logging.INFO
    logging.basicConfig(**logging_args)


# pylint: disable=invalid-name
if __name__ == "__main__":

    arg_parser = _add_script_args()
    args = arg_parser.parse_args()

    _configure_logging(args)
    main(args)
