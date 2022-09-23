# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""Github download and conversion functions."""

import logging
import shutil
from itertools import chain
from pathlib import Path
from typing import List, Union

import pandas as pd

from .kql_file_parser import (
    download_git_archive,
    format_repo_url,
    get_sentinel_queries_from_github,
    parse_kql_to_dict,
    parse_markdown_to_dict,
    parse_yaml,
    read_config,
)
from .kql_query import KqlQuery

__author__ = "Ashwin Patil, Jannie Li, Ian Hellen"


_CURR_DIR = Path.cwd()


def get_sentinel_queries(output_path: Path = _CURR_DIR):
    """Return Sentinel queries from repo."""
    # download sentinel github and extract yaml files only
    azsentinel_git_url = "https://github.com/Azure/Azure-Sentinel/archive/master.zip"
    get_sentinel_queries_from_github(git_url=azsentinel_git_url, outputdir=output_path)

    # Parsing yaml files and converting to dataframe
    base_dir = str(output_path.joinpath("Azure-Sentinel-master"))
    detections_df = parse_yaml(parent_dir=base_dir, child_dir="Detections")
    hunting_df = parse_yaml(parent_dir=base_dir, child_dir="Hunting Queries")
    solutions_df = parse_yaml(parent_dir=base_dir, child_dir="Solutions")

    # tmp dirs
    logging.info(
        "Detections: %d Hunting Queries: %d Solutions: %d",
        len(detections_df),
        len(hunting_df),
        len(hunting_df),
    )
    _remove_tmp_folder(output_path.joinpath("Azure-Sentinel-master"))
    # Filtering yamls with no KQL queries
    query_list = _sent_dfs_to_kql_query_list(
        detections_df=detections_df[detections_df["query"].notnull()],
        hunting_df=hunting_df[hunting_df["query"].notnull()],
        solutions_df=solutions_df[solutions_df["query"].notnull()],
    )
    return [KqlQuery(**query) for query in query_list]


def _sent_dfs_to_kql_query_list(detections_df, hunting_df, solutions_df):
    # Selecting specific columns
    columns = [
        "name",
        "GithubURL",
        "query",
        "description",
        "tactics",
        "relevantTechniques",
    ]
    all_dfs = [detections_df[columns], hunting_df[columns], solutions_df[columns]]
    sentinel_github = pd.concat(all_dfs, ignore_index=True, sort=True)

    # renaming to columns to match with schema
    sentinel_github = sentinel_github.rename(
        columns={
            "GithubURL": "source_path",
            "name": "query_name",
            "relevantTechniques": "techniques",
        },
    )

    cols = ["description", "techniques", "tactics"]
    # create new column by merging selected columns into dictionary
    sentinel_github["attributes"] = sentinel_github[cols].to_dict(orient="records")

    # select columns and display sample dataframe records
    select_columns = ["source_path", "query_name", "query", "attributes"]
    sentinel_github[select_columns].head()

    # return it as list of dictionary
    return sentinel_github[select_columns].to_dict(orient="records")


# ### KQL - Community Github Repos


def get_community_queries(output_dir: Path = _CURR_DIR, config: Union[Path, str] = "repos.yaml"):
    """Return KqlQuery list from community repos."""
    # Read yaml config file
    repos = read_config(config)

    # Compile list of github urls to download
    repo_urls: List[str] = []
    tmp_dirs: List[str] = []
    for item in repos:
        url = format_repo_url(item["Github"]["repo"], item["Github"]["branch"])
        repo_urls.append(url)
        tmp_dirs.append(str(output_dir.joinpath(f"{item['Github']['repo']}-{item['Github']['branch']}")))

    # download github urls one by one
    for url in repo_urls:
        download_git_archive(url, output_dir)

    txt_queries = _read_community_txt_queries(repos, output_dir)
    md_queries = _read_community_md_queries(repos, output_dir)
    to_remove = tmp_dirs.copy()
    for tmp_dir in to_remove:
        _remove_tmp_folder(tmp_dir)
        tmp_dirs.remove(tmp_dir)
    return [
        query if isinstance(query, KqlQuery)
        else KqlQuery(**query)
        for query in chain(txt_queries, md_queries)
    ]


def _read_community_txt_queries(repos, src_path):
    """Parse text files."""
    parsed_txt_queries = []

    for item in repos:
        repo_name = item["Github"]["repo"]
        branch_name = item["Github"]["branch"]
        list_of_dict = parse_kql_to_dict(repo_name, branch_name, src_path)
        parsed_txt_queries.extend(list_of_dict)
    # display parsed sample record
    logging.info("Parsed %d queries from text files", len(parsed_txt_queries))
    return parsed_txt_queries


def _read_community_md_queries(repos, src_path):
    """Parses markdown files."""
    parsed_md_queries = []

    for item in repos:
        repo_name = item["Github"]["repo"]
        branch_name = item["Github"]["branch"]
        list_of_dict = parse_markdown_to_dict(repo_name, branch_name, src_path)
        parsed_md_queries.extend(list_of_dict)

    logging.info("Parsed %d queries from text files", len(parsed_md_queries))
    return parsed_md_queries


def _remove_tmp_folder(tmp_dir):
    if Path(tmp_dir).is_dir():
        try:
            shutil.rmtree(tmp_dir)
        except Exception as err:  # pylint: disable=broad-except
            logging.exception(
                "Error trying to remove temporary folder '%s'.",
                tmp_dir,
                exc_info=err
            )
