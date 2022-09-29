# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""Query download and parsing functions."""

import glob
import io
import logging
import urllib.parse
import warnings
import zipfile
from pathlib import Path
from typing import List

import pandas as pd
import requests
import yaml
from pandas import json_normalize
from requests.exceptions import HTTPError
from tqdm.auto import tqdm

from .kql_query import KqlQuery

__author__ = "Ashwin Patil, Jannie Li"


def read_config(filename):
    with open(filename, "r", encoding="utf-8") as yamlfile:
        data = yaml.safe_load(yamlfile)
    return data


def format_repo_url(repo_name, branch_name):
    return f"https://github.com/{repo_name}/archive/{branch_name}.zip"


def download_git_archive(git_url, output_dir):
    logging.info("Downloading %s, may take few mins..", git_url)
    try:
        r = requests.get(git_url)
        repo_zip = io.BytesIO(r.content)
        logging.info("Extracting files..")
        with zipfile.ZipFile(repo_zip, mode="r") as archive:
            for file in tqdm(archive.namelist()):
                archive.extract(file, path=output_dir)
        logging.info("Downloaded and Extracted Files successfully")
    except HTTPError as http_err:
        warnings.warn(f"HTTP error occurred trying to download from Github: {http_err}")


def get_sentinel_queries_from_github(git_url, outputdir):
    logging.info("Downloading from Azure Sentinel Github, may take 2-3 mins..")
    try:
        r = requests.get(git_url)
        repo_zip = io.BytesIO(r.content)

        with zipfile.ZipFile(repo_zip, mode="r") as archive:
            # Only extract Detections and Hunting Queries Folder
            logging.info("Extracting files..")
            for file in tqdm(archive.namelist()):
                if file.startswith(
                    (
                        "Azure-Sentinel-master/Detections/",
                        "Azure-Sentinel-master/Hunting Queries/",
                        "Azure-Sentinel-master/Solutions/",
                    )
                ) and file.endswith(".yaml"):
                    archive.extract(file, path=outputdir)
        logging.info("Downloaded and Extracted Files successfully")
    except HTTPError as http_err:
        warnings.warn(f"HTTP error occurred trying to download from Github: {http_err}")


def parse_yaml(parent_dir, child_dir):
    sentinel_repourl = "https://github.com/Azure/Azure-Sentinel/blob/master"
    bad_yamls = [
        (
            "/home/jovyan/work/Hackathon/kql-query-store/dev-notebooks/"
            "Azure-Sentinel-master/Hunting Queries/Microsoft 365 Defender"
            "/Device Inventory/Find Software By Name and Version.yaml"
        )
    ]
    # Collect list of files recursively under a folder
    yaml_queries = glob.glob(f"{parent_dir}/{child_dir}/**/*.yaml", recursive=True)
    yaml_queries = [query for query in yaml_queries if query not in bad_yamls]

    frames: List[pd.DataFrame] = []

    # Recursively load yaml Files and append to dataframe
    logging.info("Parsing yaml queries..")
    for query in tqdm(yaml_queries):
        with open(query, "r", encoding="utf-8", errors="ignore") as file_stream:
            try:
                parsed_yaml_df = json_normalize(yaml.safe_load(file_stream))
            except Exception as err:  # pylint: disable=broad-except
                logging.exception(
                    "Exception parsing yaml_query %s", query, exc_info=err
                )
                continue
            parsed_yaml_df["GithubURL"] = urllib.parse.quote(
                query.replace(parent_dir, sentinel_repourl), safe=":/"
            )
            # #URL encode
            # parsed_yaml_df["GithubURL"] = urllib.parse.quote(parsed_yaml_df["GithubURL"], safe=':/')
            # parsed_yaml_df = parsed_yaml_df[columns]
            frames.append(parsed_yaml_df)

    return pd.concat(frames, ignore_index=True, sort=True)


def parse_kql_to_dict(repo_name, branch_name, src_path):
    parent_dir = Path(src_path).joinpath(f"{repo_name.split('/')[-1]}-{branch_name}")
    kql_files = glob.glob(f"{parent_dir}/**/*.kql", recursive=True)

    git_repo_url = f"https://github.com/{repo_name}/tree/main"

    list_of_kql_files_dict = []
    logging.info("Parsing queries..")
    for file in tqdm(kql_files):
        with open(file, "r", encoding="utf-8", errors="ignore") as f:
            kql_query = KqlQuery(
                query=f.read(),
                source_path=urllib.parse.quote(
                    file.replace(str(parent_dir), git_repo_url), safe=":/"
                ),
                query_name=Path(file).stem,
                source_type="text",
                attributes={},
            )
            list_of_kql_files_dict.append(kql_query)

    return list_of_kql_files_dict


def parse_markdown_to_dict(repo_name, branch_name, src_path):
    parent_dir = Path(src_path).joinpath(f"{repo_name.split('/')[-1]}-{branch_name}")
    md_files = glob.glob(f"{parent_dir}/**/*.md", recursive=True)
    logging.info(
        "Processing %d markdown files from repo: %s",
        len(md_files),
        repo_name,
    )
    git_repo_url = f"https://github.com/{repo_name}/tree/main"

    # src_path_list = []
    logging.info("Parsing markdown files..")
    kql_query_list: List[KqlQuery] = []
    for file in tqdm(md_files):
        file_path = Path(file)
        lines = file_path.read_text(encoding="utf-8").split("\n")

        in_kql = False
        kql_text = []
        last_header = None
        context = []
        qry_index = 0
        for line in lines:
            if line.startswith("```kql"):
                in_kql = True
                continue
            if line.strip() == "```":
                kql_query_list.append(
                    KqlQuery(
                        query="\n".join(kql_text),
                        source_path=urllib.parse.quote(
                            str(file_path).replace(str(parent_dir), git_repo_url),
                            safe=":/",
                        ),
                        source_type="markdown",
                        source_index=qry_index,
                        query_name=last_header or f"{file_path.stem}_{qry_index}",
                        context="\n".join(context[-10:]),
                    )
                )
                qry_index += 1
                in_kql = False
                kql_text = []
                last_header = None
                context = []
                continue
            if not in_kql and line.startswith("#"):
                last_header = line
            if in_kql:
                kql_text.append(line)
            else:
                context.append(line)

        # ct = 0
        # kql = False
        # kql_collect = []
        # title_collect = []
        # cur_kql = []
        # title = "n/a"
        # while ct < len(lines):
        #     if kql:
        #         cur_kql.append(lines[ct])
        #     if lines[ct].startswith("#") and lines[ct + 2] == "```kql":
        #         kql = True
        #         title = lines[ct]
        #     elif lines[ct] == "```kql":
        #         kql = True
        #     elif lines[ct] == "```":
        #         kql = False
        #         cur_kql = "\n".join(cur_kql)
        #         kql_collect.append(cur_kql)
        #         title_collect.append(title)
        #         title = "n/a"
        #         cur_kql = []
        #     ct += 1
        #     src_path = urllib.parse.quote(
        #         str(file_path).replace(str(parent_dir), git_repo_url), safe=":/"
        #     )
        #     src_path_list.append(src_path)

        #     kql_query = KqlQuery(
        #         query_name=title_collect,
        #         query=kql_collect,
        #         source_path=src_path_list,
        #     )
        #     df = pd.concat([df, test_df])

    return kql_query_list
