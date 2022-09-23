# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""Kql extract threading interface with .Net Kqlextract."""
from base64 import b64encode
import json
import os
import queue
import subprocess
import threading
import time
from pathlib import Path
from typing import Optional
from uuid import uuid4

__author__ = "Liam Kirton"


base_path = Path(__file__).parent
CS_PROJ_PATH = base_path.joinpath("../kqlextraction/KqlExtraction/KqlExtraction.csproj")

worker_exit = threading.Event()
worker_queue = queue.Queue()  # type: ignore
worker_results = queue.Queue()  # type: ignore
worker_thread = None

# pylint: disable=broad-except


def _worker_thread_proc():
    try:
        kql_extraction = None

        while not worker_exit.is_set():
            try:
                if kql_extraction is not None and kql_extraction.poll() is not None:
                    kql_extraction = None
                if kql_extraction is None:
                    kql_extraction = subprocess.Popen(
                        [
                            "dotnet",
                            "run",
                            "-c",
                            "Release",
                            "--project",
                            str(CS_PROJ_PATH),
                        ],
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
            except Exception as subp_ex:
                print(f"[!] Exception Starting KqlExtraction Process.\n{subp_ex}")
                break

            try:
                uuid, kql = worker_queue.get(timeout=2.0)
                kql_extraction.stdin.write(
                    bytes(f"{uuid},", encoding="utf-8")
                    + b64encode(bytes(kql, encoding="utf-8"))
                    + b"\n"
                )
                kql_extraction.stdin.flush()

                kql_extraction_result = kql_extraction.stdout.readline()
                worker_results.put(json.loads(kql_extraction_result))
            except queue.Empty:
                pass
            except Exception as thread_ex:
                print(
                    '[!] Unhandled Exception in "while not worker_exit.is_set":',
                    thread_ex,
                )
                kql_extraction.kill()

        if kql_extraction.poll() is None:
            kql_extraction.kill()
    except Exception as thread_out_ex:
        print(
            '[!] Unhandled Exception at "while not worker_exit.is_set()', thread_out_ex
        )


def extract_kql(kql_query: str, query_id: Optional[str] = None):
    """Extract kql_properties from Kql query."""
    kql_id = query_id or str(uuid4())
    worker_queue.put((kql_id, kql_query))

    try:
        kql_result = {}
        while True:
            kql_result = worker_results.get(timeout=5.0)
            if "Id" in kql_result and kql_result["Id"] == kql_id:
                break
    except Exception as extr_err:
        #print("[!] Exception in extract_kql", extr_err)
        pass

    return kql_result


if __name__ == "__main__":
    worker_thread = threading.Thread(target=_worker_thread_proc)
    worker_thread.start()

    test_path = base_path.joinpath("test_data")
    print("using", test_path)
    print(len(list(test_path.glob("*.kql"))), "kql files")
    try:
        for file_no, kql_file in enumerate(test_path.glob("*.kql")):
            # kql_file = os.path.join(base_path, "tests", kql_file)
            print(f"[{file_no}], {kql_file.name}")
            print(f"[{file_no}]\n".join(kql_file.read_text(encoding="utf-8").split("\n")[:5]))
            with open(kql_file, "r", encoding="utf-8") as f:
                kql_text = f.read()

            print(f"[{file_no}]", extract_kql(kql_text, query_id=file_no))

    except Exception as ex:
        print("[!] Unhandled Exception", ex)

    while not worker_queue.empty():
        time.sleep(0.5)

    worker_exit.set()
    worker_thread.join()
