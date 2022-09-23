from base64 import b64encode
import json
import os
import queue
import subprocess
import threading
import time
from uuid import uuid4


worker_exit = threading.Event()
worker_queue = queue.Queue()
worker_results = queue.Queue()
worker_thread = None


def _worker_thread_proc():
    try:
        kql_extraction = None

        while not worker_exit.is_set():
            try:
                if kql_extraction is not None:
                    if kql_extraction.poll() is not None:
                        kql_extraction = None
                if kql_extraction is None:
                    kql_extraction = subprocess.Popen([
                        'dotnet',
                        'run',
                        '-c',
                        'Release',
                        '--project',
                        os.path.join(os.path.abspath(os.path.split(__file__)[0]), 'KqlExtraction', 'KqlExtraction.csproj')
                    ], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            except Exception as ex:
                print('[!] Exception Starting KqlExtraction Process')
                break

            try:
                uuid, kql = worker_queue.get(timeout=2.0)
                kql_extraction.stdin.write(bytes(f'{uuid},', encoding='utf-8') + b64encode(bytes(kql, encoding='utf-8')) + b'\n')
                kql_extraction.stdin.flush()

                kql_extraction_result = kql_extraction.stdout.readline()
                worker_results.put(json.loads(kql_extraction_result))
            except queue.Empty:
                pass
            except Exception as ex:
                kql_extraction.kill()

        if kql_extraction.poll() is None:
            kql_extraction.kill()
    except Exception as ex:
        print('[!] Unhandled Exception', str(ex))


def extract_kql(kql):
    kql_id = str(uuid4())
    worker_queue.put((kql_id, kql))

    try:
        kql_result = {}
        while True:
            kql_result = worker_results.get(timeout=5.0)
            if 'Id' in kql_result and kql_result['Id'] == kql_id:
                break
    except Exception:
        pass

    return kql_result


if __name__ == '__main__':
    worker_thread = threading.Thread(target=_worker_thread_proc)
    worker_thread.start()

    try:
        base_path = os.path.abspath(os.path.split(__file__)[0])
        for kql_file in os.listdir(os.path.join(base_path, 'tests')):
            kql_file = os.path.join(base_path, 'tests', kql_file)

            with open(kql_file, 'r') as f:
                kql = f.read()

            print(extract_kql(kql))
    except Exception as ex:
        print('[!] Unhandled Exception', str(ex))

    while not worker_queue.empty():
        time.sleep(0.5)

    worker_exit.set()
    worker_thread.join()
