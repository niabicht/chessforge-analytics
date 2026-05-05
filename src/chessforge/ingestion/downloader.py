import os
import requests
from typing import Callable

from chessforge.utils.global_constants import LICHESS_DUMP_URL, PATH_DATA_RAW
from chessforge.utils.utils import generate_past_months, build_lichess_name


def get_lichess_url(filename: str) -> str:
    return f"{LICHESS_DUMP_URL}/{filename}"


def check_remote_file(filename: str) -> bool:
    url = get_lichess_url(filename)
    try:
        response = requests.head(url, allow_redirects=True, timeout=5)
        return response.status_code == 200
    except requests.RequestException:
        return False
    

def find_latest_lichess_dump() -> str | None:
    for month in generate_past_months():
        file_name = build_lichess_name(month, add_file_extension=True)
        if check_remote_file(file_name):
            return month
    return None


def download_file(filename: str, log=lambda _: None, on_progress: Callable[[int, int], None] = None) -> bool:   
    local_path = os.path.join(PATH_DATA_RAW, filename)
    tmp_path = os.path.join(PATH_DATA_RAW, "download_" + filename + ".tmp") 

    if os.path.exists(local_path):
        log(f"File {filename} already exists. Aborting.")
        return False

    url = get_lichess_url(filename)
    log(f"Downloading from {url} ...")

    response = requests.get(url, stream=True)
    if response.status_code != 200:
        log(f"Failed to download file. Status: {response.status_code}")
        return False

    total_size = response.headers.get("content-length")
    total_size = int(total_size) if total_size else None

    with open(tmp_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=1048576): # 1 Mib
            if chunk:
                file.write(chunk)
                if on_progress:
                    on_progress(len(chunk), total_size)

    os.replace(tmp_path, local_path)
    return True
