import os
import requests
from typing import Callable

from chessforge.utils.global_constants import LICHESS_DUMP_URL, PATH_DATA_RAW
from chessforge.utils.utils import get_recent_months_string_generator, build_lichess_name


def get_lichess_url(filename: str) -> str:
    return f"{LICHESS_DUMP_URL}/{filename}"


def check_remote_file(filename: str) -> bool:
    url = get_lichess_url(filename)
    try:
        response = requests.head(url, allow_redirects=True, timeout=5)
        return (response.status_code == 200)
    except requests.RequestException:
        return False
    

def find_latest_lichess_dump_month() -> str | None:
    for month in get_recent_months_string_generator():
        file_name = build_lichess_name(month, add_file_extension=True)
        if check_remote_file(file_name):
            return month
    return None


def download_lichess_dump_file(filename: str, log=lambda _: None, on_progress: Callable[[int, int], None] = None) -> bool:   
    """
    TODO
    """

    # Hanlde paths
    local_path = os.path.join(PATH_DATA_RAW, filename)
    tmp_path = os.path.join(PATH_DATA_RAW, "download_" + filename + ".tmp") 

    if os.path.exists(local_path):
        log(f"File {filename} already exists. Aborting.")
        return False

    # Initialize download
    url = get_lichess_url(filename)
    log(f"Downloading from {url} ...")
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        log(f"Failed to download file. Status: {response.status_code}")
        return False

    # Determine file size for progress callback
    total_size = response.headers.get("content-length")
    total_size = int(total_size) if total_size else None

    # Download and write to output file
    with open(tmp_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=1048576): # 1 Mib
            if chunk:
                file.write(chunk)
                if on_progress: on_progress(len(chunk), total_size)

    os.replace(tmp_path, local_path)
    return True
