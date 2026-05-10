import os
import requests

from chessforge.utils.global_constants import URL_LICHESS_DUMP, PATH_DATA_RAW_DIR
from chessforge.utils.utils import get_recent_months_string_generator, build_lichess_name, get_download_tmp_file_name


def get_lichess_url(file_name: str) -> str:
    return f"{URL_LICHESS_DUMP}/{file_name}"


def check_remote_file(file_name: str) -> bool:
    url = get_lichess_url(file_name)
    try:
        response = requests.head(url, allow_redirects=True, timeout=5)
        return (response.status_code == 200)
    except requests.RequestException:
        return False
    

def find_latest_lichess_dump_month(n_months: int) -> str | None:
    for month in get_recent_months_string_generator(n_months):
        file_name = build_lichess_name(month, add_file_extension=True)
        if check_remote_file(file_name):
            return month
    return None


def download_lichess_dump_file(file_name: str, log=lambda message: None, on_progress = lambda progress, total_size: None, on_done=lambda: None) -> bool:   
    """
    Download a Lichess monthly dump file by name and save it to the raw data directory.

    Uses a temporary filename during download.
    Progress is reported per chunk via on_progress(bytes, total).

    Returns:
        bool: True on success, False if the file already exists or the request fails.
    """

    # Hanlde paths
    local_path = os.path.join(PATH_DATA_RAW_DIR, file_name)
    tmp_path = os.path.join(PATH_DATA_RAW_DIR, get_download_tmp_file_name(file_name)) 

    if os.path.exists(local_path):
        log(f"File {file_name} already exists.")
        return False

    # Initialize download
    url = get_lichess_url(file_name)
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
                on_progress(len(chunk), total_size)

    os.replace(tmp_path, local_path)
    on_done()
    log("Download completed.")
    return True


# NOTE could also get numbers of games per dump from https://database.lichess.org/standard/counts.txt for some extra UX feedback