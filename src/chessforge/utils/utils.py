import os
import random
import re
from datetime import datetime
from typing import Any, Iterator

from chessforge.utils.global_constants import (
    PATH_EXAMPLE_DIR, 
    PATH_DATA_RAW_DIR, 
    NAME_EXAMPLE_FILE, 
    PREFIX_LICHESS_PGN_FILE, 
    PREFIX_DOWNLOAD_TMP_FILE,
    EXTENSION_INPUT_FILE,
    EXTENSION_DOWNLOAD_TMP_FILE,
)


##############
### File Utils
##############

def get_path_example_file() -> str:
    return os.path.join(PATH_EXAMPLE_DIR, NAME_EXAMPLE_FILE + EXTENSION_INPUT_FILE)

def get_example_dataset_name() -> str:
    return NAME_EXAMPLE_FILE

def get_path_lichess_file(month: str) -> str:
    file_name = build_lichess_name(month, add_file_extension=True)
    return os.path.join(PATH_DATA_RAW_DIR, file_name)

def build_lichess_name(month: str, add_file_extension: bool = False) -> str:
    name = f"{PREFIX_LICHESS_PGN_FILE}{month}"
    if add_file_extension: name += EXTENSION_INPUT_FILE
    return name   

def get_dataset_name_from_file_path(file_path: str) -> str:
    return file_path.split("/")[-1].replace(EXTENSION_INPUT_FILE, "")

def is_input_lichess_file(file_name: str) -> bool:
    return (file_name.startswith(PREFIX_LICHESS_PGN_FILE) and file_name.endswith(EXTENSION_INPUT_FILE))

def does_local_input_file_exist(month: str) -> bool:
    file_path = get_path_lichess_file(month)
    return os.path.exists(file_path)

def get_file_size_string(file_path) -> str:
    size_gib = os.path.getsize(file_path) / (1024**3)
    return f"{size_gib:.2f} GiB"

def ensure_data_dir_exists(path) -> None:
    os.makedirs(path, exist_ok=True)


   
#########################
### Type and String Utils
#########################

def identity(value: Any) -> Any:
    return value

def int_or_none(value) -> int | None:
    try: return int(value)
    except: return None

def str_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) else None

def mixed_to_snake(string: str) -> str:
    # Insert underscore before capitalized words. E.g. "<anything>Xy" -> "<anything>_Xy" or "WhiteElo" -> "White_Elo"
    string = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', string)

    # Insert underscore between remaining lowercase/digit that is followed by uppercase. E.g. "xYZ" -> "x_YZ" or "WhiteELO" -> "White_ELO"
    string = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', string)

    # Transform e.g. "White_Elo" to "white_elo"
    return string.lower()

def kebab_to_snake(string: str) -> str:
    return string.replace("-", "_")

def snake_to_kebab(string: str) -> str:
    return string.replace("_", "-")

def get_recent_months_string_generator(n: int) -> Iterator[str]:
    now = datetime.now()
    year = now.year
    month = now.month

    for _ in range(n):
        yield f"{year:04d}-{month:02d}" # E.g. "2026-04"

        month -= 1
        if month == 0:
            month = 12
            year -= 1

def get_download_tmp_file_name(file_name: str) -> str:
    return (PREFIX_DOWNLOAD_TMP_FILE + file_name + EXTENSION_DOWNLOAD_TMP_FILE)
         
def contains_incomplete_download(string: str) -> bool:
    return (PREFIX_DOWNLOAD_TMP_FILE in string and EXTENSION_DOWNLOAD_TMP_FILE in string)       

#############################
### Random and Sampling Utils
#############################

def reservoir_sample_from_stream(stream, k: int) -> list:
    """
    Reservoir sampling over a stream of items. Works for any stream item type.
    Picks k uniformly random items from a stream of unknown size in a single pass using O(k) memory.
    """

    sample = []
    for i, item in enumerate(stream):
        if i < k:
            # Phase 1: fill the reservoir with first k items
            sample.append(item)
        else:
            # Phase 2: replace elements with decreasing probability
            # i is current index (0-based)
            # j is random position in [0, i]
            j = random.randint(0, i)

            # If random index falls inside reservoir, replace it
            if j < k:
                sample[j] = item

    return sample




















