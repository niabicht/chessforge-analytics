import os
import random
import re
from datetime import datetime
from typing import Iterator

from chessforge.utils.global_constants import PATH_EXAMPLE_FOLDER, NAME_EXAMPLE_FILE, PATH_DATA_RAW, LICHESS_PGN_FILE_NAME_PREFIX, INPUT_FILE_EXTENSION


##############
### File Utils
##############

def get_path_example_file() -> str:
    return os.path.join(PATH_EXAMPLE_FOLDER, NAME_EXAMPLE_FILE + INPUT_FILE_EXTENSION)

def get_example_dataset_name() -> str:
    return NAME_EXAMPLE_FILE

def get_path_lichess_file(month: str) -> str:
    file_name = build_lichess_name(month, add_file_extension=True)
    return os.path.join(PATH_DATA_RAW, file_name)

def build_lichess_name(month: str, add_file_extension: bool = False) -> str:
    name = f"{LICHESS_PGN_FILE_NAME_PREFIX}_{month}"
    if add_file_extension: name += INPUT_FILE_EXTENSION
    return name   

def get_dataset_name_from_file_path(file_path: str) -> str:
    return file_path.split("/")[-1].replace(INPUT_FILE_EXTENSION, "")

def is_input_lichess_file(file_name: str) -> bool:
    return (file_name.startswith(LICHESS_PGN_FILE_NAME_PREFIX) and file_name.endswith(INPUT_FILE_EXTENSION))

def does_local_input_file_exist(month: str) -> bool:
    file_path = get_path_lichess_file(month)
    return os.path.exists(file_path)

def get_file_size_string(file_path) -> str:
    size_gib = os.path.getsize(file_path) / (1024**3)
    return f"{size_gib:.2f} GiB"

def ensure_data_dir_exists(path):
    os.makedirs(path, exist_ok=True)


   
#########################
### Type and String Utils
#########################

def int_or_none(value) -> int | None:
    try: return int(value)
    except: return None

def camel_to_snake(name: str) -> str:
    """
    Converts CamelCase or mixedCase to snake_case.

    Examples:
        WhiteElo -> white_elo
        TimeControl -> time_control
        ECO -> eco
    """
    # Handle transitions like "WhiteElo" -> "White_Elo"
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)

    # Handle transitions like "ELOValue" -> "ELO_Value"
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)

    return s2.lower()

def kebab_to_snake(name: str) -> str:
    return name.replace("-", "_")

def snake_to_kebab(name: str) -> str:
    return name.replace("_", "-")

def generate_past_months(n: int = 12) -> Iterator[str]:
    now = datetime.now()
    year = now.year
    month = now.month

    for _ in range(n):
        yield f"{year:04d}-{month:02d}"

        month -= 1
        if month == 0:
            month = 12
            year -= 1
            

#############################
### Random and Sampling Utils
#############################

def reservoir_sample_from_stream(stream, k: int) -> list:
    """
    Reservoir sampling over a stream of items.

    Picks k uniformly random items from a stream of unknown size
    in a single pass using O(k) memory.
    """

    sample = []
    for i, item in enumerate(stream):
        if i < k:
            # Phase 1: fill the reservoir with first k items
            sample.append(item)
        else:
            break # TODO remove, just for testing
            # Phase 2: replace elements with decreasing probability
            # i is current index (0-based)
            # j is random position in [0, i]
            j = random.randint(0, i)

            # If random index falls inside reservoir, replace it
            if j < k:
                sample[j] = item

    return sample




















