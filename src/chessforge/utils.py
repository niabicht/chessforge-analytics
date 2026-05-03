import os
import random
import requests
import re
import typer

from datetime import datetime

from chessforge.global_constants import LICHESS_PGN_FILE_NAME_PREFIX


def int_or_none(value):
    try: return int(value)
    except: return None


def generate_past_months(n: int = 12):
    now = datetime.now()
    year = now.year
    month = now.month

    for _ in range(n):
        yield f"{year:04d}-{month:02d}"

        month -= 1
        if month == 0:
            month = 12
            year -= 1


def remote_file_exists(url: str) -> bool:
    response = requests.head(url)
    return (response.status_code == 200)


def is_lichess_pgn_file(file_name: str) -> bool:
    return (file_name.startswith(LICHESS_PGN_FILE_NAME_PREFIX) and file_name.endswith(".pgn.zst"))


def build_lichess_name(month: str, use_file_extension: bool = False) -> str:
    name = f"{LICHESS_PGN_FILE_NAME_PREFIX}_{month}"
    return f"{name}.pgn.zst" if use_file_extension else name            


def echo_file_with_size(path, file_name):
    size_gib = os.path.getsize(os.path.join(path, file_name)) / (1024**3)
    typer.echo(f"{file_name} ({size_gib:.2f} GiB)")


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


def reservoir_sample_from_stream(stream, k: int):
    """
    Reservoir sampling over a stream of items.

    Picks k uniformly random items from a stream of unknown size
    in a single pass using O(k) memory.

    This is ideal for huge datasets (like Lichess PGN dumps)
    where loading everything into memory is not possible.
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