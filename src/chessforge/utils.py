import random
import re


def int_or_none(value):
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
            # Phase 2: replace elements with decreasing probability
            # i is current index (0-based)
            # j is random position in [0, i]
            j = random.randint(0, i)

            # If random index falls inside reservoir, replace it
            if j < k:
                sample[j] = item

    return sample