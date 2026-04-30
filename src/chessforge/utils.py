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