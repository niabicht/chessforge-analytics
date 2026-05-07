import pytest

from chessforge.ingestion.parser import parse_game_string_into_dict
from chessforge.ingestion.feature_registry import GAME_COLUMNS


# ---------------------------------------------------------------------------
# Test fixtures — realistic PGN strings sourced from actual Lichess dumps
# ---------------------------------------------------------------------------

# A normal game with Stockfish evaluations in the move text
PGN_WITH_EVAL = """[Event "Rated Rapid game"]
[Site "https://lichess.org/0rgP75Yn"]
[Date "2021.04.10"]
[Round "-"]
[White "isaacdoRJ"]
[Black "CGP85"]
[Result "1-0"]
[UTCDate "2021.04.10"]
[UTCTime "01:30:46"]
[WhiteElo "960"]
[BlackElo "937"]
[WhiteRatingDiff "+25"]
[BlackRatingDiff "-5"]
[ECO "C60"]
[Opening "Ruy Lopez: Nürnberg Variation"]
[TimeControl "600+0"]
[Termination "Normal"]

1. e4 { [%eval 0.05] [%clk 0:10:00] } 1... e5 { [%eval 0.21] [%clk 0:10:00] } 2. Nf3 { [%eval 0.23] [%clk 0:10:00] } 2... f6? { [%eval 1.75] [%clk 0:09:57] } 3. Bb5 { [%eval 1.27] [%clk 0:09:59] } 3... Nc6 { [%eval 1.0] [%clk 0:09:45] } 4. Bxc6? { [%eval -0.52] [%clk 0:09:53] } 4... bxc6? { [%eval 0.86] [%clk 0:09:43] } 1-0"""

# A normal game without Stockfish evaluations
PGN_WITHOUT_EVAL = """[Event "Rated Bullet tournament https://lichess.org/tournament/N3m5vd2o"]
[Site "https://lichess.org/frYKvMLS"]
[Date "2021.04.07"]
[Round "-"]
[White "neoplan7777"]
[Black "vabatgrav"]
[Result "0-1"]
[UTCDate "2021.04.07"]
[UTCTime "14:07:53"]
[WhiteElo "2087"]
[BlackElo "1969"]
[WhiteRatingDiff "-8"]
[BlackRatingDiff "+8"]
[ECO "B40"]
[Opening "Sicilian Defense: Four Knights Variation, Exchange Variation"]
[TimeControl "60+0"]
[Termination "Time forfeit"]

1. e4 { [%clk 0:01:00] } 1... c5 { [%clk 0:01:00] } 2. Nf3 { [%clk 0:00:59] } 2... Nc6 { [%clk 0:00:59] } 0-1"""

# A game where both players are anonymous (no Elo headers)
PGN_NO_ELO = """[Event "Rated Blitz game"]
[Site "https://lichess.org/abc123"]
[Date "2021.04.10"]
[Round "-"]
[White "Anonymous"]
[Black "Anonymous"]
[Result "1/2-1/2"]
[UTCDate "2021.04.10"]
[UTCTime "12:00:00"]
[ECO "A00"]
[Opening "Uncommon Opening"]
[TimeControl "300+0"]
[Termination "Normal"]

1. e4 e5 1/2-1/2"""

# Completely empty / garbage string — should return None without crashing
PGN_MALFORMED = "this is not a pgn game at all $$$ !!!"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestParseGameStringIntoDict:

    def test_returns_dict_for_valid_game_with_eval(self):
        result = parse_game_string_into_dict(PGN_WITH_EVAL)
        assert isinstance(result, dict)

    def test_returns_dict_for_valid_game_without_eval(self):
        result =parse_game_string_into_dict(PGN_WITHOUT_EVAL)
        assert isinstance(result, dict)

    def test_output_contains_all_expected_columns(self):
        """Output keys must match GAME_COLUMNS."""
        result = parse_game_string_into_dict(PGN_WITH_EVAL)
        for column in GAME_COLUMNS:
            assert column in result, f"Expected column '{column}' missing from parsed result"

    def test_output_contains_no_unexpected_columns(self):
        """Parser should not return columns outside GAME_COLUMNS."""
        result = parse_game_string_into_dict(PGN_WITH_EVAL)
        for key in result:
            assert key in GAME_COLUMNS, f"Unexpected column '{key}' in parsed result"

    def test_correct_values_with_eval(self):
        result = parse_game_string_into_dict(PGN_WITH_EVAL)
        assert result["Result"] == 2
        assert result["WhiteElo"] == 960
        assert result["BlackElo"] == 937
        assert result["ECO"] == 260
        assert result["TimeControl"] == 600

    def test_correct_values_without_eval(self):
        result = parse_game_string_into_dict(PGN_WITHOUT_EVAL)
        assert result["Result"] == 0
        assert result["WhiteElo"] == 2087
        assert result["BlackElo"] == 1969
        assert result["ECO"] == 140
        assert result["TimeControl"] == 60


    def test_elo_fields_are_integers(self):
        """INT-typed columns in GAME_COLUMNS must be parsed as int, not string."""
        result = parse_game_string_into_dict(PGN_WITH_EVAL)
        int_columns = [col for col, typ in GAME_COLUMNS.items() if typ == "INT"]
        for col in int_columns:
            value = result[col]
            assert value is None or isinstance(value, int), (
                f"Column '{col}' should be int or None, got {type(value)}"
            )

    def test_missing_elo_returns_none(self):
        """Anonymous players have no Elo header. Should produce None, not crash."""
        result = parse_game_string_into_dict(PGN_NO_ELO)
        assert result["WhiteElo"] is None
        assert result["BlackElo"] is None

    def test_malformed_pgn_returns_none_or_empty(self):
        """python-chess is lenient — garbage input returns a dict with null/unknown values
        rather than None. The important thing is it does not raise an exception."""
        result = parse_game_string_into_dict(PGN_MALFORMED)
        assert result is None or isinstance(result, dict)
