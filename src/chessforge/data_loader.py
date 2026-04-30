import chess.pgn
import zstandard as zstd

from io import StringIO

from chessforge.utils import int_or_none


def stream_pgn_zst(file_path):
    """Generator that yields individual PGN games as strings from a .zst-compressed file."""
    with open(file_path, "rb") as file:
        decompressor = zstd.ZstdDecompressor()
        stream_reader = decompressor.stream_reader(file)

        buffer = ""
        current_game = ""
        while True:
            chunk = stream_reader.read(65536) # 65536 = 64 KB. don't make it smaller than one single game, so AT LEAST 8 KB 
            if not chunk: break

            buffer += chunk.decode("utf-8", errors="ignore")
            lines = buffer.split("\n")
            buffer = lines[-1]

            for line in lines[:-1]:
                if line.startswith("[Event") and current_game: 
                    # a new game is starting, so we can yield the current one
                    yield current_game.strip()
                    current_game = ""

                current_game += line + "\n"

        # after the loop, we probably still have one last game in the buffer that we need to yield
        if current_game:
            yield current_game.strip()


def parse_game_string_into_dict(pgn_text: str):
    """Parses a single PGN game text and extracts relevant information into a dict."""
    game = chess.pgn.read_game(StringIO(pgn_text))
    if game is None:
        print(f"WARNING: could not parse PGN: {pgn_text[:100]}")
        return None

    headers = game.headers
    return {
        "event": headers.get("Event"),
        "result": headers.get("Result"),
        "white_elo": int_or_none(headers.get("WhiteElo", None)),
        "black_elo": int_or_none(headers.get("BlackElo", None)),
        "eco": headers.get("ECO"), # opening classification code
        "opening": headers.get("Opening", None),
        "time_control": headers.get("TimeControl"),
        "termination": headers.get("Termination"),
    }