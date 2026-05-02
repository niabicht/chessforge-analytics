import chess.pgn
import os
from tqdm import tqdm
import zstandard as zstd

from io import StringIO

from chessforge.global_constants import GAME_COLUMNS
from chessforge.utils import int_or_none


def stream_pgn_zst(file_path):
    """Generator that yields individual PGN games as strings from a .zst-compressed file."""

    with open(file_path, "rb") as file:
        decompressor = zstd.ZstdDecompressor()
        stream_reader = decompressor.stream_reader(file)

        buffer = ""
        current_game = ""

        # UI
        progress_bar = tqdm(total=None, unit="B", unit_scale=True, desc="Streaming Lichess dataset")
        game_counter = 0

        while True:
            chunk_size = 65536 # 65536 = 64 KB. don't make it smaller than one single game, so AT LEAST 8 KB    
            chunk = stream_reader.read(chunk_size)
            if not chunk: break

            buffer += chunk.decode("utf-8", errors="ignore")
            lines = buffer.split("\n")
            buffer = lines[-1]

            # UI
            progress_bar.update(len(chunk))

            for line in lines[:-1]:
                if line.startswith("[Event") and current_game: 
                    # a new game is starting, so we can yield the current one
                    yield current_game.strip()
                    current_game = ""

                    # UI
                    game_counter += 1
                    progress_bar.set_postfix(games=game_counter)

                current_game += line + "\n"

        # after the loop, we probably still have one last game in the buffer that we need to yield
        if current_game:
            yield current_game.strip()

        # UI
        progress_bar.close()


def parse_game_string_into_dict(pgn_text: str):
    """Parses a single PGN game text and extracts relevant information into a dict."""
    game = chess.pgn.read_game(StringIO(pgn_text))
    if game is None:
        print(f"WARNING: could not parse PGN: {pgn_text[:100]}")
        return None

    headers = game.headers

    game_dict = {}
    for column, column_type in GAME_COLUMNS.items():
        value = headers.get(column)
        if column_type == "INT": value = int_or_none(value) # none if e.g. player has no elo
        game_dict[column] = value

    return game_dict