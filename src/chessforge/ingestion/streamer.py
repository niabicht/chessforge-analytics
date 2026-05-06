from typing import Iterator

import zstandard as zstd


def stream_pgn_zst_generator(file_path: str, on_progress = lambda progress: None, on_done=lambda: None) -> Iterator[str]:
    """
    Stream a compressed Lichess .pgn.zst file, yielding one game at a time as string.
    The file is decompressed and processed in chunks to avoid loading it fully into memory.
    Separate games are detected by looking for "[Event" headers and then reconstructed line by line.

    Args:
        file_path: E.g. "data/raw/lichess_db_standard_rated_2026-04.pgn.zst"
        on_progress: Optional callback receiving the number of processed bytes per chunk.

    Yields:
        str: A single pgn game as a string.
    """

    with open(file_path, "rb") as file:
        decompressor = zstd.ZstdDecompressor()
        stream_reader = decompressor.stream_reader(file)

        buffer = ""
        current_game = ""

        while True:
            # Get the next chunk
            chunk_size = 65536 # 65536 = 64 KB
            chunk = stream_reader.read(chunk_size)
            if not chunk: break
            on_progress(len(chunk))

            # Decode into string lines
            buffer += chunk.decode("utf-8")
            lines = buffer.split("\n")
            buffer = lines[-1] # The last line is likely incomplete and will be handled with the next chunk

            # Add lines to current game. When new game starts, yield the current game.
            for line in lines[:-1]:
                if line.startswith("[Event") and current_game:
                    yield current_game.strip()
                    current_game = ""

                current_game += line + "\n"

        # After the loop, the last game will not have been yielded. So lets do that.
        if current_game:
            yield current_game.strip()

    on_done()