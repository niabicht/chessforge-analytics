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

        byte_buffer = b""
        line_buffer = ""
        current_game = ""

        while True:
            # Get the next chunk
            chunk = stream_reader.read(65536)
            if not chunk: break
            on_progress(len(chunk))

            # Accumulate bytes and decode, preserving incomplete multi-byte characters
            byte_buffer += chunk
            for trim in range(4):
                end = len(byte_buffer) - trim
                try:
                    decoded = byte_buffer[:end].decode("utf-8") # Decode as much as possible... 
                    byte_buffer = byte_buffer[end:] # ...keeping incomplete character as leftover
                    break
                except UnicodeDecodeError:
                    continue

            lines = (line_buffer + decoded).split("\n")
            line_buffer = lines[-1] # The last line is likely incomplete and will be handled with the next chunk

            # Add lines to current game. When new game starts, yield the current game.
            for line in lines[:-1]:
                if line.startswith("[Event") and current_game:
                    yield current_game.strip()
                    current_game = ""
                current_game += line + "\n"

        # Flush
        remaining = line_buffer + byte_buffer.decode("utf-8", errors="replace")
        for line in remaining.split("\n"):
            current_game += line + "\n"

        # After the loop, the last game will not have been yielded. So lets do that.
        if current_game.strip():
            yield current_game.strip()

    on_done()