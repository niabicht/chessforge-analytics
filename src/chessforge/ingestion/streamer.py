import zstandard as zstd
from typing import Callable


def stream_pgn_zst(file_path, on_progress: Callable[[int], None] = None):
    with open(file_path, "rb") as file:
        decompressor = zstd.ZstdDecompressor()
        stream_reader = decompressor.stream_reader(file)

        buffer = ""
        current_game = ""

        while True:
            chunk_size = 65536 # 65536 = 64 KB
            chunk = stream_reader.read(chunk_size)
            if not chunk: break
            if on_progress: on_progress(len(chunk))

            buffer += chunk.decode("utf-8")
            lines = buffer.split("\n")
            buffer = lines[-1]

            for line in lines[:-1]:
                if line.startswith("[Event") and current_game:
                    yield current_game.strip()
                    current_game = ""

                current_game += line + "\n"

        # after the loop, we probably still have one last game in the buffer that we need to yield
        if current_game:
            yield current_game.strip()