import os

import zstandard as zstd

import chessforge.ingestion.streamer as streamer
import chessforge.ingestion.downloader as downloader 
from chessforge.utils.global_constants import PATH_DATA_RAW
from chessforge.utils.utils import (
    get_recent_months_string_generator, 
    reservoir_sample_from_stream, 
    ensure_data_dir_exists,
    does_local_input_file_exist, 
    get_path_example_file, 
    get_path_lichess_file, 
    build_lichess_name, 
    is_input_lichess_file, 
    get_file_size_string, 
    contains_incomplete_download,
)


def create_example_file(n_games: int, log=lambda message: None, on_progress=lambda progress: None, on_done=lambda: None) -> bool:
    # Find input file
    input_path = None
    n_months = 60
    for month in get_recent_months_string_generator(n_months):
        if does_local_input_file_exist(month):
            input_path = get_path_lichess_file(month)
            break
    if not input_path:
        log(f"No local input file found for the last {n_months}.")
        return False 

    # Get the example games
    stream = streamer.stream_pgn_zst_generator(input_path, on_progress=on_progress, on_done=on_done)
    sampled_games = reservoir_sample_from_stream(stream, n_games) # all loaded into memory simultaniously

    # Write to compressed output file
    log("Writing to file...")
    output_path = get_path_example_file()
    with open(output_path, "wb") as file:
        compressor = zstd.ZstdCompressor(level=20)
        with compressor.stream_writer(file) as writer:
            for game in sampled_games:
                writer.write((game + "\n\n").encode("utf-8"))

    log(f"Saved example file to {output_path}.")
    return True


def list_files(log=lambda message: None, on_all_files=lambda has_incomplete_download: None) -> bool:
    ensure_data_dir_exists(PATH_DATA_RAW)

    files = os.listdir(PATH_DATA_RAW)
    pgn_files = [f for f in files if is_input_lichess_file(f)]
    tmp_files = [f for f in files if contains_incomplete_download(f)]

    output = "Data files:"
    if not pgn_files: output += "\nNone"
    else:
        for file_name in pgn_files:
            size = get_file_size_string(os.path.join(PATH_DATA_RAW, file_name))
            output += f"\n{file_name} ({size})"

    if tmp_files:
        output += "\n\nIncomplete downloads:"
        for file_name in tmp_files:
            size = get_file_size_string(os.path.join(PATH_DATA_RAW, file_name))
            output += f"\n{file_name} ({size})"

    log(output)
    on_all_files(len(tmp_files) > 0)

    return True


def download(month: str = None, download_latest: bool = False, log=lambda message: None, on_progress = lambda progress, total_size: None, on_done=lambda: None) -> bool:
    if download_latest:
        # Find month of latest Lichess dump
        n_months = 12
        month = downloader.find_latest_lichess_dump_month(n_months)
        if not month:
            log(f"No dataset found in the last {n_months} months.")
            return False
        log(f"Latest dataset found: {month}")

    # Handle local path and download
    ensure_data_dir_exists(PATH_DATA_RAW)
    file_name = build_lichess_name(month, add_file_extension=True)
    is_success = downloader.download_lichess_dump_file(file_name, log=log, on_progress=on_progress, on_done=on_done)

    return is_success


def delete_files(all: bool = False, month: str = None, log=lambda message: None) -> bool:
    if all:
        deleted = 0
        for file in os.listdir(PATH_DATA_RAW):
            if is_input_lichess_file(file):
                os.remove(os.path.join(PATH_DATA_RAW, file))
                deleted += 1

        log(f"Deleted {deleted} files.")
        return True

    # Find and delete specific file
    file_name = build_lichess_name(month, add_file_extension=True)
    file_path = os.path.join(PATH_DATA_RAW, file_name)

    if os.path.exists(file_path):
        os.remove(file_path)
        log(f"Deleted {file_name}.")
        return True
    else:
        log(f"File {file_name} not found.")
        return False


def delete_incomplete_downloads(log=lambda message: None) -> bool:
    deleted = 0
    for file in os.listdir(PATH_DATA_RAW):
        if contains_incomplete_download(file):
            os.remove(os.path.join(PATH_DATA_RAW, file))
            deleted += 1

    log(f"Deleted {deleted} incomplete-download files.")
    return True