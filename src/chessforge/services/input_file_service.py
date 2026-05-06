import os
from typing import Callable

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
)


def create_example_file(n_games: int, log=lambda _: None) -> None:
    # Find input file
    input_path = None
    for month in get_recent_months_string_generator():
        if does_local_input_file_exist(month):
            input_path = get_path_lichess_file(month)
            break
    if not input_path:
        log("No local input file found. Aborting.")
        return    

    # Get the example games
    stream = streamer.stream_pgn_zst_generator(input_path)
    sampled_games = reservoir_sample_from_stream(stream, n_games) # all loaded into memory simultaniously

    # Write to compressed output file
    output_path = get_path_example_file()
    with open(output_path, "wb") as file:
        compressor = zstd.ZstdCompressor(level=20)
        with compressor.stream_writer(file) as writer:
            for game in sampled_games:
                writer.write((game + "\n\n").encode("utf-8"))

    log(f"Saved example file to {output_path}")


def get_all_files_string() -> None:
    ensure_data_dir_exists(PATH_DATA_RAW)

    # Find all input files
    files = os.listdir(PATH_DATA_RAW)
    file_names_pgn = [file for file in files if is_input_lichess_file(file)]
    file_names_tmp = [file for file in files if (file.startswith("download") and file.endswith(".tmp"))]

    # Add good files
    all_files_string = "Data files:"
    if not file_names_pgn: all_files_string += "\nNone"
    for file_name in file_names_pgn:
        size = get_file_size_string(os.path.join(PATH_DATA_RAW, file_name))
        all_files_string += f"\n{file_name} ({size})"
        
    # Add incomplete downloads
    if file_names_tmp: all_files_string += "\n\nIncomplete downloads:"
    for file_name in file_names_tmp:
        size = get_file_size_string(os.path.join(PATH_DATA_RAW, file_name))
        all_files_string += f"\n{file_name} ({size})"

    return all_files_string


def download(month: str = None, download_latest: bool = False, log=lambda _: None, on_progress: Callable[[int, int], None] = None) -> bool:
    if download_latest:
        # Find month of latest Lichess dump
        month = downloader.find_latest_lichess_dump_month()
        if not month:
            log("No dataset found in the last 12 months.")
            return False
        log(f"Latest dataset found: {month}")

    # Handle local path and download
    ensure_data_dir_exists(PATH_DATA_RAW)
    filename = build_lichess_name(month, add_file_extension=True)
    success = downloader.download_lichess_dump_file(filename=filename, log=log, on_progress=on_progress)

    return success


def delete_files(all: bool = False, month: str = None, log=lambda _: None) -> None:
    if all:
        deleted = 0
        for file in os.listdir(PATH_DATA_RAW):
            if is_input_lichess_file(file):
                os.remove(os.path.join(PATH_DATA_RAW, file))
                deleted += 1

        log(f"Deleted {deleted} files.")
        return

    # Find and delete specific file
    filename = build_lichess_name(month, add_file_extension=True)
    file_path = os.path.join(PATH_DATA_RAW, filename)

    if os.path.exists(file_path):
        os.remove(file_path)
        log(f"Deleted {filename}.")
    else:
        log(f"File {filename} not found.")


def delete_incomplete_downloads(log=lambda _: None) -> None:
    deleted = 0
    for file in os.listdir(PATH_DATA_RAW):
        if file.endswith(".tmp"):
            os.remove(os.path.join(PATH_DATA_RAW, file))
            deleted += 1

    log(f"Deleted {deleted} incomplete-download files.")