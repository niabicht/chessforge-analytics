import os

import chessforge.database.connections as connections
import chessforge.database.repository as repository
import chessforge.ingestion.streamer as streamer
import chessforge.ingestion.parser as parser
from chessforge.utils.utils import get_path_example_file, get_path_lichess_file, get_dataset_name_from_file_path


def validate_ingestion(ingest_example: bool, month: str, log=lambda message: None) -> bool:
    # Determine file path
    file_path = get_path_example_file() if ingest_example else get_path_lichess_file(month)

    # Check if input file exists
    if not os.path.exists(file_path):
        log(f"File {file_path} not found.")
        return False

    # Check if file had already been ingeszed
    with connections.InitializedConnection() as connection:
        dataset_name = get_dataset_name_from_file_path(file_path)
        does_exist = repository.does_dataset_exist(connection, dataset_name)
        if does_exist:
            log(f"Dataset already ingested from {file_path}.")
            return False

        log("Validation successful.")
        return True

        
def ingest_file(ingest_example: bool, month, on_progress = lambda progress, games: None, on_done=lambda: None) -> bool:
    """
    Orchestrates the ETL process: 
    Streams compressed PGN data.
    Parses game headers into structured dictionaries.
    Bulk inserts into the database.
    """

    # Determine file path
    file_path = get_path_example_file() if ingest_example else get_path_lichess_file(month)

    with connections.InitializedConnection() as connection:
        # Create dataset entry
        dataset_name = get_dataset_name_from_file_path(file_path)
        dataset_id = repository.register_dataset_return_id(connection, dataset_name)

        # Stream games from input file, parse, and ingest in batches into database
        batch_size = 400  # TODO tune this    
        batch = []
        game_counter = 0

        def on_stream_progress(bytes_progress: int):
            on_progress(bytes_progress, game_counter)

        # TODO optimize: batch parsing + multiprocessing, build custom (faster) parser, could also insert on a parralel threat
        for game_text in streamer.stream_pgn_zst_generator(file_path, on_progress=on_stream_progress): 
            game = parser.parse_game_string_into_dict(game_text) # NOTE Rather slow. Could maybe parallelize.
            batch.append(game)
            game_counter += 1

            if len(batch) >= batch_size:           
                repository.flush_games_batch_into_database(connection, batch, dataset_id)        

            # if game_counter >= 5000: # TODO remove this, just for testing
            #     on_progress(0, game_counter)
            #     break         

        # Flush remaining
        if batch:
            on_progress(0, game_counter)
            repository.flush_games_batch_into_database(connection, batch, dataset_id)

        repository.update_dataset_game_count(connection, dataset_id, game_counter)

    on_done()
    return True



