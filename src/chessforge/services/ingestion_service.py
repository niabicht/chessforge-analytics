import os
from typing import Callable

import chessforge.database.connections as connections
import chessforge.database.repository as repository
import chessforge.ingestion.streamer as streamer
import chessforge.ingestion.parser as parser
from chessforge.utils.utils import get_dataset_name_from_file_path


def validate_ingestion(file_path) -> tuple[bool, str]: # TODO pass message via log callback instead of return
    # Check if input file exists
    if not os.path.exists(file_path):
        error_message = f"File {file_path} not found."
        return False, error_message

    # Check if file had already been ingeszed
    connection = connections.get_initialized_connection()
    dataset_name = get_dataset_name_from_file_path(file_path)
    does_exist = repository.does_dataset_exist(connection, dataset_name)
    connection.close()
    if does_exist:
        error_message = f"Dataset already ingested from {file_path}."
        return False, error_message
        
    return True, "Validation successful."

    
def ingest_file(file_path: str, on_progress: Callable[[int, int], None] = None) -> None:
    dataset_name = get_dataset_name_from_file_path(file_path)

    # Create dataset entry
    connection = connections.get_initialized_connection()
    dataset_id = repository.register_dataset_return_id(connection, dataset_name)

    # Stream games from input file, parse, and ingest in batches into database
    batch_size = 400  # TODO tune this    
    batch = []
    game_counter = 0
    def on_stream_progress(bytes_progress: int): # TODO maybe make this lambda instead
        if on_progress: on_progress(bytes_progress, game_counter)

    for game_text in streamer.stream_pgn_zst_generator(file_path, on_progress=on_stream_progress):      
        game = parser.parse_game_string_into_dict(game_text)
        batch.append(game)
        game_counter += 1

        if len(batch) >= batch_size:            
            repository.flush_games_batch_into_database(connection, batch, dataset_id)        

        if game_counter >= 5000: # TODO remove this, just for testing
            if on_progress: on_progress(0, game_counter)
            break 

    # Flush remaining
    if batch:
        if on_progress: on_progress(0, game_counter)
        repository.flush_games_batch_into_database(connection, batch, dataset_id)

    repository.update_dataset_game_count(connection, dataset_id, game_counter)
    connection.close()



