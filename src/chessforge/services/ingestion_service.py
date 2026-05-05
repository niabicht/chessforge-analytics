import os
from typing import Callable

from chessforge.utils.utils import get_dataset_name_from_file_path
from chessforge.database.connection import get_initialized_connection
from chessforge.database.repository import does_dataset_exist, create_dataset, flush_games_batch_into_database, update_dataset_game_count
from chessforge.ingestion.streamer import stream_pgn_zst
from chessforge.ingestion.parser import parse_game_string_into_dict


def validate_ingestion(file_path) -> tuple[bool, str]: # TODO bass message via log callback instead of return
    if not os.path.exists(file_path):
        error_message = f"File {file_path} not found."
        return False, error_message

    dataset_name = get_dataset_name_from_file_path(file_path)
    connection = get_initialized_connection()
    does_exist = does_dataset_exist(connection, dataset_name)
    connection.close()
    if does_exist:
        error_message = f"Dataset already ingested from {file_path}."
        return False, error_message
        
    return True, "Validation successful."

    
def ingest_file(file_path: str, on_progress: Callable[[int, int], None] = None):    
    connection = get_initialized_connection()
    
    dataset_name = get_dataset_name_from_file_path(file_path)

    # create dataset entry
    dataset_id = create_dataset(connection, dataset_name)

    batch_size = 400  # TODO tune this    
    batch = []
    game_counter = 0
    def on_stream_progress(bytes_progress: int):
        if on_progress: on_progress(bytes_progress, game_counter)

    for game_text in stream_pgn_zst(file_path, on_progress=on_stream_progress):      
        game = parse_game_string_into_dict(game_text)
        batch.append(game)
        game_counter += 1
        if len(batch) >= batch_size:            
            flush_games_batch_into_database(connection, batch, dataset_id)        

        if game_counter >= 5000: # TODO remove this, just for testing
            if on_progress: on_progress(0, game_counter)
            break 

    # flush remaining
    if batch:
        if on_progress: on_progress(0, game_counter)
        flush_games_batch_into_database(connection, batch, dataset_id)

    update_dataset_game_count(connection, dataset_id, game_counter)
    connection.close()



