import chessforge.database.connections as connections
import chessforge.database.repository as repository
from chessforge.utils.utils import get_example_dataset_name, build_lichess_name


def log_datasets(log=lambda message: None) -> bool:
    # Get info from database
    rows = []
    with connections.InitializedConnection() as connection:
        rows = repository.get_datasets_info(connection)

    # Start the string
    datasets_string = "Datasets:"
    if not rows:
        datasets_string += "\nNone"
        log(datasets_string)
        return True

    # Add line for every dataset
    n_games_total = 0
    for name, count, timestamp in rows:
        n_games_total += count if count else 0
        count_string = "corrupted" if count is None else f"{count} games"
        timestamp_string = "corrupted" if timestamp is None else f"{timestamp:%Y-%m-%d %H:%M}"
        datasets_string += f"\n{name:33} | {count_string:14} | {timestamp_string}"
    
    datasets_string += f"\nTotal games: {n_games_total}"

    log(datasets_string)
    return True


def delete_dataset(all: bool, example: bool, month: str, log=lambda message: None) -> bool:   

    with connections.InitializedConnection() as connection:
        if all:
            repository.delete_all_datasets(connection)
            log("All datasets deleted.")
        else:
            dataset_name = get_example_dataset_name() if example else build_lichess_name(month)  
            repository.delete_dataset(connection, dataset_name, log)

    return True


