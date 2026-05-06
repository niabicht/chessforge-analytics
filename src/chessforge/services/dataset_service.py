import chessforge.database.connections as connections
import chessforge.database.repository as repository


def get_datasets_string() -> str: # TODO log directly via callback
    # Get info from database
    connection = connections.get_initialized_connection()
    rows = repository.get_datasets_info(connection)

    # Start the string
    datasets_string = "Datasets:"
    if not rows:
        datasets_string += "\nNone"
        return datasets_string

    # Add line for every dataset
    n_games_total = 0
    for name, count, timestamp in rows:
        n_games_total += count if count else 0
        count_string = "corrupted" if count is None else f"{count} games"
        timestamp_string = "corrupted" if timestamp is None else f"{timestamp:%Y-%m-%d %H:%M}"
        datasets_string += f"\n{name:33} | {count_string:14} | {timestamp_string}"
    
    datasets_string += f"\nTotal games: {n_games_total}"

    connection.close()
    return datasets_string


def delete_dataset(all: bool = False, dataset_name: str = None, log=lambda _: None) -> None:
    connection = connections.get_initialized_connection()

    if all:
        repository.delete_all_datasets(connection)
        log("All datasets deleted.")
    elif dataset_name:
        repository.delete_dataset(connection, dataset_name, log)

    connection.close()


