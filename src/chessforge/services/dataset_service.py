from chessforge.database.connection import get_initialized_connection
from chessforge.database.repository import get_datasets_info, delete_dataset, delete_all_datasets


def get_datasets_list_string():
    connection = get_initialized_connection()
    rows = get_datasets_info(connection)

    datasets_list_string = "Datasets:"
    if not rows:
        datasets_list_string += "\nNone"
        return datasets_list_string

    n_games_total = 0
    for name, count, timestamp in rows:
        count_string = "corrupted" if count is None else f"{count} games"
        timestamp_string = "corrupted" if timestamp is None else f"{timestamp:%Y-%m-%d %H:%M}"
        datasets_list_string += f"\n{name:33} | {count_string:14} | {timestamp_string}"
        n_games_total += count if count else 0
    
    datasets_list_string += f"\nTotal games: {n_games_total}"

    connection.close()
    return datasets_list_string

def datasets_delete(all: bool = False, dataset_name: str = None, log=lambda _: None) -> None:
    connection = get_initialized_connection()

    if all:
        delete_all_datasets(connection)
        log("All datasets deleted.")
    elif dataset_name:
        delete_dataset(connection, dataset_name, on_result_message=log)

    connection.close()


