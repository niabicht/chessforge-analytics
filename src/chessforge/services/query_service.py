import os

import chessforge.database.connections as connections
import chessforge.database.repository as repository
from chessforge.utils.global_constants import PATH_QUERY_FOLDER
from chessforge.utils.utils import kebab_to_snake, snake_to_kebab


def validate_query(query_name: str, log=lambda _: None) -> bool:
    file_name = kebab_to_snake(query_name)
    path = os.path.join(PATH_QUERY_FOLDER, f"{file_name}.sql") # query name must match file name
    if not os.path.exists(path): 
        log(f"Unknown query name {query_name}. Aborting. Available queries: {get_query_names_list()}")
        return False
    
    # NOTE could add validation of the query syntax here
    return True

def get_query_names_list() -> list[str]:
    return [
        snake_to_kebab(file.replace(".sql", ""))
        for file in os.listdir(PATH_QUERY_FOLDER)
        if file.endswith(".sql")
    ]

def load_query(query_name: str) -> str:
    file_name = kebab_to_snake(query_name)
    path = os.path.join(PATH_QUERY_FOLDER, f"{file_name}.sql") # query name must match file name  
    with open(path, "r") as file:
        return file.read()


def run_query(query_name: str) -> None:
    connection = connections.get_initialized_connection()
    try: # TODO refactor all services to this more defensive pattern
        sql_query = load_query(query_name)
        return repository.execute_query(connection, sql_query)
    finally:
        connection.close()