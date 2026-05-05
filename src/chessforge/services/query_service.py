import os

from chessforge.utils.global_constants import PATH_QUERY_FOLDER
from chessforge.utils.utils import kebab_to_snake, snake_to_kebab
from chessforge.database.connection import get_initialized_connection
from chessforge.database.repository import execute_query


def validate_query(query_name: str, log=lambda _: None) -> bool:
    file_name = kebab_to_snake(query_name)
    path = os.path.join(PATH_QUERY_FOLDER, f"{file_name}.sql") # query name must match file name
    print(path)
    if not os.path.exists(path): 
        log(f"Unknown query name {query_name}. Aborting. Available queries: {list_query_names()}")
        return False
    
    # NOTE could add validation of the query syntax here
    return True

def list_query_names():
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


def run_query(query_name: str):
    connection = get_initialized_connection()
    try: # TODO refactor all services to this more defensive pattern
        sql_query = load_query(query_name)
        return execute_query(connection, sql_query)
    finally:
        connection.close()