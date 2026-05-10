import os

import chessforge.database.connections as connections
import chessforge.database.repository as repository
from chessforge.utils.global_constants import PATH_QUERY_DIR
from chessforge.utils.utils import kebab_to_snake, snake_to_kebab


def validate_query(query_name: str, log=lambda message: None) -> bool:
    file_name = kebab_to_snake(query_name)
    path = os.path.join(PATH_QUERY_DIR, f"{file_name}.sql") # query name must match file name
    if not os.path.exists(path): 
        log(f"Unknown query name {query_name}. Available queries: {get_query_names_list()}")
        return False
    
    # NOTE could add validation of the query syntax here
    return True


# NOTE Exception to the rule that service functions return is_success bool
# Makes it easier to use it for cli typer help
def get_query_names_list() -> list[str]:
    return [
        snake_to_kebab(file.replace(".sql", ""))
        for file in os.listdir(PATH_QUERY_DIR)
        if file.endswith(".sql")
    ]


# NOTE Exception to the rule that service functions return is_success bool
# Makes it easier to have it separate from run_query function, which I wanted in case I later need it for LLM interface or whatever
def load_query(query_name: str) -> str:
    file_name = kebab_to_snake(query_name)
    path = os.path.join(PATH_QUERY_DIR, f"{file_name}.sql") # query name must match file name  
    with open(path, "r") as file:
        return file.read()


def run_query(query_name: str, on_result=lambda result: None) -> bool:
    with connections.InitializedConnection() as connection:
        sql_query = load_query(query_name)
        result = repository.execute_query_return_result(connection, sql_query)
        on_result(result)
    
    return True