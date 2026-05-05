import typer
from tqdm import tqdm

from chessforge.utils.utils import get_path_example_file, get_example_dataset_name, get_path_lichess_file, build_lichess_name
import chessforge.services.input_file_service as input_file_service
import chessforge.services.ingestion_service as ingestion_service
import chessforge.services.dataset_service as dataset_service


app = typer.Typer(help="Chessforge Analytics CLI")

###########
### Helpers
###########

def confirm(question: str):
    if typer.confirm(question): return True
    else:
        typer.echo("Aborting.")
        return False

def require_exactly_one(*flags: bool) -> bool:
    return (sum(bool(flag) for flag in flags) == 1)


###############
### Input files
###############

@app.command()
# User does not need to use this.
@app.command()
def create_example_file(n: int = typer.Option(1000, help="Number of games to sample from the dataset")):
    if n > 10000 and not confirm(f"All {n} games will be loaded into memory simultaniously. Are you sure you want to proceed?"): return
    input_file_service.create_example_file(n, log=typer.echo)

@app.command()
def files_list():
    files_list_string = input_file_service.get_files_list_string()
    typer.echo(files_list_string)

    if "download" in files_list_string and ".tmp" in files_list_string and confirm("Delete all incomplete downloads?"):
        input_file_service.delete_incomplete_downloads(log=typer.echo)

@app.command()
def download(
    month: str = typer.Option(None, help="Month of the Lichess dump in format YYYY-MM"),
    latest: bool = typer.Option(False, help="Automatically download the latest available dataset"),
):
    if not require_exactly_one(month, latest):
        typer.echo("Use exactly one flag, either --month or --latest. Aborting.")
        return

    progress_bar = None
    def on_progress(chunk_size: int, total_size: int):
        nonlocal progress_bar
        if not progress_bar: progress_bar = tqdm(total=total_size, unit="B", unit_scale=True) # Lazy initialize when we know the total size. This IS VERY clean.
        progress_bar.update(chunk_size)

    success = input_file_service.download(month=month, download_latest=latest, log=typer.echo, on_progress=on_progress)
    if progress_bar: progress_bar.close()
    if success: typer.echo("Download completed.")

@app.command()
def files_delete(
    all: bool = typer.Option(False, help="Delete all downloaded Lichess files"),
    month: str = typer.Option(None, help="Delete a specific monthly dataset (YYYY-MM)"),
):
    if not require_exactly_one(all, month):
        typer.echo("Use exactly one flag, either --all or --month. Aborting.")
        return

    if all and not confirm("Delete ALL lichess files?"): return

    input_file_service.files_delete(all=all, month=month, log=typer.echo)  


############
### Datasets
############

@app.command()
def datasets_list():
    datasets_list_string = dataset_service.get_datasets_list_string()
    typer.echo(datasets_list_string)   

@app.command()
def ingest(
    example: bool = typer.Option(False, help="Ingest the example dataset instead of a monthly dump"),
    month: str = typer.Option(None, help="Month to ingest (YYYY-MM)"),
):
    if not require_exactly_one(month, example):
        typer.echo("Use exactly one flag, either --example or --month. Aborting.")
        return
    
    file_path = get_path_example_file() if example else get_path_lichess_file(month)
    is_valid, error_message = ingestion_service.validate_ingestion(file_path)
    if not is_valid:
        typer.echo(f"{error_message} Aborting.")
        return  
    
    progress = tqdm(unit="B", unit_scale=True)
    def on_progress(bytes_progress: int, n_games_total: int):
        progress.update(bytes_progress)
        progress.set_postfix(games=n_games_total)

    ingestion_service.ingest_file(file_path, on_progress=on_progress)

    progress.close()
    typer.echo(f"Succesfully ingested from {file_path}.")

@app.command()
def datasets_delete(
    all: bool = typer.Option(False, help="Delete all datasets"),
    example: bool = typer.Option(False, help="Delete the example dataset"),
    month: str = typer.Option(None, help="Delete dataset for a specific month (YYYY-MM)"),
):
    if not require_exactly_one(all, example, month):
        typer.echo("Use exactly one flag, either --all, --example or --month. Aborting.")
        return

    if all:
        if not confirm("Delete all datasets?"): return
        dataset_service.datasets_delete(all=True, log=typer.echo)      
    else:
        dataset_name = get_example_dataset_name() if example else build_lichess_name(month)             
        dataset_service.datasets_delete(dataset_name=dataset_name, log=typer.echo)    

 
#############
### SQL query
#############


# # NOTE these are just some preliminary queries for testing
# # will be replaced later by maybe LLM interface or at least be strongly refactored anyway
# @app.command()
# def stats(stat_type: str):
#     """Preliminary stats commands to query the database."""
#     connection = get_initialized_connection()
# 
#     with connection.cursor() as cursor:
#         if stat_type == "openings":
#             cursor.execute("""
#                 SELECT opening, COUNT(*) 
#                 FROM games 
#                 GROUP BY opening 
#                 ORDER BY COUNT(*) DESC 
#                 LIMIT 10;
#             """)
# 
#         elif stat_type == "ratings":
#             cursor.execute("""
#                 SELECT AVG(white_elo), AVG(black_elo) FROM games;
#             """)
# 
#         elif stat_type == "results":
#             cursor.execute("""
#                 SELECT result, COUNT(*) 
#                 FROM games 
#                 GROUP BY result;
#             """)
# 
#         elif stat_type == "number":
#             cursor.execute("""
#                 SELECT COUNT(*) FROM games;
#             """)
# 
#         elif stat_type == "debug":
#             cursor.execute("""
#                 SELECT column_name
#                     FROM information_schema.columns
#                     WHERE table_name = 'games';
#             """)
# 
#         else:
#             raise typer.BadParameter("Unknown stats type")
# 
#         results = cursor.fetchall()
# 
#     connection.close()
# 
#     for row in results:
#         typer.echo(row)


if __name__ == "__main__":
    app()