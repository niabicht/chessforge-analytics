import typer
from tqdm import tqdm

import chessforge.services.dataset_service as dataset_service
import chessforge.services.ingestion_service as ingestion_service
import chessforge.services.input_file_service as input_file_service
import chessforge.services.ml_service as ml_service
import chessforge.services.query_service as query_service


app = typer.Typer(help="Chessforge Analytics CLI")



###########
### Helpers
###########

def is_exactly_one_flag(*flags: bool) -> bool:
    return (sum(bool(flag) for flag in flags) == 1)


def confirm(question: str) -> bool:
    if typer.confirm(question): return True
    else:
        typer.echo("Aborting.")
        return False


def handle_service_result(is_success: bool, message_success: str = "", message_fail: str = "Aborting.") -> None:
    # NOTE can expand later, maybe also do error code ints
    typer.echo(message_success) if is_success else typer.echo(message_fail)



###############
### Input files
###############

# User does not need to use create_example_file() but he can of course.
@app.command()
def create_example_file(n: int = typer.Option(1000, help="Number of games to sample from the dataset")):
    if n > 10000 and not confirm(f"All {n} games will be loaded into memory simultaneously. Are you sure you want to proceed?"): return

    progress_log = tqdm(unit="B", unit_scale=True)
    def on_progress(progress: int):
        progress_log.update(progress)

    is_success = input_file_service.create_example_file(n, log=typer.echo, on_progress=on_progress, on_done=lambda: progress_log.close()) 
    handle_service_result(is_success)


@app.command()
def list_files():
    def on_all_files(has_incomplete_download: bool):
        if has_incomplete_download and confirm("Delete all incomplete downloads?"):
            input_file_service.delete_incomplete_downloads(log=typer.echo)

    success = input_file_service.list_files(log=typer.echo, on_all_files=on_all_files)
    handle_service_result(success)
        

@app.command()
def download(
    month: str = typer.Option(None, help="Month of the Lichess dump in format YYYY-MM"),
    latest: bool = typer.Option(False, help="Automatically download the latest available dataset"),
):
    if not is_exactly_one_flag(month, latest):
        typer.echo("Use exactly one flag, either --month or --latest. Aborting.")
        return

    progress_bar = None
    def on_progress(progress: int, total_size: int):
        nonlocal progress_bar
        if not progress_bar: progress_bar = tqdm(total=total_size, unit="B", unit_scale=True) # Lazy initialize when we know the total size. This IS VERY clean.
        progress_bar.update(progress)

    is_success = input_file_service.download(month=month, download_latest=latest, log=typer.echo, on_progress=on_progress, on_done=lambda: progress_bar.close())
    if progress_bar: progress_bar.close()
    handle_service_result(is_success)


@app.command()
def delete_file(
    all: bool = typer.Option(False, help="Delete all downloaded Lichess files"),
    month: str = typer.Option(None, help="Delete a specific monthly dataset (YYYY-MM)"),
):
    if not is_exactly_one_flag(all, month):
        typer.echo("Use exactly one flag, either --all or --month. Aborting.")
        return

    if all and not confirm("Delete ALL Lichess files?"): return

    is_success = input_file_service.delete_files(all=all, month=month, log=typer.echo)  
    handle_service_result(is_success)



############
### Datasets
############

@app.command()
def list_datasets():
    is_success = dataset_service.log_datasets(log=typer.echo)
    handle_service_result(is_success)


@app.command()
def ingest(
    example: bool = typer.Option(False, help="Ingest the example dataset instead of a monthly dump"),
    month: str = typer.Option(None, help="Month to ingest (YYYY-MM)"),
):
    if not is_exactly_one_flag(month, example):
        typer.echo("Use exactly one flag, either --example or --month. Aborting.")
        return
    
    if not ingestion_service.validate_ingestion(example, month, log=typer.echo):
        handle_service_result(is_success=False)
        return
        
    progress_log = tqdm(unit="B", unit_scale=True)
    def on_progress(progress: int, n_games_total: int):
        progress_log.update(progress)
        progress_log.set_postfix(games=n_games_total)

    is_success = ingestion_service.ingest_file(example, month, on_progress=on_progress, on_done=lambda: progress_log.close())

    progress_log.close()
    handle_service_result(is_success)


@app.command()
def delete_dataset(
    all: bool = typer.Option(False, help="Delete all datasets"),
    example: bool = typer.Option(False, help="Delete the example dataset"),
    month: str = typer.Option(None, help="Delete dataset for a specific month (YYYY-MM)"),
):
    if not is_exactly_one_flag(all, example, month):
        typer.echo("Use exactly one flag, either --all, --example or --month. Aborting.")
        return
    
    if all and not confirm("Delete all datasets?"): return

    is_success = dataset_service.delete_dataset(all, example, month, log=typer.echo)  
    handle_service_result(is_success)


 
#############
### SQL query
#############

@app.command()
def query(name: str = typer.Option(..., help=f"Name of predefined query. Available queries: {query_service.get_query_names_list()}")): # NOTE this message is evaluated at compile time. If later queries are added during run time, maybe instead do something generic like "Use `queries-list` to see options." with a corresponding separate cli command
    if not query_service.validate_query(name, log=typer.echo): 
        handle_service_result(False)
        return

    is_success = query_service.run_query(name, on_result=typer.echo)
    handle_service_result(is_success)



############
### Datasets
############

@app.command()
def debug():
    ml_service.debug()



if __name__ == "__main__":
    app()