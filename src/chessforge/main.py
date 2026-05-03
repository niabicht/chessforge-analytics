import os
import requests
import typer
import zstandard as zstd

from tqdm import tqdm

from chessforge.data_loader import stream_pgn_zst, parse_game_string_into_dict
from chessforge.database import get_initialized_connection, flush_games_batch_into_database, dataset_exists, create_dataset, update_dataset_game_count, delete_dataset
from chessforge.global_constants import LICHESS_PGN_FILE_NAME_PREFIX, EXAMPLE_FILE_NAME, PATH_DATA_RAW, PATH_EXAMPLE_FILE
from chessforge.utils import build_lichess_name, echo_file_with_size, generate_past_months, is_lichess_pgn_file, remote_file_exists, reservoir_sample_from_stream


app = typer.Typer(help="Chessforge Analytics CLI")


@app.command()
def ingest(month: str):
    ingest_shared(month=month, example=False)

@app.command()
def ingest_example():
    ingest_shared(month=None, example=True)

def ingest_shared(month: str | None, example: bool):
    """Ingests a single month's PGN file into the database. If example=True, ingests the example dataset instead."""
    dataset_name = EXAMPLE_FILE_NAME if example else build_lichess_name(month)
    file_path = os.path.join(PATH_DATA_RAW, f"{dataset_name}.pgn.zst") if not example else PATH_EXAMPLE_FILE
    if not os.path.exists(file_path):
        typer.echo(f"File {file_path} not found. Returning.")
        return

    connection = get_initialized_connection()

    # check if dataset already exists
    if dataset_exists(connection, dataset_name):
        typer.echo(f"Dataset {dataset_name} already ingested. Returning.")
        connection.close()
        return

    # create dataset entry
    dataset_id = create_dataset(connection, dataset_name)

    batch_size = 500  # TODO adjust this    
    batch = []
    game_counter = 0

    for game_text in stream_pgn_zst(file_path):
        if game_counter >= 5000: break # TODO remove this, just for testing

        game = parse_game_string_into_dict(game_text)
        if game is None: continue

        batch.append(game)

        if len(batch) >= batch_size:
            flush_games_batch_into_database(connection, batch, dataset_id)
            game_counter += batch_size

    # flush remaining
    if batch:
        flush_games_batch_into_database(connection, batch, dataset_id)
        game_counter += len(batch)

    update_dataset_game_count(connection, dataset_id, game_counter)

    connection.close()

    typer.echo(f"Ingested {game_counter} games for {dataset_name}")


# TODO create the example file again and also remove it from gitignore
@app.command()
def create_example_file(
    input_file_path: str = typer.Option(
        "data/raw/lichess_db_standard_rated_2026-03.pgn.zst",
        help="Path to input .pgn.zst file"
    ),
    n: int = typer.Option(
        1000,
        help="Number of games to sample"
    ),
):
    """Creates a random sample of PGN games using reservoir sampling and writes them to a file."""

    typer.echo(f"Sampling {n} games from {input_file_path}...")

    stream = stream_pgn_zst(input_file_path)
    sampled_games = reservoir_sample_from_stream(stream, n)

    with open(PATH_EXAMPLE_FILE, "wb") as file:
        compressor = zstd.ZstdCompressor(level=20)
        with compressor.stream_writer(file) as writer:
            for game in sampled_games:
                writer.write((game + "\n\n").encode("utf-8"))

    typer.echo(f"Saved sample to {PATH_EXAMPLE_FILE}")


@app.command()
def datasets_list():
    connection = get_initialized_connection()

    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT name, game_count, ingested_at
            FROM datasets
            ORDER BY ingested_at DESC;
        """)
        rows = cursor.fetchall()

    connection.close()

    typer.echo("Datasets:" if rows else "Datasets: none")
    for name, count, timestamp in rows:
        typer.echo(f"{name:33} | {count:8} games | {timestamp:%Y-%m-%d %H:%M}")


@app.command()
def datasets_delete(month: str = None, all: bool = False, example: bool = False):
    connection = get_initialized_connection()

    if all:
        confirm = typer.confirm("Delete ALL datasets?")
        if not confirm: raise typer.Abort()

        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM games;")
            cursor.execute("DELETE FROM datasets;")
        connection.commit()
        typer.echo("All datasets deleted.")
    elif example:
        success = delete_dataset(connection, EXAMPLE_FILE_NAME)

        if success: typer.echo(f"Deleted example dataset")
        else: typer.echo(f"Example dataset not found")          
    else:
        if not month: raise typer.BadParameter("Provide --month or --all")

        dataset_name = build_lichess_name(month)
        success = delete_dataset(connection, dataset_name)

        if success: typer.echo(f"Deleted dataset {month}")
        else: typer.echo(f"Dataset {month} not found")

    connection.close()


@app.command()
def files_list():
    if not os.path.exists(PATH_DATA_RAW):
        typer.echo("No raw data directory.")
        return

    files = os.listdir(PATH_DATA_RAW)
    pgn_files = [f for f in files if is_lichess_pgn_file(f)]
    tmp_files = [f for f in files if f.endswith(".tmp")]

    typer.echo("Data files:")
    for file in pgn_files:
        echo_file_with_size(PATH_DATA_RAW, file)

    if tmp_files:
        typer.echo("\nIncomplete downloads:")
        for file in tmp_files:
            echo_file_with_size(PATH_DATA_RAW, file)


@app.command()
def files_delete(month: str = None, all: bool = False):
    if all:
        confirm = typer.confirm("Delete ALL raw files?")
        if not confirm: raise typer.Abort()

        for file in os.listdir(PATH_DATA_RAW):
            if is_lichess_pgn_file(file):
                os.remove(os.path.join(PATH_DATA_RAW, file))

        typer.echo("All files deleted.")
        return

    if not month: raise typer.BadParameter("Provide --month or --all")

    filename = build_lichess_name(month, use_file_extension=True)
    file_path = os.path.join(PATH_DATA_RAW, filename)

    if os.path.exists(file_path):
        os.remove(file_path)
        typer.echo(f"Deleted {filename}.")
    else:
        typer.echo(f"File {filename} not found.")


@app.command()
def files_cleanup():
    if all:
        confirm = typer.confirm("Delete all .tmp files?")
        if not confirm: raise typer.Abort()

        for file in os.listdir(PATH_DATA_RAW):
            if file.endswith(".tmp"):
                os.remove(os.path.join(PATH_DATA_RAW, file))

        typer.echo("All .tmp files deleted.")


@app.command()
def download(
    month: str = typer.Option(None),
    latest: bool = typer.Option(False),
):
    """Downloads a Lichess PGN dump."""

    if latest and month: raise typer.BadParameter("Use either --month or --latest, not both")
    if not latest and not month: raise typer.BadParameter("Provide --month or use --latest")

    if latest:
        # find the latest month by checking which files are available on the server
        for candidate in generate_past_months():
            filename = build_lichess_name(candidate, use_file_extension=True)
            url = f"https://database.lichess.org/standard/{filename}"
            typer.echo(f"Checking {candidate}...")
            if remote_file_exists(url): 
                month = candidate
                typer.echo(f"Latest dataset found: {month}")
                break

    # if still no month, it means we couldn't find any dataset in the last 12 months
    if not month:
        typer.BadParameter("No dataset found in the last 12 months")
    
    filename = build_lichess_name(month, use_file_extension=True)
    file_path = os.path.join(PATH_DATA_RAW, filename)
    tmp_path = file_path + ".tmp"

    os.makedirs(PATH_DATA_RAW, exist_ok=True)

    if os.path.exists(file_path):
        typer.echo(f"File {filename} already exists. Returning.")
        return

    url = f"https://database.lichess.org/standard/{filename}"
    typer.echo(f"Downloading from {url}")

    response = requests.get(url, stream=True)
    if response.status_code != 200:
        typer.echo(f"Failed to download file. Status: {response.status_code}")
        raise typer.Abort()

    file_size = response.headers.get("content-length")
    file_size = int(file_size) if file_size else None

    with open(tmp_path, "wb") as f:
        with tqdm(total=file_size, unit="B", unit_scale=True) as progress_bar:
            for chunk in response.iter_content(chunk_size=1048576): # 1 MiB
                if chunk:
                    f.write(chunk)
                    progress_bar.update(len(chunk))

    os.replace(tmp_path, file_path)

    typer.echo(f"Downloaded {filename}")


# NOTE these are just some preliminary queries for testing
@app.command()
def stats(stat_type: str):
    """Preliminary stats commands to query the database."""
    connection = get_initialized_connection()

    with connection.cursor() as cursor:
        if stat_type == "openings":
            cursor.execute("""
                SELECT opening, COUNT(*) 
                FROM games 
                GROUP BY opening 
                ORDER BY COUNT(*) DESC 
                LIMIT 10;
            """)

        elif stat_type == "ratings":
            cursor.execute("""
                SELECT AVG(white_elo), AVG(black_elo) FROM games;
            """)

        elif stat_type == "results":
            cursor.execute("""
                SELECT result, COUNT(*) 
                FROM games 
                GROUP BY result;
            """)

        elif stat_type == "number":
            cursor.execute("""
                SELECT COUNT(*) FROM games;
            """)

        elif stat_type == "debug":
            cursor.execute("""
                SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'games';
            """)

        else:
            raise typer.BadParameter("Unknown stats type")

        results = cursor.fetchall()

    connection.close()

    for row in results:
        typer.echo(row)


if __name__ == "__main__":
    app()