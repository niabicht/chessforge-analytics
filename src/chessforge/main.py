import os
import typer
import zstandard as zstd

from chessforge.data_loader import stream_pgn_zst, parse_game_string_into_dict
from chessforge.database import get_initialized_connection, flush_games_batch_into_database, dataset_exists, create_dataset, update_dataset_game_count, delete_dataset
from chessforge.global_constants import LICHESS_PGN_FILE_NAME_PREFIX, EXAMPLE_FILE_NAME, PATH_DATA_RAW, PATH_EXAMPLE_FILE
from chessforge.utils import is_lichess_pgn_file, reservoir_sample_from_stream


app = typer.Typer(help="Chessforge Analytics CLI")


@app.command()
def ingest(month: str):
    ingest_shared(month=month, example=False)

@app.command()
def ingest_example():
    ingest_shared(month=None, example=True)

def ingest_shared(month: str | None, example: bool):
    """Ingests a single month's PGN file into the database. If example=True, ingests the example dataset instead."""
    dataset_name = "example_games" if example else f"{LICHESS_PGN_FILE_NAME_PREFIX}_{month}"
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


@app.command()
def create_example_file(
    input_file_path: str = typer.Option(
        "data/raw/lichess_db_standard_rated_2026-03.pgn.zst",
        help="Path to input .pgn.zst file"
    ),
    n: int = typer.Option(
        1000,
        "--n", "-n",
        help="Number of games to sample"
    ),
):
    """
    Creates a random sample of PGN games using reservoir sampling
    and writes them to a file.
    """

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

        dataset_name = f"{LICHESS_PGN_FILE_NAME_PREFIX}_{month}"
        success = delete_dataset(connection, dataset_name)

        if success: typer.echo(f"Deleted dataset {month}")
        else: typer.echo(f"Dataset {month} not found")

    connection.close()


@app.command()
def files_list():
    if not os.path.exists(PATH_DATA_RAW):
        typer.echo("No raw data directory.")
        return

    files = [file for file in os.listdir(PATH_DATA_RAW) if is_lichess_pgn_file(file)]
    typer.echo("Files:" if files else "Files: none")
    for file in os.listdir(PATH_DATA_RAW):
        size = os.path.getsize(os.path.join(PATH_DATA_RAW, file)) / (1024**3)
        typer.echo(f"{file} | {size:.2f} GiB")


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

    filename = f"{LICHESS_PGN_FILE_NAME_PREFIX}_{month}.pgn.zst"
    path = os.path.join(PATH_DATA_RAW, filename)

    if os.path.exists(path):
        os.remove(path)
        typer.echo(f"Deleted {filename}.")
    else:
        typer.echo(f"File {filename} not found.")


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