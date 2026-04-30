import typer

from chessforge.data_loader import stream_pgn_zst, parse_game_string_into_dict
from chessforge.database import connect_to_database_or_wait, initialize_database, flush_games_batch_into_database


app = typer.Typer(help="Chessforge Analytics CLI")


@app.command()
def ingest(file_path: str = "data/raw/lichess_db_standard_rated_2026-03.pgn.zst"):
    """Ingest a PGN file into the database."""
    database_connection = connect_to_database_or_wait()
    initialize_database(database_connection)

    batch_size = 40  # TODO adjust this
    batch = []

    for i, game_text in enumerate(stream_pgn_zst(file_path)):
        if i >= 100: break # TODO remove this, just for testing
        game = parse_game_string_into_dict(game_text)
        if game is None: continue

        batch.append(game)

        if len(batch) >= batch_size:
            flush_games_batch_into_database(database_connection, batch)

     # flush remaining
    if batch:
        flush_games_batch_into_database(database_connection, batch)

    database_connection.close()


@app.command()
def reset():
    """Resets database."""
    confirm = typer.confirm("This will delete all data. Continue?")
    if not confirm: raise typer.Abort()

    connection = connect_to_database_or_wait()
    with connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS games;")
    connection.commit()
    connection.close()

    typer.echo("Database reset complete.")


@app.command()
def stats(stat_type: str):
    """Preliminary stats commands to query the database."""
    connection = connect_to_database_or_wait()

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

        else:
            raise typer.BadParameter("Unknown stats type")

        results = cursor.fetchall()

    connection.close()

    for row in results:
        typer.echo(row)


if __name__ == "__main__":
    app()