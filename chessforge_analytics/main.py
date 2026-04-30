from chessforge_analytics.data_loader import stream_pgn_zst, parse_game_string_into_dict
from chessforge_analytics.database import connect_or_wait, initialize_database, flush_games_batch


def main():
    connection = connect_or_wait()
    initialize_database(connection)

    path = "data/raw/lichess_db_standard_rated_2026-03.pgn.zst"
    
    batch_size = 200
    batch = []
    for i, game_text in enumerate(stream_pgn_zst(path)):
        if i >= 100: break # TODO remove this, just for testing
        game = parse_game_string_into_dict(game_text)
        if game is None: continue

        batch.append(game)

        if len(batch) >= batch_size:
            flush_games_batch(connection, batch)

    # flush remaining
    if batch:
        flush_games_batch(connection, batch)

    connection.close()

if __name__ == "__main__":
    main()