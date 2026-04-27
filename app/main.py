import os
import time
import psycopg2


def connect():
    return psycopg2.connect(
        host="database",
        database="game_database",
        user="test",
        password="test"
    )


def main():
    print("Starting app...")

    connection = None
    for i in range(10):
        try:
            connection = connect()
            break
        except Exception:
            print("DB not ready yet, retrying...")
            time.sleep(2)

    if connection is None:
        raise Exception("Could not connect to database")

    print("Connected to DB")

    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id SERIAL PRIMARY KEY,
        
            white_elo INT,
            black_elo INT,
        
            result TEXT,              -- white_win / black_win / draw
            played_as TEXT,          -- white / black (optional später)
            
            opening_name TEXT,
            eco_code TEXT,
        
            time_control TEXT,       -- e.g. 600+0
            time_class TEXT          -- blitz / rapid / bullet
        );
    """)

    cursor.execute("""
        INSERT INTO games (
            white_elo,
            black_elo,
            result,
            played_as,
            opening_name,
            eco_code,
            time_control,
            time_class
        )
        VALUES (
            1600,
            1500,
            'white_win',
            'white',
            'Sicilian Defense',
            'B20',
            '600+0',
            'rapid'
        );
    """)

    connection.commit()

    cursor.close()
    connection.close()

    print("Table created and test row inserted")


if __name__ == "__main__":
    main()