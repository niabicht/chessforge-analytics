import pytest
import psycopg2

import chessforge.database.connections as connections
import chessforge.database.repository as repository
import chessforge.services.ingestion_service as ingestion_service
import chessforge.services.dataset_service as dataset_service
from chessforge.utils.utils import get_example_dataset_name


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def db_connection():
    """
    Opens a real database connection for the duration of this test module.
    Requires a running PostgreSQL instance reachable via the DB_* environment
    variables (set in docker-compose.yml for local use, ci.yml for CI).
    """
    with connections.InitializedConnection() as connection:
        yield connection


@pytest.fixture(scope="module", autouse=True)
def clean_example_dataset(db_connection):
    """
    Ensures the example dataset is removed before and after the test module runs.
    - Before: so the ingestion test starts from a clean state even if a previous
      test run left data behind.
    - After: so integration tests don't pollute the database for subsequent runs.
    """
    dataset_name = get_example_dataset_name()
    repository.delete_dataset(db_connection, dataset_name)
    db_connection.commit()

    yield  # tests run here

    repository.delete_dataset(db_connection, dataset_name)
    db_connection.commit()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestIngestionPipeline:

    def test_example_file_ingests_without_error(self):
        """Full ingestion of the example file should complete without raising."""
        is_success = ingestion_service.ingest_file(ingest_example=True, month=None)
        assert is_success is True

    def test_dataset_is_registered_after_ingestion(self, db_connection):
        """After ingestion, the dataset should appear in the datasets table."""
        dataset_name = get_example_dataset_name()
        exists = repository.does_dataset_exist(db_connection, dataset_name)
        assert exists is True

    def test_game_count_is_greater_than_zero(self, db_connection):
        """The games table should contain rows after ingestion."""
        with db_connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM games;")
            count = cursor.fetchone()[0]
        assert count > 0

    def test_game_count_matches_dataset_record(self, db_connection):
        """The game_count stored in the datasets table should match the actual row count."""
        dataset_name = get_example_dataset_name()
    
        with db_connection.cursor() as cursor:
            cursor.execute(
                "SELECT id, game_count FROM datasets WHERE name = %s;",
                (dataset_name,)
            )
            row = cursor.fetchone()
            dataset_id, recorded_count = row[0], row[1]
    
        with db_connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM games WHERE dataset_id = %s;",
                (dataset_id,)
            )
            actual_count = cursor.fetchone()[0]
    
        assert recorded_count == actual_count

    def test_ingesting_same_file_twice_is_rejected(self):
        """Re-ingesting an already ingested file should fail validation, not silently duplicate data."""
        is_valid = ingestion_service.validate_ingestion(ingest_example=True, month=None)
        assert is_valid is False

    def test_games_have_expected_fields(self, db_connection):
        """Spot-check that ingested rows contain non-null values for key columns."""
        with db_connection.cursor() as cursor:
            cursor.execute("""
                SELECT result, eco, time_control
                FROM games
                LIMIT 10;
            """)
            rows = cursor.fetchall()

        assert len(rows) > 0
        for result, eco, time_control in rows:
            assert result is not None, "result should not be null"
            assert eco is not None, "eco should not be null" # TODO or can openings sometimes be so weird that they don't get an ECO?
            assert time_control is not None, "time_control should not be null"
