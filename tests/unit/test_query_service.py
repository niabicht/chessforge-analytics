import os
import pytest

from unittest.mock import patch

from chessforge.services.query_service import validate_query, get_query_names_list, load_query


class TestGetQueryNamesList:

    def test_returns_a_list(self):
        result = get_query_names_list()
        assert isinstance(result, list)

    def test_returns_kebab_case_names(self):
        """Query names exposed to the CLI should use kebab-case, not snake_case."""
        for name in get_query_names_list():
            assert "_" not in name, (
                f"Query name '{name}' should be kebab-case but contains underscores"
            )

    def test_known_queries_are_present(self):
        """Spot-check that expected queries exist. Update this list if queries are renamed."""
        known = {"opening-winrates", "common-openings", "ratings", "elo-diff-winrates"}
        result = set(get_query_names_list())
        for name in known:
            assert name in result, f"Expected query '{name}' not found in query list"


class TestValidateQuery:

    def test_valid_query_name_passes(self):
        # Use the first name from the real query list so this stays in sync automatically
        names = get_query_names_list()
        assert len(names) > 0, "No queries found. Is the queries folder present?"
        assert validate_query(names[0]) is True

    def test_invalid_query_name_fails(self):
        assert validate_query("this-query-does-not-exist") is False

    def test_invalid_query_logs_message(self):
        """validate_query should call the log callback with a helpful message on failure."""
        messages = []
        validate_query("nonexistent-query", log=messages.append)
        assert len(messages) == 1
        assert "nonexistent-query" in messages[0]

    def test_valid_query_does_not_log_error(self):
        names = get_query_names_list()
        messages = []
        validate_query(names[0], log=messages.append)
        assert len(messages) == 0


class TestLoadQuery:

    def test_load_query_returns_string(self):
        names = get_query_names_list()
        query = load_query(names[0])
        assert isinstance(query, str)
        assert len(query) > 0

    def test_loaded_query_contains_select(self):
        """Every predefined query should be a SELECT statement."""
        for name in get_query_names_list():
            sql = load_query(name)
            assert "SELECT" in sql.upper(), (
                f"Query '{name}' does not appear to contain a SELECT statement"
            )
