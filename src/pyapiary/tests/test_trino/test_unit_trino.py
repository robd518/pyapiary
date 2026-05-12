import pytest
from unittest.mock import MagicMock, patch
from pyapiary.dbms_connectors.trino import TrinoConnector


@pytest.fixture
def mock_connect(mocker):
    """Patch trino.dbapi.connect and return the mock connection."""
    mock_conn = MagicMock()
    mocker.patch("pyapiary.dbms_connectors.trino.connect", return_value=mock_conn)
    return mock_conn


@pytest.fixture
def connector(mock_connect):
    """Return a TrinoConnector backed by a mocked connection."""
    return TrinoConnector(
        host="trino.trashcollector.dev",
        port=443,
        user="test_user",
        catalog="hive",
        schema="default",
        http_scheme="https",
    )


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------

class TestInit:
    def test_connect_called_with_provided_kwargs(self, mocker):
        mock_connect = mocker.patch("pyapiary.dbms_connectors.trino.connect")
        TrinoConnector(host="myhost", port=443, user="alice", http_scheme="https")
        mock_connect.assert_called_once_with(
            host="myhost",
            port=443,
            user="alice",
            http_scheme="https",
        )

    def test_connect_called_with_minimal_kwargs(self, mocker):
        mock_connect = mocker.patch("pyapiary.dbms_connectors.trino.connect")
        TrinoConnector(host="myhost", port=8080, user="alice")
        mock_connect.assert_called_once_with(host="myhost", port=8080, user="alice")

    def test_conn_attribute_set(self, mock_connect, connector):
        assert connector.conn is mock_connect

    def test_arbitrary_kwargs_forwarded(self, mocker):
        """Any kwarg the trino client supports should be forwarded as-is."""
        mock_connect = mocker.patch("pyapiary.dbms_connectors.trino.connect")
        TrinoConnector(host="h", port=443, user="u", http_scheme="https",
                       verify=False, session_properties={"query_max_run_time": "1h"})
        _, call_kwargs = mock_connect.call_args
        assert call_kwargs["verify"] is False
        assert call_kwargs["session_properties"] == {"query_max_run_time": "1h"}


# ---------------------------------------------------------------------------
# query()
# ---------------------------------------------------------------------------

class TestQuery:
    def test_returns_rows_when_description_present(self, mock_connect, connector):
        mock_cursor = MagicMock()
        mock_cursor.description = [("col1",), ("col2",)]
        mock_cursor.fetchall.return_value = [(1, "a"), (2, "b")]
        mock_connect.cursor.return_value.__enter__.return_value = mock_cursor

        result = connector.query("SELECT * FROM foo")

        mock_cursor.execute.assert_called_once_with("SELECT * FROM foo")
        mock_cursor.fetchall.assert_called_once()
        assert result == [(1, "a"), (2, "b")]

    def test_returns_none_when_no_description(self, mock_connect, connector):
        mock_cursor = MagicMock()
        mock_cursor.description = None
        mock_connect.cursor.return_value.__enter__.return_value = mock_cursor

        result = connector.query("CREATE TABLE foo (id INT)")

        mock_cursor.execute.assert_called_once_with("CREATE TABLE foo (id INT)")
        mock_cursor.fetchall.assert_not_called()
        assert result is None

    def test_returns_empty_list_for_empty_result_set(self, mock_connect, connector):
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",)]
        mock_cursor.fetchall.return_value = []
        mock_connect.cursor.return_value.__enter__.return_value = mock_cursor

        result = connector.query("SELECT * FROM empty_table")

        assert result == []

    def test_cursor_used_as_context_manager(self, mock_connect, connector):
        mock_cursor = MagicMock()
        mock_cursor.description = None
        mock_connect.cursor.return_value.__enter__.return_value = mock_cursor

        connector.query("SELECT 1")

        mock_connect.cursor.return_value.__enter__.assert_called_once()
        mock_connect.cursor.return_value.__exit__.assert_called_once()

    def test_propagates_execute_exception(self, mock_connect, connector):
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = RuntimeError("syntax error")
        mock_connect.cursor.return_value.__enter__.return_value = mock_cursor

        with pytest.raises(RuntimeError, match="syntax error"):
            connector.query("SELECT bad %%")

    def test_show_catalogs(self, mock_connect, connector):
        mock_cursor = MagicMock()
        mock_cursor.description = [("Catalog",)]
        mock_cursor.fetchall.return_value = [("hive",), ("iceberg",), ("tpch",)]
        mock_connect.cursor.return_value.__enter__.return_value = mock_cursor

        result = connector.query("SHOW CATALOGS")

        assert result == [("hive",), ("iceberg",), ("tpch",)]


# ---------------------------------------------------------------------------
# bulk_insert()
# ---------------------------------------------------------------------------

class TestBulkInsert:
    def test_inserts_single_row(self, mock_connect, connector):
        mock_cursor = MagicMock()
        mock_connect.cursor.return_value.__enter__.return_value = mock_cursor

        result = connector.bulk_insert("my_table", [{"id": 1, "name": "alice"}])

        expected_query = "INSERT INTO my_table (id, name) VALUES (?, ?)"
        mock_cursor.executemany.assert_called_once_with(expected_query, [(1, "alice")])
        assert result is True

    def test_inserts_multiple_rows(self, mock_connect, connector):
        mock_cursor = MagicMock()
        mock_connect.cursor.return_value.__enter__.return_value = mock_cursor

        data = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}, {"id": 3, "val": "c"}]
        result = connector.bulk_insert("my_table", data)

        actual_values = mock_cursor.executemany.call_args[0][1]
        assert actual_values == [(1, "a"), (2, "b"), (3, "c")]
        assert result is True

    def test_returns_none_for_empty_list(self, mock_connect, connector, capsys):
        result = connector.bulk_insert("my_table", [])

        assert result is None
        captured = capsys.readouterr()
        assert "Invalid" in captured.out

    def test_empty_list_does_not_touch_cursor(self, mock_connect, connector):
        mock_cursor = MagicMock()
        mock_connect.cursor.return_value.__enter__.return_value = mock_cursor

        connector.bulk_insert("my_table", [])

        mock_cursor.executemany.assert_not_called()

    def test_query_string_uses_correct_table_name(self, mock_connect, connector):
        mock_cursor = MagicMock()
        mock_connect.cursor.return_value.__enter__.return_value = mock_cursor

        connector.bulk_insert("hive.default.target_table", [{"x": 99}])

        actual_query = mock_cursor.executemany.call_args[0][0]
        assert "hive.default.target_table" in actual_query

    def test_column_order_matches_first_row_keys(self, mock_connect, connector):
        mock_cursor = MagicMock()
        mock_connect.cursor.return_value.__enter__.return_value = mock_cursor

        data = [{"z": 3, "a": 1, "m": 2}]
        connector.bulk_insert("t", data)

        actual_query = mock_cursor.executemany.call_args[0][0]
        for col in ["z", "a", "m"]:
            assert col in actual_query

    def test_placeholder_count_matches_column_count(self, mock_connect, connector):
        mock_cursor = MagicMock()
        mock_connect.cursor.return_value.__enter__.return_value = mock_cursor

        data = [{"a": 1, "b": 2, "c": 3, "d": 4}]
        connector.bulk_insert("t", data)

        actual_query = mock_cursor.executemany.call_args[0][0]
        assert actual_query.count("?") == 4

    def test_propagates_executemany_exception(self, mock_connect, connector):
        mock_cursor = MagicMock()
        mock_cursor.executemany.side_effect = RuntimeError("DB write error")
        mock_connect.cursor.return_value.__enter__.return_value = mock_cursor

        with pytest.raises(RuntimeError, match="DB write error"):
            connector.bulk_insert("t", [{"id": 1}])

    def test_cursor_used_as_context_manager(self, mock_connect, connector):
        mock_cursor = MagicMock()
        mock_connect.cursor.return_value.__enter__.return_value = mock_cursor

        connector.bulk_insert("t", [{"id": 1}])

        mock_connect.cursor.return_value.__enter__.assert_called_once()
        mock_connect.cursor.return_value.__exit__.assert_called_once()