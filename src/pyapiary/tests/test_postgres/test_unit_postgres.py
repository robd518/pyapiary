import sys
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

# Mock psycopg_pool before importing the module so tests run even when the
# driver is not installed in the environment.
sys.modules.setdefault("psycopg_pool", MagicMock())

from pyapiary.dbms_connectors.postgres import PostgresConnector, AsyncPostgresConnector


# ──────────────────────────────────────────────
#  Sync PostgresConnector
# ──────────────────────────────────────────────

@pytest.fixture
def pg():
    with patch("pyapiary.dbms_connectors.postgres.ConnectionPool"):
        yield PostgresConnector("postgresql://user:pass@localhost/testdb")


class TestPostgresConnectorInit:
    @patch("pyapiary.dbms_connectors.postgres.ConnectionPool")
    def test_creates_pool_with_defaults(self, mock_pool):
        conn = PostgresConnector("postgresql://localhost/db")
        mock_pool.assert_called_once_with(
            "postgresql://localhost/db",
            kwargs={"autocommit": True},
            min_size=5,
            max_size=30,
        )
        assert conn.dsn == "postgresql://localhost/db"
        assert conn.min_size == 5
        assert conn.max_size == 30

    @patch("pyapiary.dbms_connectors.postgres.ConnectionPool")
    def test_creates_pool_with_custom_sizes(self, mock_pool):
        conn = PostgresConnector("dsn", min_size=1, max_size=10)
        mock_pool.assert_called_once_with(
            "dsn",
            kwargs={"autocommit": True},
            min_size=1,
            max_size=10,
        )

    @patch("pyapiary.dbms_connectors.postgres.ConnectionPool")
    def test_accepts_custom_logger(self, mock_pool):
        logger = MagicMock()
        conn = PostgresConnector("dsn", logger=logger)
        assert conn.logger is logger

    @patch("pyapiary.dbms_connectors.postgres.ConnectionPool")
    def test_uses_default_logger_when_none(self, mock_pool):
        conn = PostgresConnector("dsn")
        assert conn.logger is not None


class TestPostgresConnectorContextManager:
    def test_enter_returns_self(self, pg):
        assert pg.__enter__() is pg

    def test_exit_closes_pool(self, pg):
        pg.__exit__(None, None, None)
        pg.connection_pool.close.assert_called_once()


class TestPostgresConnectorClose:
    def test_close_closes_pool(self, pg):
        pg.close()
        pg.connection_pool.close.assert_called_once()

    def test_close_when_pool_is_none(self, pg):
        pg.connection_pool = None
        pg.close()  # should not raise


class TestPostgresConnectorQuery:
    def test_query_returns_results(self, pg):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [("row1",), ("row2",)]
        pg.connection_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        pg.connection_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.transaction.return_value.__enter__ = MagicMock()
        mock_conn.transaction.return_value.__exit__ = MagicMock(return_value=False)

        result = pg.query("SELECT 1")
        assert result == [("row1",), ("row2",)]
        mock_conn.execute.assert_called_once_with("SELECT 1", None)

    def test_query_passes_params(self, pg):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        pg.connection_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        pg.connection_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.transaction.return_value.__enter__ = MagicMock()
        mock_conn.transaction.return_value.__exit__ = MagicMock(return_value=False)

        pg.query("SELECT * FROM t WHERE id = %s", (42,))
        mock_conn.execute.assert_called_once_with("SELECT * FROM t WHERE id = %s", (42,))


class TestPostgresConnectorBulkInsert:
    def test_empty_data_returns_immediately(self, pg):
        pg.bulk_insert("my_table", [])
        pg.connection_pool.connection.assert_not_called()

    def test_bulk_insert_calls_copy(self, pg):
        mock_copy = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.copy.return_value.__enter__ = MagicMock(return_value=mock_copy)
        mock_cursor.copy.return_value.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        pg.connection_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        pg.connection_pool.connection.return_value.__exit__ = MagicMock(return_value=False)

        data = [{"name": "alice", "age": 30}, {"name": "bob", "age": 25}]
        pg.bulk_insert("users", data)

        mock_cursor.copy.assert_called_once_with("COPY users (name, age) FROM STDIN")
        assert mock_copy.write_row.call_count == 2
        mock_copy.write_row.assert_any_call(("alice", 30))
        mock_copy.write_row.assert_any_call(("bob", 25))


class TestPostgresConnectorLog:
    def test_log_default_level(self, pg):
        pg.logger = MagicMock()
        pg._log("test message")
        pg.logger.info.assert_called_once_with("test message")

    def test_log_custom_level(self, pg):
        pg.logger = MagicMock()
        pg._log("warning msg", level="warning")
        pg.logger.warning.assert_called_once_with("warning msg")

    def test_log_falls_back_to_info(self, pg):
        pg.logger = MagicMock(spec=["info"])
        pg.logger.info = MagicMock()
        pg._log("msg", level="nonexistent")
        pg.logger.info.assert_called_once_with("msg")


# ──────────────────────────────────────────────
#  Async AsyncPostgresConnector
# ──────────────────────────────────────────────

@pytest.fixture
def async_pg():
    with patch("pyapiary.dbms_connectors.postgres.AsyncConnectionPool"):
        yield AsyncPostgresConnector("postgresql://user:pass@localhost/testdb")


class TestAsyncPostgresConnectorInit:
    @patch("pyapiary.dbms_connectors.postgres.AsyncConnectionPool")
    def test_creates_pool_with_defaults(self, mock_pool):
        conn = AsyncPostgresConnector("postgresql://localhost/db")
        mock_pool.assert_called_once_with(
            "postgresql://localhost/db",
            kwargs={"autocommit": True},
            min_size=5,
            max_size=30,
            open=False,
        )
        assert conn.dsn == "postgresql://localhost/db"

    @patch("pyapiary.dbms_connectors.postgres.AsyncConnectionPool")
    def test_creates_pool_with_custom_sizes(self, mock_pool):
        conn = AsyncPostgresConnector("dsn", min_size=2, max_size=20)
        mock_pool.assert_called_once_with(
            "dsn",
            kwargs={"autocommit": True},
            min_size=2,
            max_size=20,
            open=False,
        )


class TestAsyncPostgresConnectorContextManager:
    @pytest.mark.asyncio
    async def test_aenter_opens_pool(self, async_pg):
        async_pg.connection_pool.open = AsyncMock()
        result = await async_pg.__aenter__()
        assert result is async_pg
        async_pg.connection_pool.open.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_aexit_closes_pool(self, async_pg):
        async_pg.connection_pool.close = AsyncMock()
        await async_pg.__aexit__(None, None, None)
        async_pg.connection_pool.close.assert_awaited_once()


class TestAsyncPostgresConnectorQuery:
    @pytest.mark.asyncio
    async def test_async_query_returns_results(self, async_pg):
        mock_cursor = AsyncMock()
        mock_cursor.fetchall.return_value = [("row1",)]

        mock_conn = AsyncMock()
        mock_conn.execute.return_value = mock_cursor

        async_cm = AsyncMock()
        async_cm.__aenter__.return_value = mock_conn
        async_pg.connection_pool.connection.return_value = async_cm

        result = await async_pg.async_query("SELECT 1")
        assert result == [("row1",)]
        mock_conn.execute.assert_awaited_once_with("SELECT 1", None)

    @pytest.mark.asyncio
    async def test_async_query_passes_params(self, async_pg):
        mock_cursor = AsyncMock()
        mock_cursor.fetchall.return_value = []

        mock_conn = AsyncMock()
        mock_conn.execute.return_value = mock_cursor

        async_cm = AsyncMock()
        async_cm.__aenter__.return_value = mock_conn
        async_pg.connection_pool.connection.return_value = async_cm

        await async_pg.async_query("SELECT * FROM t WHERE id = %s", (1,))
        mock_conn.execute.assert_awaited_once_with("SELECT * FROM t WHERE id = %s", (1,))


class TestAsyncPostgresConnectorBulkInsert:
    @pytest.mark.asyncio
    async def test_empty_data_returns_immediately(self, async_pg):
        await async_pg.async_bulk_insert("my_table", [])
        async_pg.connection_pool.connection.assert_not_called()

    @pytest.mark.asyncio
    async def test_async_bulk_insert_calls_copy(self, async_pg):
        mock_copy = AsyncMock()

        mock_cursor = MagicMock()
        mock_cursor.copy.return_value.__aenter__ = AsyncMock(return_value=mock_copy)
        mock_cursor.copy.return_value.__aexit__ = AsyncMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__aexit__ = AsyncMock(return_value=False)

        async_cm = AsyncMock()
        async_cm.__aenter__.return_value = mock_conn
        async_pg.connection_pool.connection.return_value = async_cm

        data = [{"name": "alice", "age": 30}, {"name": "bob", "age": 25}]
        await async_pg.async_bulk_insert("users", data)

        mock_cursor.copy.assert_called_once_with("COPY users (name, age) FROM STDIN")
        assert mock_copy.write_row.await_count == 2
        mock_copy.write_row.assert_any_await(("alice", 30))
        mock_copy.write_row.assert_any_await(("bob", 25))
