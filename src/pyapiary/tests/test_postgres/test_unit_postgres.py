import sys
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

sys.modules.setdefault("psycopg_pool", MagicMock())

from pyapiary.dbms_connectors.postgres import PostgresConnector, AsyncPostgresConnector


# ──────────────────────────────────────────────
#  Helpers
# ──────────────────────────────────────────────

def make_sync_conn(cursor):
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn

def wire_pool(pg, conn):
    pg.connection_pool.connection.return_value.__enter__ = MagicMock(return_value=conn)
    pg.connection_pool.connection.return_value.__exit__ = MagicMock(return_value=False)


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
    def test_select_returns_rows(self, pg):
        cur = MagicMock()
        cur.description = [("col1",)]
        cur.fetchall.return_value = [("row1",), ("row2",)]
        wire_pool(pg, make_sync_conn(cur))

        result = pg.query("SELECT 1")
        assert result == [("row1",), ("row2",)]
        cur.execute.assert_called_once_with("SELECT 1", None)

    def test_select_passes_params(self, pg):
        cur = MagicMock()
        cur.description = [("col1",)]
        cur.fetchall.return_value = []
        wire_pool(pg, make_sync_conn(cur))

        pg.query("SELECT * FROM t WHERE id = %s", (42,))
        cur.execute.assert_called_once_with("SELECT * FROM t WHERE id = %s", (42,))

    def test_non_select_returns_none(self, pg):
        cur = MagicMock()
        cur.description = None  # INSERT/DDL has no description
        wire_pool(pg, make_sync_conn(cur))

        result = pg.query("INSERT INTO t VALUES (1)")
        assert result is None
        cur.fetchall.assert_not_called()

    def test_select_empty_result_returns_empty_list(self, pg):
        cur = MagicMock()
        cur.description = [("col1",)]
        cur.fetchall.return_value = []
        wire_pool(pg, make_sync_conn(cur))

        result = pg.query("SELECT 1 WHERE false")
        assert result == []


class TestPostgresConnectorBulkInsert:
    def test_empty_data_returns_immediately(self, pg):
        pg.bulk_insert("my_table", [])
        pg.connection_pool.connection.assert_not_called()

    def test_bulk_insert_calls_copy(self, pg):
        mock_copy = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.copy.return_value.__enter__ = MagicMock(return_value=mock_copy)
        mock_cursor.copy.return_value.__exit__ = MagicMock(return_value=False)

        wire_pool(pg, make_sync_conn(mock_cursor))

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
    def _wire(self, async_pg, cur):
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(return_value=cur)
        async_cm = AsyncMock()
        async_cm.__aenter__.return_value = mock_conn
        async_pg.connection_pool.connection.return_value = async_cm
        return mock_conn

    @pytest.mark.asyncio
    async def test_select_returns_rows(self, async_pg):
        cur = AsyncMock()
        cur.description = [("col1",)]
        cur.fetchall = AsyncMock(return_value=[("row1",)])
        conn = self._wire(async_pg, cur)

        result = await async_pg.async_query("SELECT 1")
        assert result == [("row1",)]
        conn.execute.assert_awaited_once_with("SELECT 1", None)

    @pytest.mark.asyncio
    async def test_select_passes_params(self, async_pg):
        cur = AsyncMock()
        cur.description = [("col1",)]
        cur.fetchall = AsyncMock(return_value=[])
        conn = self._wire(async_pg, cur)

        await async_pg.async_query("SELECT * FROM t WHERE id = %s", (1,))
        conn.execute.assert_awaited_once_with("SELECT * FROM t WHERE id = %s", (1,))

    @pytest.mark.asyncio
    async def test_non_select_returns_none(self, async_pg):
        cur = AsyncMock()
        cur.description = None
        conn = self._wire(async_pg, cur)

        result = await async_pg.async_query("INSERT INTO t VALUES (1)")
        assert result is None
        cur.fetchall.assert_not_called()


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