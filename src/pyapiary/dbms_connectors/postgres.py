from typing import List, Dict, Any, Optional, Generator, Type, Union 
from types import TracebackType
from pyapiary.helpers import setup_logger
from psycopg_pool import AsyncConnectionPool, ConnectionPool

class PostgresConnector:
    def __init__(self,conn_str, logger=None, min_size=5, max_size=30):
        self.dsn = conn_str
        self.min_size = min_size
        self.max_size = max_size
        self.connection_pool = ConnectionPool(self.dsn, kwargs={"autocommit":True}, min_size=self.min_size, max_size=self.max_size)
        self.logger = logger if logger else setup_logger(__name__)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        """Close the PG connection."""
        if self.connection_pool:
            self.connection_pool.close()
            self._log("PG connection closed")

    def _log(self, msg: str, level: str = "info"):
        if self.logger:
            log_method = getattr(self.logger, level, self.logger.info)
            log_method(msg)  

    def query(self, query: str, params=None):
        """
        query - string query representing the work the user wants done
        params - must be legal for psycopog_pool AsyncConnectionPool object
        https://www.psycopg.org/psycopg3/docs/api/pool.html#module-psycopg_pool
        """
        with self.connection_pool.connection() as conn:
            with conn.cursor() as cur:
                # claude recommended a transaction wrapper here
                cur.execute(query, params)
                if cur.rowcount >0:
                    return cur.fetchall()
                else:
                    return None

    def bulk_insert(self, table: str, data: List[Dict[str, Any]]):
        if not data:
            return
    
        self._log(f"Inserting {len(data)} rows into table {table}")
    
        columns = list(data[0].keys())
        copy_query = f"COPY {table} ({', '.join(columns)}) FROM STDIN"
    
        with self.connection_pool.connection() as conn:
            with conn.cursor() as cur:
                with cur.copy(copy_query) as copy:
                    for row in data:
                        copy.write_row(tuple(row[col] for col in columns))
                        # Note: pool is configured for autocommit, commit will happen before the with block ends

# Async Version 
## Need to write an async_bulk_insert
class AsyncPostgresConnector:
    def __init__(self,conn_str, min_size=5, max_size=30, logger=None):
        self.dsn = conn_str
        self.min_size = min_size
        self.max_size = max_size
        self.connection_pool = AsyncConnectionPool(self.dsn, kwargs={"autocommit":True}, min_size=self.min_size, max_size=self.max_size, open=False)
        self.logger = logger if logger else setup_logger(__name__)

    async def __aenter__(self):
        # for async with calls
        await self.connection_pool.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # for async with calls
        await self.connection_pool.close()   

    async def async_query(self, query: str, params=None):
        """
        query - string query representing the work the user wants done
        params - must be legal for psycopog_pool AsyncConnectionPool object
        https://www.psycopg.org/psycopg3/docs/api/pool.html#module-psycopg_pool
        """
        async with self.connection_pool.connection() as conn:
            cur = await conn.execute(query, params)
            return await cur.fetchall()

    async def async_bulk_insert(self, table_name: str, data: List[Dict[str, Any]]):
        if not data:
            return
    
        columns = list(data[0].keys())

        # Google recommended using an cursor.copy command here to process the dict, better performance over a high volume of rows, more efficient
        # than the odbc write/execute_many paradigm.        
        async with self.connection_pool.connection() as aconn:
            async with aconn.cursor() as acur:
                # using COPY is the most performative for millions of rows
                copy_query = f"COPY {table_name} ({', '.join(columns)}) FROM STDIN"
                
                async with acur.copy(copy_query) as copy:
                    for record in data:
                        row = tuple(record[col] for col in columns)
                        await copy.write_row(row)
                        # Note: since asyncpool passes autocommit kwarg, commits will happen before the with block ends