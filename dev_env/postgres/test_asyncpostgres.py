import asyncio
from pyapiary.dbms_connectors.postgres import AsyncPostgresConnector
from pyapiary.helpers import combine_env_configs, setup_logger
from typing import Dict, Any

async def test_async_db():
    # Load config and setup logging
    env_config: Dict[str, Any] = combine_env_configs()
    logger = setup_logger("pg logger")
    
    # Use 'async with' to handle the background worker threads and connection pool
    async with AsyncPostgresConnector(conn_str=env_config["PGSQL_DSN"], logger=logger) as conn:

        logger.info("inserting one row (async)")
        # Await the execution of the insert
        await conn.async_bulk_insert("employees", [{"name": "rob", "department": "hr"}])
        
        base_query = "SELECT * FROM employees"
        logger.info("Querying with pagination (async):")
        
        # Await the coroutine to get the actual list of results
        rows = await conn.async_query(base_query)

        # Standard loop through the returned list
        for i, row in enumerate(rows):
            print(row)
            if i >= 9:
                break

if __name__ == "__main__":
    # Standard entry point for async scripts
    asyncio.run(test_async_db())