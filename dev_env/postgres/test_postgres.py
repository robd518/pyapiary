from pyapiary.dbms_connectors.postgres import PostgresConnector, AsyncPostgresConnector
from pyapiary.helpers import combine_env_configs, setup_logger
from typing import Dict, Any

env_config: Dict[str, Any] = combine_env_configs()

logger = setup_logger("pg logger")
with PostgresConnector(conn_str=env_config["PGSQL_DSN"], logger=logger) as conn:

    # Optional insert test
    logger.info("inserting one row")

    conn.bulk_insert("employees", [{"name": "rob", "department": "hr"}])
    base_query = "SELECT * FROM employees"

    logger.info("Querying with pagination:")
    for i, row in enumerate(conn.query(base_query)):
        print(row)
        if i >= 9:
            break