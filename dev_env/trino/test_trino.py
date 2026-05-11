from pyapiary.dbms_connectors.trino import TrinoConnector
from pyapiary.helpers import combine_env_configs
from typing import Dict, Any

env_config: Dict[str, Any] = combine_env_configs()


client = TrinoConnector(
    host=env_config["TRINO_HOST"],
    port=env_config["TRINO_PORT"],
    user=env_config["TRINO_USER"]
)

# Find
print(client.query("SELECT * FROM pg.public.employees"))


mylist = [{'name':'Pat','department':'Facilities MGMT'}]

print(client.bulk_insert('pg.public.employees',mylist))