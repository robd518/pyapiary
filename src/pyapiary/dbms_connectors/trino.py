from trino.dbapi import connect
from typing import List, Dict, Any

class TrinoConnector:
    def __init__(self, **kwargs):
        self.conn = connect(
            **kwargs
        )
    
    def query(self, query_str):
        with self.conn.cursor() as cur:
            cur.execute(query_str)
            if cur.description:
                return cur.fetchall()
            else:
                return None
    
    def bulk_insert(self, table, data : List[Dict[str,Any]]):
        if not len(data)>0:
            print('Invalid List of Dict type passed. Must have more than one element.')
            return None
        columns = data[0].keys()
        placeholders = ", ".join(["?"] * len(columns))
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        values = [tuple(row[col] for col in columns) for row in data]
        with self.conn.cursor() as cur:
            cur.executemany(query, values)
            return True