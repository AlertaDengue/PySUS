import duckdb


class StorageManager:
    def __init__(self, connection: duckdb.DuckDBPyConnection):
        self.con = connection

    def query(self, sql: str):
        return self.con.execute(sql).df()

    def get_file_url(self, path: str) -> str:
        return f"s3://pysus/public/{path}"

    def list_tables(self):
        return self.con.execute("SHOW TABLES").df()
