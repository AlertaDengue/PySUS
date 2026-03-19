from typing import List


class CatalogBrowser:
    def __init__(self, client):
        self.client = client

    def list_datasets(self) -> List[str]:
        res = self.client.con.execute("SELECT name FROM datasets").fetchall()
        return [r[0] for r in res]

    def get_groups(self, dataset_name: str):
        query = f"""
            SELECT g.name, g.id
            FROM dataset_groups g
            JOIN datasets d ON g.dataset_id = d.id
            WHERE d.name = '{dataset_name}'
        """
        return self.client.con.execute(query).df()

    def get_files(self, group_id: int):
        return self.client.con.execute(
            f"SELECT * FROM files WHERE group_id = {group_id}"
        ).df()
