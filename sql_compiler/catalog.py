#SQL编译器 - 目录管理

# sql_compiler/catalog.py
import json
import os
from typing import Dict, List, Any, Optional


class Catalog:
    def __init__(self, catalog_file: str = 'data/catalog.json'):
        self.catalog_file = catalog_file
        self.tables = self._load_catalog()

    def _load_catalog(self) -> Dict[str, Any]:
        if os.path.exists(self.catalog_file):
            with open(self.catalog_file, 'r') as f:
                return json.load(f)
        return {}

    def _save_catalog(self):
        os.makedirs(os.path.dirname(self.catalog_file), exist_ok=True)
        with open(self.catalog_file, 'w') as f:
            json.dump(self.tables, f, indent=2)

    def table_exists(self, table_name: str) -> bool:
        return table_name in self.tables

    def create_table(self, table_name: str, columns: List[Dict[str, str]]):
        if self.table_exists(table_name):
            raise Exception(f"Table '{table_name}' already exists")

        self.tables[table_name] = {
            'columns': columns,
            'row_count': 0
        }
        self._save_catalog()

    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        return self.tables.get(table_name)

    def drop_table(self, table_name: str):
        if table_name in self.tables:
            del self.tables[table_name]
            self._save_catalog()

    def update_row_count(self, table_name: str, count: int):
        if table_name in self.tables:
            self.tables[table_name]['row_count'] = count
            self._save_catalog()