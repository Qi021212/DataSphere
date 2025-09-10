#SQL编译器 - 目录管理

# sql_compiler/catalog.py
import json
import os
from typing import Dict, List, Any, Optional


class Catalog:
    def __init__(self, catalog_file: str = 'data/catalog.json'):
        self.catalog_file = catalog_file
        # 👇 关键修改：直接初始化为新的空字典
        self.tables = {}
        # 👇 然后再尝试从文件加载，如果文件存在则覆盖 self.tables
        self._load_catalog_from_file()

    def _load_catalog_from_file(self):
        """从文件加载目录，如果文件不存在则不进行任何操作"""
        if os.path.exists(self.catalog_file):
            try:
                with open(self.catalog_file, 'r') as f:
                    loaded_data = json.load(f)
                    # 👇 只有文件存在且加载成功，才用加载的数据覆盖内存中的字典
                    self.tables = loaded_data
                    print(f"DEBUG: 从 {self.catalog_file} 成功加载目录")
            except json.JSONDecodeError:
                print(f"DEBUG: {self.catalog_file} 文件损坏，将创建新的空目录")
                # 文件损坏，我们保留空的 self.tables，并在下次 _save_catalog 时覆盖它
            except Exception as e:
                print(f"DEBUG: 加载目录时发生未知错误: {e}")
                # 同样，保留空的 self.tables
        else:
            print(f"目录文件 {self.catalog_file} 不存在，将创建新的空目录")

    def _load_catalog(self) -> Dict[str, Any]: # 这个方法可以保留，但不再在 __init__ 中调用
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

    def create_table(self, table_name: str, columns: List[Dict[str, str]],
                     constraints: List[tuple] = None):  # 👈 修改签名，添加 constraints 参数
        if self.table_exists(table_name):
            raise Exception(f"Table '{table_name}' already exists")
        self.tables[table_name] = {
            'columns': columns,
            'row_count': 0,
            'constraints': constraints or []  # 👈 关键：存储约束
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

    # 👇 新增方法：查找引用了指定表和列的外键
    def find_referencing_tables(self, target_table: str, target_column: str) -> list:
        """
        查找所有外键引用了指定表和列的表。
        返回: [(引用表名, 引用列名), ...]
        """
        referencing_tables = []
        for table_name, table_meta in self.tables.items():
            for constraint in table_meta.get('constraints', []):
                if (constraint[0] == 'FOREIGN_KEY' and
                    constraint[2] == target_table and  # ref_table
                    constraint[3] == target_column):  # ref_col
                    referencing_tables.append((table_name, constraint[1])) # (本表名, 本表的外键列)
        return referencing_tables