# 数据库引擎 - 目录管理

# engine/catalog_manager.py
from sql_compiler.catalog import Catalog
from engine.storage_engine import StorageEngine


class CatalogManager:
    def __init__(self, catalog: Catalog, storage_engine: StorageEngine):
        self.catalog = catalog
        self.storage_engine = storage_engine
        self.system_table_name = 'system_catalog'

        # 初始化系统目录表
        if not self.catalog.table_exists(self.system_table_name):
            self._init_system_catalog()

    def _init_system_catalog(self):
        columns = [
            {'name': 'table_name', 'type': 'VARCHAR(50)'},
            {'name': 'column_name', 'type': 'VARCHAR(50)'},
            {'name': 'column_type', 'type': 'VARCHAR(20)'},
            {'name': 'row_count', 'type': 'INT'}
        ]
        self.catalog.create_table(self.system_table_name, columns)

    def create_table(self, table_name: str, columns: list):
        self.catalog.create_table(table_name, columns)
        # 将元数据写入系统表
        for col in columns:
            record = {
                'table_name': table_name,
                'column_name': col['name'],
                'column_type': col['type'],
                'row_count': 0
            }
            self.storage_engine.insert_record(self.system_table_name, record)

    def get_table_info(self, table_name: str):
        # 从系统表中查询
        condition = {
            'left': {'type': 'column', 'value': 'table_name'},
            'operator': '=',
            'right': {'type': 'constant', 'value_type': 'string', 'value': table_name}
        }
        records = self.storage_engine.read_records(self.system_table_name, condition)

        columns = []
        for record in records:
            columns.append({
                'name': record['column_name'],
                'type': record['column_type']
            })

        return {
            'columns': columns,
            'row_count': records[0]['row_count'] if records else 0
        }