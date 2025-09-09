#数据库引擎 - 目录管理

# engine/catalog_manager.py
from sql_compiler.catalog import Catalog


class CatalogManager:
    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def create_table(self, table_name: str, columns: list):
        """创建表目录信息"""
        return self.catalog.create_table(table_name, columns)

    def drop_table(self, table_name: str):
        """删除表目录信息"""
        return self.catalog.drop_table(table_name)

    def get_table_info(self, table_name: str):
        """获取表信息"""
        return self.catalog.get_table_info(table_name)

    def table_exists(self, table_name: str):
        """检查表是否存在"""
        return self.catalog.table_exists(table_name)

    def update_row_count(self, table_name: str, count: int):
        """更新表行数"""
        return self.catalog.update_row_count(table_name, count)