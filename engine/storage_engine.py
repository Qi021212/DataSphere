#数据库引擎 - 存储引擎

# engine/storage_engine.py
from storage.file_manager import FileManager


class StorageEngine:
    def __init__(self, file_manager: FileManager):
        self.file_manager = file_manager

    def create_table(self, table_name: str, columns: list):
        """创建表存储结构"""
        return self.file_manager.create_table_file(table_name)

    def insert_record(self, table_name: str, record: dict):
        """插入记录"""
        # 这里应该有实际的存储逻辑
        # 简化实现
        pass

    def read_records(self, table_name: str, condition: dict = None):
        """读取记录"""
        # 这里应该有实际的读取逻辑
        # 简化实现
        return []

    def delete_records(self, table_name: str, condition: dict = None):
        """删除记录"""
        # 这里应该有实际的删除逻辑
        # 简化实现
        return 0

    def flush(self):
        """刷新所有缓冲数据到磁盘"""
        self.file_manager.flush_all()