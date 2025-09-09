#存储系统 - 文件管理

# storage/file_manager.py
import os
import json
from typing import Dict, List, Any, Optional
from storage.buffer import BufferPool
from storage.page import PageManager


class FileManager:
    def __init__(self, data_dir: str = 'data'):
        self.data_dir = data_dir
        self.page_manager = PageManager(os.path.join(data_dir, 'pages'))
        self.buffer_pool = BufferPool(self.page_manager)

        # 表文件映射
        self.table_files: Dict[str, List[int]] = {}
        self._load_table_files()

    def _load_table_files(self):
        mapping_file = os.path.join(self.data_dir, 'table_files.json')
        if os.path.exists(mapping_file):
            with open(mapping_file, 'r') as f:
                self.table_files = json.load(f)

    def _save_table_files(self):
        mapping_file = os.path.join(self.data_dir, 'table_files.json')
        with open(mapping_file, 'w') as f:
            json.dump(self.table_files, f, indent=2)

    def create_table_file(self, table_name: str) -> List[int]:
        """为表创建初始页面"""
        if table_name in self.table_files:
            raise Exception(f"Table file for '{table_name}' already exists")

        # 分配一个页面用于存储表头信息
        header_page = self.buffer_pool.allocate_page()

        # 初始化表头
        header_page.set_int(0, 0)  # 记录数
        header_page.set_int(4, 0)  # 第一个数据页面ID
        header_page.set_int(8, -1)  # 下一个页面ID（暂时没有）

        self.table_files[table_name] = [header_page.page_id]
        self._save_table_files()

        return self.table_files[table_name]

    def get_table_pages(self, table_name: str) -> Optional[List[int]]:
        return self.table_files.get(table_name)

    def add_page_to_table(self, table_name: str, page_id: int):
        if table_name in self.table_files:
            self.table_files[table_name].append(page_id)
            self._save_table_files()

    def drop_table_file(self, table_name: str):
        if table_name in self.table_files:
            # 释放所有页面
            for page_id in self.table_files[table_name]:
                self.buffer_pool.free_page(page_id)

            del self.table_files[table_name]
            self._save_table_files()

    def flush_all(self):
        self.buffer_pool.flush_all()