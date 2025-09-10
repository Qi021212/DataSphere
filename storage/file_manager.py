# storage/file_manager.py
import os
import json
import struct
from typing import Dict, List, Any, Optional

from storage.buffer import BufferPool
from storage.page import PageManager, Page
from utils.constants import PAGE_SIZE


class FileManager:
    def __init__(self, data_dir: str = 'data'):
        self.data_dir = data_dir
        self.page_manager = PageManager(os.path.join(data_dir, 'pages'))
        self.buffer_pool = BufferPool(self.page_manager)

        # 表文件映射: {table_name: [header_page_id, data_page_id_1, data_page_id_2, ...]}
        self.table_files: Dict[str, List[int]] = {}
        self._load_table_files()

    def _load_table_files(self):
        mapping_file = os.path.join(self.data_dir, 'table_files.json')
        if os.path.exists(mapping_file):
            with open(mapping_file, 'r') as f:
                self.table_files = json.load(f)

    def _save_table_files(self):
        mapping_file = os.path.join(self.data_dir, 'table_files.json')
        os.makedirs(self.data_dir, exist_ok=True)
        with open(mapping_file, 'w') as f:
            json.dump(self.table_files, f, indent=2)

    def create_table_file(self, table_name: str, columns: List[Dict[str, str]]) -> List[int]:
        """为表创建初始页面，并在页头存储表结构"""
        if table_name in self.table_files:
            raise Exception(f"Table file for '{table_name}' already exists")

        # 分配一个页面用于存储表头信息
        header_page = self.buffer_pool.allocate_page()

        # 初始化表头
        # offset 0: 记录数 (4 bytes)
        header_page.set_int(0, 0)
        # offset 4: 第一个数据页面ID (4 bytes), 初始化为-1表示无数据页
        header_page.set_int(4, -1)
        # offset 8: 下一个页面ID (4 bytes), 用于链接页，初始化为-1
        header_page.set_int(8, -1)

        # 从 offset 12 开始存储列信息
        # 先存储列数 (4 bytes)
        column_count_offset = 12
        header_page.set_int(column_count_offset, len(columns))

        # 然后依次存储每列的信息: 列名长度(2 bytes) + 列名 + 列类型长度(2 bytes) + 列类型
        current_offset = column_count_offset + 4
        for col in columns:
            col_name = col['name']
            col_type = col['type']

            # 写入列名长度和列名
            name_bytes = col_name.encode('utf-8')
            header_page.set_int(current_offset, len(name_bytes), size=2)  # 2 bytes for length
            current_offset += 2
            header_page.write_data(current_offset, name_bytes)
            current_offset += len(name_bytes)

            # 写入列类型长度和列类型
            type_bytes = col_type.encode('utf-8')
            header_page.set_int(current_offset, len(type_bytes), size=2)
            current_offset += 2
            header_page.write_data(current_offset, type_bytes)
            current_offset += len(type_bytes)

        # 确保表头页写入磁盘
        self.buffer_pool.flush_page(header_page.page_id)

        self.table_files[table_name] = [header_page.page_id]
        self._save_table_files()

        return self.table_files[table_name]

    def get_table_pages(self, table_name: str) -> Optional[List[int]]:
        """获取表的所有页面ID，包括表头页"""
        return self.table_files.get(table_name)

    def _get_table_header(self, table_name: str) -> Optional[Page]:
        """获取表的头页面"""
        page_ids = self.get_table_pages(table_name)
        if not page_ids or len(page_ids) == 0:
            return None
        return self.buffer_pool.get_page(page_ids[0])

    def _get_column_info_from_header(self, header_page: Page) -> List[Dict[str, str]]:
        """从表头页解析出列信息 (需要与create_table_file中的存储格式对应) """
        # 先读取列数 (4 bytes)
        column_count = header_page.get_int(12)
        columns = []
        current_offset = 16  # 12 + 4 (int大小)

        for _ in range(column_count):
            # 读取列名长度 (使用2字节，与写入时一致)
            name_len = header_page.get_int(current_offset, size=2)  # 修改点1: 指定 size=2
            current_offset += 2  # 修改点2: 只移动2字节
            # 读取列名
            col_name = header_page.read_data(current_offset, name_len).decode('utf-8').rstrip('\x00')
            current_offset += name_len

            # 读取列类型长度 (使用2字节，与写入时一致)
            type_len = header_page.get_int(current_offset, size=2)  # 修改点3: 指定 size=2
            current_offset += 2  # 修改点4: 只移动2字节
            # 读取列类型
            col_type = header_page.read_data(current_offset, type_len).decode('utf-8').rstrip('\x00')
            current_offset += type_len

            columns.append({'name': col_name, 'type': col_type})

        return columns

    def _serialize_record(self, record: Dict[str, Any], columns: List[Dict[str, str]]) -> bytes:
        """将记录序列化为字节流"""
        serialized_data = bytearray()
        for col in columns:
            col_name = col['name']
            col_type = col['type']
            value = record.get(col_name, None)

            if col_type == 'INT':
                serialized_data.extend(struct.pack('i', value if value is not None else 0))
            elif col_type == 'FLOAT':
                serialized_data.extend(struct.pack('f', value if value is not None else 0.0))
            elif col_type == 'VARCHAR':
                # 存储格式: 4字节长度 + 字符串内容
                str_val = str(value) if value is not None else ""
                encoded_str = str_val.encode('utf-8')
                serialized_data.extend(struct.pack('i', len(encoded_str)))
                serialized_data.extend(encoded_str)
            else:
                raise ValueError(f"Unsupported data type: {col_type}")

        return bytes(serialized_data)

    def _deserialize_record(self, data, columns: List[Dict[str, str]], offset: int = 0) -> tuple:
        """从字节流反序列化记录，并返回记录和新的偏移量"""
        record = {}
        current_offset = offset
        for col in columns:
            col_name = col['name']
            col_type = col['type']

            if col_type == 'INT':
                value = struct.unpack('i', data[current_offset:current_offset + 4])[0]
                current_offset += 4
            elif col_type == 'FLOAT':
                value = struct.unpack('f', data[current_offset:current_offset + 4])[0]
                current_offset += 4
            elif col_type == 'VARCHAR':
                str_len = struct.unpack('i', data[current_offset:current_offset + 4])[0]
                current_offset += 4
                value = data[current_offset:current_offset + str_len].decode('utf-8')
                current_offset += str_len
            else:
                raise ValueError(f"Unsupported data type: {col_type}")

            record[col_name] = value

        # --- 关键修改：返回一个元组 (record, current_offset) ---
        return record, current_offset

    def _get_record_size(self, columns: List[Dict[str, str]]) -> int:
        """计算一条记录的固定大小（简化版，假设VARCHAR也按最大长度算）"""
        size = 0
        for col in columns:
            col_type = col['type']
            if col_type == 'INT':
                size += 4
            elif col_type == 'FLOAT':
                size += 4
            elif col_type == 'VARCHAR':
                # 简化：假设每个VARCHAR字段最大255字节 + 4字节长度
                size += 4 + 255
            else:
                raise ValueError(f"Unsupported data type: {col_type}")
        return size

    def insert_record(self, table_name: str, record: Dict[str, Any]) -> bool:
        """将一条记录插入到表中 (真正写入数据页)"""
        header_page = self._get_table_header(table_name)
        if not header_page:
            raise Exception(f"Table '{table_name}' does not exist")

        columns = self._get_column_info_from_header(header_page)
        record_data = self._serialize_record(record, columns)
        record_size = len(record_data)

        # 获取第一个数据页ID (假设存储在表头页偏移量4处)
        first_data_page_id = header_page.get_int(4)

        target_page = None
        free_space_offset = -1

        if first_data_page_id <= 0:  # 如果没有数据页
            # 分配一个新的数据页
            new_data_page = self.buffer_pool.allocate_page()
            new_data_page.set_int(0, 0)  # 初始化: 当前页记录数为0
            new_data_page.set_int(4, -1)  # 下一个页ID为-1

            # 更新表头，指向新的数据页
            header_page.set_int(4, new_data_page.page_id)
            self.buffer_pool.flush_page(header_page.page_id)

            # 将新页加入表文件映射
            self.add_page_to_table(table_name, new_data_page.page_id)

            target_page = new_data_page
            free_space_offset = 8  # 假设页内偏移0-4是记录数，4-8是下一页ID
        else:
            # 简化实现: 总是插入到第一个数据页
            target_page = self.buffer_pool.get_page(first_data_page_id)
            if not target_page:
                raise Exception("Failed to load data page")

            # 获取当前页记录数
            record_count = target_page.get_int(0)
            # 计算新的空闲空间偏移 (简化: 线性分配)
            free_space_offset = 8 + (record_count * record_size)

        # 检查页空间是否足够 (简化检查)
        if free_space_offset + record_size > 4096:
            raise Exception("Page full, need to implement page linking")

        # 写入记录数据
        target_page.write_data(free_space_offset, record_data)
        # 更新页内记录数
        target_page.set_int(0, target_page.get_int(0) + 1)
        # 标记页为脏页，BufferPool会在适当时候写回磁盘

        # 更新表头的总记录数
        total_count = header_page.get_int(0)
        header_page.set_int(0, total_count + 1)
        # 表头页也会在BufferPool的LRU淘汰或显式flush时写回磁盘

        return True

    def read_records(self, table_name: str, condition: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """从表中读取所有记录 (真正从数据页读取) """
        header_page = self._get_table_header(table_name)
        if not header_page:
            raise Exception(f"Table '{table_name}' does not exist")

        columns = self._get_column_info_from_header(header_page)
        results = []

        # --- 关键修复：在循环前预先计算 record_size ---
        # 创建一个样本记录，用默认值填充
        sample_record = {}
        for col in columns:
            col_type = col['type']
            if col_type == 'INT':
                sample_record[col['name']] = 0
            elif col_type == 'FLOAT':
                sample_record[col['name']] = 0.0
            elif col_type == 'VARCHAR':
                sample_record[col['name']] = ""  # 空字符串
            else:
                raise ValueError(f"Unsupported data type: {col_type} for size calculation")

        # 序列化样本记录以获取其大小
        serialized_sample = self._serialize_record(sample_record, columns)
        record_size = len(serialized_sample)
        # -- 修复结束 --

        # 获取第一个数据页ID
        current_page_id = header_page.get_int(4)
        while current_page_id > 0:
            page = self.buffer_pool.get_page(current_page_id)
            if not page:
                break

            record_count = page.get_int(0)
            # 从偏移量8开始读取记录
            data_offset = 8
            for _ in range(record_count):
                try:
                    # --- 关键修改：正确解包元组 ---
                    record, new_offset = self._deserialize_record(page.data, columns, data_offset)
                    # --- 修改结束 ---

                    # 如果有条件，进行过滤
                    if condition is None or self._evaluate_condition_in_fm(record, condition):
                        results.append(record)

                    # 计算下一条记录的偏移 (简化: 假设所有记录大小相同)
                    if not results:  # 第一次循环，计算记录大小
                        sample_record = self._serialize_record(record, columns)
                        record_size = len(sample_record)

                    # --- 关键修改：使用 new_offset 或 record_size 来更新偏移 ---
                    # 我们有两种选择：
                    # 选择1 (推荐): 直接使用 _deserialize_record 返回的 new_offset
                    data_offset = new_offset
                    # 选择2: 使用预先计算的 record_size (需要确保 record_size 已被计算)
                    # data_offset += record_size
                    # --- 修改结束 ---

                except Exception as e:
                    print(f"Error reading record: {e}")
                    break

            # 移动到下一页 (简化实现中，我们只处理第一页)
            break
            # 完整实现应为: current_page_id = page.get_int(4)qui

        return results

    def _evaluate_condition_in_fm(self, record: Dict[str, Any], condition: Dict[str, Any]) -> bool:
        """在FileManager内部评估WHERE条件"""
        left = condition.get('left', {})
        operator = condition.get('operator', '')
        right = condition.get('right', {})

        if left.get('type') == 'column' and right.get('type') == 'constant':
            col_name = left['value']
            col_value = record.get(col_name)

            if col_value is None:
                return False

            right_value = right['value']
            if right['value_type'] == 'int':
                right_value = int(right_value)
                try:
                    col_value = int(col_value)
                except (ValueError, TypeError):
                    return False
            elif right['value_type'] == 'float':
                right_value = float(right_value)
                try:
                    col_value = float(col_value)
                except (ValueError, TypeError):
                    return False
            elif right['value_type'] == 'string':
                right_value = str(right_value)
                col_value = str(col_value)

            if operator == '=':
                return col_value == right_value
            elif operator == '>':
                return col_value > right_value
            elif operator == '<':
                return col_value < right_value
            elif operator == '>=':
                return col_value >= right_value
            elif operator == '<=':
                return col_value <= right_value
            elif operator == '!=':
                return col_value != right_value

        return False

    def delete_records(self, table_name: str, condition: Dict[str, Any] = None) -> int:
        """删除满足条件的记录 (简化实现：物理删除并重写页)"""
        header_page = self._get_table_header(table_name)
        if not header_page:
            raise Exception(f"Table '{table_name}' does not exist")

        columns = self._get_column_info_from_header(header_page)

        deleted_count = 0
        current_page_id = header_page.get_int(4)

        while current_page_id != -1:
            page = self.buffer_pool.get_page(current_page_id)
            if not page:
                break

            record_count = page.get_int(0)
            next_page_id = page.get_int(4)

            # 读取所有记录，过滤掉要删除的
            records_to_keep = []
            data_offset = 8
            for i in range(record_count):
                record, new_offset = self._deserialize_record(page.data, columns, data_offset)
                data_offset = new_offset

                if condition is None or not self._evaluate_condition_in_fm(record, condition):
                    # 保留不满足删除条件的记录
                    records_to_keep.append(record)
                else:
                    deleted_count += 1

            # 重写当前页
            page.set_int(0, len(records_to_keep))  # 更新页内记录数
            write_offset = 8
            for rec in records_to_keep:
                rec_data = self._serialize_record(rec, columns)
                page.write_data(write_offset, rec_data)
                write_offset += len(rec_data)

            # 填充剩余空间为0 (可选)
            page.write_data(write_offset, b'\x00' * (4096 - write_offset))

            current_page_id = next_page_id

        # 更新表头的总记录数
        total_count = header_page.get_int(0)
        header_page.set_int(0, total_count - deleted_count)
        self.buffer_pool.flush_page(header_page.page_id)

        return deleted_count

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