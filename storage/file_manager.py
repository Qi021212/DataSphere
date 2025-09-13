# storage/file_manager.py
import os
import json
import struct
from typing import Dict, List, Any, Optional
from storage.buffer import BufferPool
from storage.page import PageManager, Page


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
        """从表头页解析出列信息"""
        columns = []
        # 读取列数
        column_count = header_page.get_int(12)
        current_offset = 16  # 12 + 4

        for _ in range(column_count):
            # 读取列名
            name_len = header_page.get_int(current_offset, size=2)
            current_offset += 2
            col_name = header_page.read_data(current_offset, name_len).decode('utf-8')
            current_offset += name_len

            # 读取列类型
            type_len = header_page.get_int(current_offset, size=2)
            current_offset += 2
            col_type = header_page.read_data(current_offset, type_len).decode('utf-8')
            current_offset += type_len

            columns.append({'name': col_name, 'type': col_type})

        return columns

    def _serialize_record(self, record: Dict[str, Any], columns: List[Dict[str, str]]) -> bytes:
        """将记录序列化为字节流"""
        serialized_data = bytearray()
        for col in columns:
            col_name = col['name']
            col_type = col['type']
            value = record.get(col_name)

            if col_type == 'INT':
                serialized_data.extend(struct.pack('i', value if value is not None else 0))
            elif col_type == 'FLOAT':
                serialized_data.extend(struct.pack('f', value if value is not None else 0.0))
            elif col_type == 'VARCHAR':
                # 对于VARCHAR，我们存储长度(4 bytes) + 字符串内容
                if value is None:
                    value = ""
                encoded_str = value.encode('utf-8')
                serialized_data.extend(struct.pack('i', len(encoded_str)))
                serialized_data.extend(encoded_str)
            else:
                raise ValueError(f"Unsupported data type: {col_type}")

        return bytes(serialized_data)

    def _deserialize_record(self, data: bytes, columns: List[Dict[str, str]], offset: int = 0) -> (Dict[str, Any], int):
        """从字节流反序列化记录，返回记录和新的偏移量"""
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
        """将一条记录插入到表中"""
        header_page = self._get_table_header(table_name)
        if not header_page:
            raise Exception(f"Table '{table_name}' does not exist")

        columns = self._get_column_info_from_header(header_page)
        record_data = self._serialize_record(record, columns)
        actual_record_size = len(record_data)  # 使用实际序列化后的大小

        # 获取第一个数据页ID
        first_data_page_id = header_page.get_int(4)

        target_page_id = -1
        target_page = None
        free_space_offset = -1

        if first_data_page_id == -1:
            # 没有数据页，分配一个新的
            new_data_page = self.buffer_pool.allocate_page()
            # 初始化数据页：offset 0 存储本页记录数，offset 4 存储下一个页ID
            new_data_page.set_int(0, 0)  # 当前页记录数
            new_data_page.set_int(4, -1)  # 下一个页ID
            # 从 offset 8 开始存储数据
            free_space_offset = 8
            target_page = new_data_page
            target_page_id = new_page_id = new_data_page.page_id

            # 更新表头，指向新的数据页
            header_page.set_int(4, new_page_id)
            self.buffer_pool.flush_page(header_page.page_id)
            # 更新表文件映射
            self.table_files[table_name].append(new_page_id)
            self._save_table_files()
        else:
            # 遍历数据页链表，寻找有空闲空间的页
            current_page_id = first_data_page_id
            while current_page_id != -1:
                page = self.buffer_pool.get_page(current_page_id)
                if not page:
                    break

                record_count = page.get_int(0)
                next_page_id = page.get_int(4)

                # 👇 关键修复：精确计算当前页的写入偏移量
                current_offset = 8  # 跳过页头 (记录数4字节 + 下一页ID4字节)
                valid_record_count = 0  # 用于计数成功反序列化的记录

                for _ in range(record_count):
                    try:
                        _, next_offset = self._deserialize_record(page.data, columns, current_offset)
                        current_offset = next_offset
                        valid_record_count += 1
                    except Exception as e:
                        print(
                            f"Warning: Skipping corrupted record in page {current_page_id} at offset {current_offset}: {e}")
                        # 如果反序列化失败，我们跳过这条记录，但为了安全，我们中断当前页的插入，转而寻找新页。
                        # 这是一种保守策略，避免在损坏的页上继续写入。
                        break

                # 检查剩余空间是否足够
                if current_offset + actual_record_size <= 4096:  # PAGE_SIZE
                    free_space_offset = current_offset
                    target_page = page
                    target_page_id = current_page_id
                    # 更新页内记录数为有效记录数
                    page.set_int(0, valid_record_count)
                    break
                else:
                    # 空间不足，继续查找下一页
                    pass

                current_page_id = next_page_id

            if target_page_id == -1:
                # 所有现有页都满了，分配新页
                new_data_page = self.buffer_pool.allocate_page()
                new_data_page.set_int(0, 0)
                new_data_page.set_int(4, -1)
                free_space_offset = 8
                target_page = new_data_page
                target_page_id = new_page_id = new_data_page.page_id

                # 将新页链接到链表末尾
                current_page_id = first_data_page_id
                while True:
                    page = self.buffer_pool.get_page(current_page_id)
                    if page.get_int(4) == -1:
                        page.set_int(4, new_page_id)
                        self.buffer_pool.flush_page(current_page_id)
                        break
                    current_page_id = page.get_int(4)

                # 更新表文件映射
                self.table_files[table_name].append(new_page_id)
                self._save_table_files()

        # 写入记录
        target_page.write_data(free_space_offset, record_data)
        # 更新页内记录数
        target_page.set_int(0, target_page.get_int(0) + 1)
        # 标记为脏页，BufferPool会在LRU淘汰或显式flush时写回磁盘

        # 更新表头的总记录数
        total_count = header_page.get_int(0)
        header_page.set_int(0, total_count + 1)
        self.buffer_pool.flush_page(header_page.page_id)

        return True

    def read_records(self, table_name: str, condition: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """读取表中的所有记录，并根据条件过滤"""
        header_page = self._get_table_header(table_name)
        if not header_page:
            raise Exception(f"Table '{table_name}' does not exist")

        columns = self._get_column_info_from_header(header_page)

        results = []
        current_page_id = header_page.get_int(4)  # 第一个数据页ID

        while current_page_id != -1:
            page = self.buffer_pool.get_page(current_page_id)
            if not page:
                break

            record_count = page.get_int(0)
            next_page_id = page.get_int(4)

            # 从 offset 8 开始读取记录
            data_offset = 8
            for _ in range(record_count):
                try:
                    record, new_offset = self._deserialize_record(page.data, columns, data_offset)

                    # 应用条件过滤
                    if condition is None or self._evaluate_condition_in_fm(record, condition):
                        results.append(record)
                    data_offset = new_offset
                except Exception as e:
                    print(f"Error deserializing record: {e}")
                    break

            current_page_id = next_page_id

        return results

    def _evaluate_condition_in_fm(self, record: Dict[str, Any], condition: Dict[str, Any]) -> bool:
        """在FileManager内部评估条件"""
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
                except ValueError:
                    return False
            elif right['value_type'] == 'float':
                right_value = float(right_value)
                try:
                    col_value = float(col_value)
                except ValueError:
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

    def update_records(self, table_name: str, set_clause: List[tuple], condition: Dict[str, Any] = None) -> int:
        """更新满足条件的记录"""
        header_page = self._get_table_header(table_name)
        if not header_page:
            raise Exception(f"Table '{table_name}' does not exist")

        columns = self._get_column_info_from_header(header_page)
        updated_count = 0

        # 获取第一个数据页ID
        current_page_id = header_page.get_int(4)
        while current_page_id != -1:
            page = self.buffer_pool.get_page(current_page_id)
            if not page:
                break

            record_count = page.get_int(0)
            next_page_id = page.get_int(4)

            # 读取所有记录，更新满足条件的
            records = []
            data_offset = 8
            for i in range(record_count):
                record, new_offset = self._deserialize_record(page.data, columns, data_offset)
                data_offset = new_offset

                # 判断是否满足条件
                should_update = True
                if condition is not None:
                    should_update = self._evaluate_condition_in_fm(record, condition)

                if should_update:
                    # 执行更新
                    for set_col, set_value in set_clause:
                        record[set_col] = set_value
                    updated_count += 1

                records.append(record)

            # 重写当前页
            page.set_int(0, len(records))  # 更新页内记录数
            write_offset = 8
            for rec in records:
                rec_data = self._serialize_record(rec, columns)
                page.write_data(write_offset, rec_data)
                write_offset += len(rec_data)

            # 填充剩余空间 (可选)
            if write_offset < 4096:
                page.write_data(write_offset, b'\x00' * (4096 - write_offset))

            current_page_id = next_page_id

        # 更新表头的总记录数 (这里总记录数不变，因为是更新不是增删)
        # 但我们可能需要更新一些统计信息，这里暂不处理
        return updated_count