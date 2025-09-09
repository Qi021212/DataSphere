#存储系统 - 页式存储

# storage/page.py
import struct
import os
from typing import Optional
from utils.constants import PAGE_SIZE


class Page:
    def __init__(self, page_id: int, data: bytes = None):
        self.page_id = page_id
        self.data = data if data is not None else bytearray(PAGE_SIZE)
        self.is_dirty = False
        self.pin_count = 0

    def read_data(self, offset: int, size: int) -> bytes:
        return self.data[offset:offset + size]

    def write_data(self, offset: int, data: bytes):
        self.data[offset:offset + len(data)] = data
        self.is_dirty = True

    def get_int(self, offset: int) -> int:
        return struct.unpack('i', self.data[offset:offset + 4])[0]

    def set_int(self, offset: int, value: int):
        self.data[offset:offset + 4] = struct.pack('i', value)
        self.is_dirty = True

    def get_string(self, offset: int, length: int) -> str:
        return self.data[offset:offset + length].decode('utf-8').rstrip('\x00')

    def set_string(self, offset: int, value: str, length: int):
        encoded = value.encode('utf-8')
        padded = encoded.ljust(length, b'\x00')
        self.data[offset:offset + length] = padded
        self.is_dirty = True


class PageManager:
    def __init__(self, data_dir: str = 'data/pages'):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

    def read_page(self, page_id: int) -> Optional[Page]:
        file_path = self._get_page_file_path(page_id)
        if not os.path.exists(file_path):
            return None

        with open(file_path, 'rb') as f:
            data = f.read(PAGE_SIZE)
            return Page(page_id, data)

    def write_page(self, page: Page):
        if not page.is_dirty:
            return

        file_path = self._get_page_file_path(page.page_id)
        with open(file_path, 'wb') as f:
            f.write(page.data)

        page.is_dirty = False

    def allocate_page(self) -> Page:
        # 简单的实现：找到最大的page_id并加1
        max_id = -1
        for filename in os.listdir(self.data_dir):
            if filename.startswith('page_') and filename.endswith('.dat'):
                try:
                    page_id = int(filename[5:-4])
                    max_id = max(max_id, page_id)
                except ValueError:
                    continue

        new_page_id = max_id + 1
        return Page(new_page_id)

    def free_page(self, page_id: int):
        file_path = self._get_page_file_path(page_id)
        if os.path.exists(file_path):
            os.remove(file_path)

    def _get_page_file_path(self, page_id: int) -> str:
        return os.path.join(self.data_dir, f'page_{page_id}.dat')