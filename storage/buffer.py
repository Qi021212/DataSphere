#存储系统 - 缓存管理

# storage/buffer.py
from collections import OrderedDict
from typing import Optional, Dict
from storage.page import Page, PageManager
from utils.constants import BUFFER_POOL_SIZE


class BufferPool:
    def __init__(self, page_manager: PageManager, pool_size: int = BUFFER_POOL_SIZE, policy: str = 'LRU'):
        self.page_manager = page_manager
        self.pool_size = pool_size
        self.policy = policy
        self.buffer: OrderedDict[int, Page] = OrderedDict()
        self.hit_count = 0
        self.miss_count = 0

    def get_page(self, page_id: int) -> Optional[Page]:
        # 检查页面是否在缓存中
        if page_id in self.buffer:
            page = self.buffer[page_id]
            # 移动到最近使用的位置
            self.buffer.move_to_end(page_id)
            self.hit_count += 1
            return page

        # 从磁盘加载页面
        page = self.page_manager.read_page(page_id)
        if page is None:
            return None

        # 添加到缓存
        self.buffer[page_id] = page
        self.miss_count += 1

        # 如果缓存满了，移除最久未使用的页面
        if len(self.buffer) > self.pool_size:
            self.evict_page()

        return page

    def evict_page(self):
        if not self.buffer:
            return

        if self.policy == 'LRU':
            page_id, page = self.buffer.popitem(last=False)
        elif self.policy == 'FIFO':
            page_id, page = self.buffer.popitem(last=False)
        else:
            raise ValueError(f"Unsupported policy: {self.policy}")

        if page.is_dirty:
            self.page_manager.write_page(page)

    def flush_page(self, page_id: int):
        if page_id in self.buffer:
            page = self.buffer[page_id]
            if page.is_dirty:
                self.page_manager.write_page(page)
                page.is_dirty = False

    def flush_all(self):
        for page_id, page in list(self.buffer.items()):
            if page.is_dirty:
                self.page_manager.write_page(page)
                page.is_dirty = False

    def allocate_page(self) -> Page:
        page = self.page_manager.allocate_page()
        self.buffer[page.page_id] = page
        return page

    def free_page(self, page_id: int):
        if page_id in self.buffer:
            del self.buffer[page_id]
        self.page_manager.free_page(page_id)

    def get_stats(self) -> Dict[str, int]:
        total = self.hit_count + self.miss_count
        hit_ratio = self.hit_count / total if total > 0 else 0

        return {
            'hit_count': self.hit_count,
            'miss_count': self.miss_count,
            'hit_ratio': hit_ratio,
            'buffer_size': len(self.buffer)
        }