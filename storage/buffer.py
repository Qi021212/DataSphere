# storage/buffer.py
from collections import OrderedDict
from typing import Optional, Dict
from storage.page import Page, PageManager
from utils.constants import BUFFER_POOL_SIZE
import logging # 用于（可选的）替换日志

# 配置一个 logger 用于缓存相关的日志（可选）
logger = logging.getLogger(__name__)

class BufferPool:
    def __init__(self, page_manager: PageManager, pool_size: int = BUFFER_POOL_SIZE, policy: str = 'LRU'):
        self.page_manager = page_manager
        self.pool_size = pool_size
        # 验证策略
        if policy not in ['LRU', 'FIFO']:
            raise ValueError(f"Unsupported policy: {policy}. Supported policies are 'LRU', 'FIFO'.")
        self.policy = policy
        # 使用 OrderedDict 实现 LRU/FIFO
        self.buffer: OrderedDict[int, Page] = OrderedDict()
        self.hit_count = 0
        self.miss_count = 0
        # 如果需要 LFU，可能需要额外的数据结构，如 {page_id: frequency}

    def get_page(self, page_id: int) -> Optional[Page]:
        """获取页面。如果页面在缓存中，则根据策略更新其位置（LRU）并增加命中计数；
           如果不在缓存中，则从磁盘加载，增加未命中计数，并管理缓存大小。
        """
        # 检查页面是否在缓存中
        if page_id in self.buffer:
            page = self.buffer[page_id]
            # 对于 LRU，在访问后移动到最近使用的位置
            if self.policy == 'LRU':
                self.buffer.move_to_end(page_id)
            # FIFO: 访问不改变顺序
            self.hit_count += 1
            # logger.debug(f"Cache HIT for page_id: {page_id}") # 可选日志
            return page

        # 未命中缓存
        # 从磁盘加载页面
        page = self.page_manager.read_page(page_id)
        if page is None:
            # logger.debug(f"Page {page_id} not found on disk.") # 可选日志
            return None

        self.miss_count += 1
        # logger.debug(f"Cache MISS for page_id: {page_id}. Loaded from disk.") # 可选日志

        # 添加到缓存
        self.buffer[page_id] = page

        # 如果缓存 *超过* 了容量限制，则移除最久未使用的页面
        # 添加 self.pool_size > 0 检查以处理 pool_size 为 0 或负数的边缘情况
        # 使用 > 而不是 >= 是标准行为：达到容量不驱逐，超过才驱逐
        if self.pool_size > 0 and len(self.buffer) > self.pool_size:
            # logger.debug("Cache exceeded capacity. Evicting a page...") # 可选日志
            self._evict_page() # 调用驱逐
            # logger.debug("Eviction completed.") # 可选日志

        # 对于 FIFO，新页直接加在末尾，无需 move_to_end
        # 对于 LRU，新页加在末尾，也是最近使用的，无需 move_to_end

        return page

    def _evict_page(self) -> Optional[int]:
        """根据指定策略驱逐一页。返回被驱逐的页ID，如果缓存为空则返回None。"""
        if not self.buffer:
            return None

        evicted_page_id = None
        page_to_evict = None

        if self.policy in ['LRU', 'FIFO']:
            # popitem(last=False) 移除并返回最早插入的项 (FIFO/LRU 驱逐点)
            evicted_page_id, page_to_evict = self.buffer.popitem(last=False)
        # elif self.policy == 'LFU':
        #     # 实现 LFU 逻辑：找到频率最低且最久未使用的页
        #     # ...
        #     pass
        # elif self.policy == 'Clock':
        #     # 实现 Clock 算法逻辑
        #     # ...
        #     pass
        else:
            # 这个分支理论上不会到达，因为 __init__ 已经检查过了
            raise ValueError(f"Unsupported policy during eviction: {self.policy}")

        # 如果被驱逐的页是脏页，则刷新到磁盘
        if page_to_evict and page_to_evict.is_dirty:
            # logger.debug(f"Flushing dirty page {evicted_page_id} to disk during eviction.") # 可选日志
            self.page_manager.write_page(page_to_evict)
            page_to_evict.is_dirty = False # 刷新后标记为干净

        return evicted_page_id


    def flush_page(self, page_id: int):
        """将指定的脏页刷新到磁盘，并标记为干净。"""
        if page_id in self.buffer:
            page = self.buffer[page_id]
            if page.is_dirty:
                self.page_manager.write_page(page)
                page.is_dirty = False
                # logger.debug(f"Flushed page {page_id} to disk.") # 可选日志

    def flush_all(self):
        """将所有脏页刷新到磁盘。"""
        for page_id, page in list(self.buffer.items()): # 使用 list() 避免在迭代时修改字典
            if page.is_dirty:
                self.page_manager.write_page(page)
                page.is_dirty = False
        # logger.debug("Flushed all dirty pages to disk.") # 可选日志

    def allocate_page(self) -> Page:
        # 分配新页由 PageManager 完成
        page = self.page_manager.allocate_page()

        # 将新页添加到缓存中
        self.buffer[page.page_id] = page
        page.is_dirty = True  # 新分配的页通常需要写回

        # 如果缓存 *超过* 了容量限制，则移除最久未使用的页面
        # 添加 self.pool_size > 0 检查以处理 pool_size 为 0 或负数的边缘情况
        if self.pool_size > 0 and len(self.buffer) > self.pool_size: # 保持 > 逻辑
            # logger.debug("Cache exceeded capacity upon allocation. Evicting a page...") # 可选日志
            self._evict_page() # 调用驱逐
            # logger.debug("Eviction completed.") # 可选日志

        # logger.debug(f"Allocated and cached new page_id: {page.page_id}") # 可选日志
        return page

    def free_page(self, page_id: int):
        """从缓存和磁盘中释放一个页。"""
        # 如果页在缓存中，先从缓存中移除
        if page_id in self.buffer:
            del self.buffer[page_id]
            # logger.debug(f"Freed page {page_id} from cache.") # 可选日志
        # 通知 PageManager 从磁盘释放
        self.page_manager.free_page(page_id)
        # logger.debug(f"Freed page {page_id} from disk.") # 可选日志

    def get_stats(self) -> Dict[str, int]:
        """获取缓存统计信息。"""
        total_requests = self.hit_count + self.miss_count
        hit_ratio = self.hit_count / total_requests if total_requests > 0 else 0
        return {
            'hit_count': self.hit_count,
            'miss_count': self.miss_count,
            'total_requests': total_requests,
            'hit_ratio': hit_ratio,
            'current_size': len(self.buffer),
            'max_size': self.pool_size,
            'policy': self.policy
        }

    # 重置统计信息
    def reset_stats(self):
        """重置命中和未命中计数器。"""
        self.hit_count = 0
        self.miss_count = 0
