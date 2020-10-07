import config
from communication import hub, Payload
from schedulers.scheduler_base import SchedulerBase
from allocators.default_allocator import DefaultAllocator

from log import logger


class FIFOScheduler(SchedulerBase):
    def __init__(self, cluster, timer):
        """
        Args:
            timer (Timer): timer instance
        """
        super().__init__(cluster, timer)
        self.module_name = 'fifo_scheduler'
        self.allocator = DefaultAllocator(cluster)
        self.start()

    def _schedule(self):
        pass