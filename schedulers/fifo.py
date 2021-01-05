import config
import queue
from schedulers.scheduler_base import SchedulerBase
from allocators.default_allocator import DefaultAllocator

from log import logger


class FIFOScheduler(SchedulerBase):
    def __init__(self, cluster, timer):
        super().__init__(cluster)
        self.allocator = DefaultAllocator(cluster)
        self.name = 'FIFO'

    def _schedule(self):
        fifo_queue = queue.PriorityQueue()
        for job in self.uncompleted_jobs:
            fifo_queue.put((job.arrival_time, job))

        self.cluster.used_cpu = 0
        self.cluster.used_mem = 0
        self.cluster.used_bw = 0
        self.cluster.used_gpu = 0

        flag = False
        while not fifo_queue.empty():
            (_, job) = fifo_queue.get()

            cpu_req = job.resources.ps.ps_cpu + job.resources.worker.worker_cpu
            mem_req = job.resources.ps.ps_mem + job.resources.worker.worker_mem
            bw_req = job.resources.ps.ps_bw + job.resources.worker.worker_bw
            gpu_req = job.resources.worker.worker_gpu

            for i in range(config.MAX_NUM_WORKERS):
                suff_resr = self.cluster.check_cluster_resource_full(cpu_req, mem_req, bw_req, gpu_req)
                if suff_resr:
                    job.resources.ps.num_ps += 1
                    job.resources.worker.num_worker += 1
                    self.cluster.used_cpu += cpu_req
                    self.cluster.used_mem += mem_req
                    self.cluster.used_bw += bw_req
                    self.cluster.used_gpu += gpu_req
                else: # try next job before quitting
                    flag = True
                    break
            if flag:
                break
