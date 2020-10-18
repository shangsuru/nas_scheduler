import config
import queue
from communication import hub, Payload
from schedulers.scheduler_base import SchedulerBase
from allocators.default_allocator import DefaultAllocator
import numpy as np

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
        fifo_queue = queue.PriorityQueue()
        for job in self.uncompleted_jobs:
            fifo_queue.put((job.arrv_time, job))

        flag = False
        while not fifo_queue.empty():
            (_, job) = fifo_queue.get()

            for i in range(job.resources.ps.num_ps + job.resources.worker.num_worker):

                if job.type == "ps":
                    cpu_req = job.resources.ps.ps_cpu
                    mem_req = job.resources.ps.ps_mem
                    bw_req = job.resources.ps.ps_bw
                    gpu_req = 0
                elif job.type == "worker":
                    cpu_req = job.worker_cpu
                    mem_req = job.worker_mem
                    bw_req = job.worker_bw
                    gpu_req = job.worker_gpu

                suff_resr = self.cluster.check_cluster_resource_full(cpu_req, mem_req, bw_req, gpu_req)
                if suff_resr:
                    if job.type == "ps":
                        job.resources.ps.num_ps += 1
                    elif job.type == "worker":
                        job.resources.ps.num_worker += 1
                    self.cluster.used_cpu += cpu_req
                    self.cluster.used_mem += mem_req
                    self.cluster.used_bw += bw_req
                    self.cluster.used_gpu += gpu_req
                else: # try next job before quitting
                    flag = True
                    continue
                if flag:
                    break


