import config
import queue
from communication import hub, Payload
from schedulers.scheduler_base import SchedulerBase
from allocators.default_allocator import DefaultAllocator

from log import logger


class DRFScheduler(SchedulerBase):
    def __init__(self, cluster, timer):
        """
        Args:
            timer (Timer): timer instance
        """
        super().__init__(cluster, timer)
        self.allocator = DefaultAllocator(cluster)
        self.start()

    def _schedule(self):
        drf_queue = queue.PriorityQueue()
        for job in self.uncompleted_jobs:
            drf_queue.put((0, job.arrival_time, job))

        self.cluster.used_cpu = 0
        self.cluster.used_mem = 0
        self.cluster.used_bw = 0
        self.cluster.used_gpu = 0

        while not drf_queue.empty():
            (_, job_arrival, job) = drf_queue.get()

            cpu_req = job.resources.ps.ps_cpu + job.resources.worker.worker_cpu
            mem_req = job.resources.ps.ps_mem + job.resources.worker.worker_mem
            bw_req = job.resources.ps.ps_bw + job.resources.worker.worker_bw
            gpu_req = job.resources.worker.worker_gpu

            suff_resr = self.cluster.check_cluster_resource_full(cpu_req, mem_req, bw_req, gpu_req)
            if suff_resr:
                job.resources.ps.num_ps += 1
                job.resources.worker.num_worker += 1
                self.cluster.used_cpu += cpu_req
                self.cluster.used_mem += mem_req
                self.cluster.used_bw += bw_req
                self.cluster.used_gpu += gpu_req
                dom_share = (job.resources.worker.num_worker * job.resources.worker.worker_cpu + job.resources.ps.num_ps * job.resources.ps.ps_cpu) / self.cluster.num_cpu
                dom_share = max(dom_share, (job.resources.worker.num_worker * job.resources.worker.worker_gpu) / self.cluster.num_gpu)
                if job.resources.ps.num_ps < config.MAX_NUM_WORKERS and job.resources.worker.num_worker < config.MAX_NUM_WORKERS:
                    drf_queue.put((dom_share, job_arrival, job))
            else:
                break
