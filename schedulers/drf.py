import config
import queue
from allocators.default_allocator import DefaultAllocator
from schedulers.scheduler_base import SchedulerBase


class DRFScheduler(SchedulerBase):
    def __init__(self, cluster):
        super().__init__(cluster)
        self.allocator = DefaultAllocator(cluster)
        self.name = "drf_scheduler"

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

            req = job.get_required_resources_per_node()

            if self.cluster.check_cluster_resource_full(req["cpu"], req["mem"], req["bw"], req["gpu"]):
                job.increment_num_instances()
                self.cluster.used_cpu += req["cpu"]
                self.cluster.used_mem += req["mem"]
                self.cluster.used_bw += req["bw"]
                self.cluster.used_gpu += req["gpu"]
                # computes the job's share of its dominant resource (cpu/gpu) within the cluster
                dom_share = (
                    job.resources.worker.num_worker * job.resources.worker.worker_cpu
                    + job.resources.ps.num_ps * job.resources.ps.ps_cpu
                ) / self.cluster.num_cpu
                dom_share = max(
                    dom_share,
                    (job.resources.worker.num_worker * job.resources.worker.worker_gpu) / self.cluster.num_gpu,
                )
                if (
                    job.resources.ps.num_ps < config.MAX_NUM_WORKERS
                    and job.resources.worker.num_worker < config.MAX_NUM_WORKERS
                ):
                    drf_queue.put((dom_share, job_arrival, job))
            else:
                break
