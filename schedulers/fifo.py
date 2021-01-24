import config
import queue
from allocators.default_allocator import DefaultAllocator
from schedulers.scheduler_base import SchedulerBase


class FIFOScheduler(SchedulerBase):
    def __init__(self, cluster):
        super().__init__(cluster)
        self.allocator = DefaultAllocator(cluster)
        self.name = "fifo_scheduler"

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

            req = job.get_required_resources_per_node()

            for i in range(config.MAX_NUM_WORKERS):
                if self.cluster.check_cluster_resource_full(req["cpu"], req["mem"], req["bw"], req["gpu"]):
                    job.increment_num_instances()
                    self.cluster.used_cpu += req["cpu"]
                    self.cluster.used_mem += req["mem"]
                    self.cluster.used_bw += req["bw"]
                    self.cluster.used_gpu += req["gpu"]
                else:  # try next job before quitting
                    # Caution: Comment in line above is wrong. Once else is entered scheduling is quit.
                    flag = True
                    break
            if flag:
                break
