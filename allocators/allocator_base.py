import abc
import time
from log import logger
from queue import PriorityQueue


class ResourceAllocator(metaclass=abc.ABCMeta):
    def __init__(self, cluster):
        self.cluster = cluster

    def free_job_resources(self, job):
        for node in job.ps_placement:
            self.cluster.free_resources(job, "ps", 1, self.cluster.get_node_index(node))
        for node in job.worker_placement:
            self.cluster.free_resources(job, "worker", 1, self.cluster.get_node_index(node))
        logger.debug("freed up job resources")

    def allocate(self, jobs):
        """
        Allocate resources for the given jobs.
        We place jobs in increasing order of their resource demand (i.e., smallest job first) in order
        to avoid job starvation. If the job uses the allreduce distribution strategy, we know that num_ps == 0.
        We try allocating resources until all jobs are placed or until there aren't enough resources left
        to allocate a job.
        Note that the number of jobs the servers can accommodate might be smaller than the number of
        jobs we allocate resource to through the resource allocation algorithm (which considers overall
        resource capacity in the entire cluster).
        Jobs which are not placed will be temporarily paused and rescheduled in the next scheduling interval.

        Args:
            jobs (list[dl_job]): list of jobs to be allocated on the resources.
        Returns:
            ps_placements (dict of int: string): mapping of job.uid to the nodes used for allocating the ps
            worker_placements (list of int): mapping of job.uid to the nodes used for allocating the workers
        """
        tic = time.time()

        # remove unscheduled jobs (jobs with num_ps/num_worker==0 should simply be ignored)
        jobs = [job for job in jobs if not (job.resources.worker.num_worker == 0 and job.resources.ps.num_ps == 0)]

        # sort jobs ascending based on num_ps and num_worker (smallest job first)
        sorted_job_queue = PriorityQueue()
        for job in jobs:
            resource_usage = job.resources.ps.num_ps + job.resources.worker.num_worker
            sorted_job_queue.put((resource_usage, job))

        ps_placements = dict()
        worker_placements = dict()

        while not sorted_job_queue.empty():
            task_num, job = sorted_job_queue.get()
            # check if node resource can satisfy the job's resource requirements
            ps_nodes, worker_nodes = self.allocate_job(job)

            if (len(ps_nodes) > 0 or job.metadata.dist_strategy == "allreduce") and len(worker_nodes) > 0:
                # in this case the resources were successfully allocated
                ps_placements[job.uid] = [self.cluster.nodes[node] for node in ps_nodes]
                job.resources.ps.num_ps = len(ps_placements[job.uid])
                worker_placements[job.uid] = [self.cluster.nodes[node] for node in worker_nodes]
                job.resources.worker.num_worker = len(worker_placements[job.uid])

        logger.debug(f"used cpu: {self.cluster.node_used_cpu_list}")
        toc = time.time()
        logger.info(f"Finished job placement in {toc - tic:.3f} seconds")

        return ps_placements, worker_placements

    @abc.abstractmethod
    def allocate_job(self, job):
        pass
