import time
from queue import PriorityQueue
import numpy as np
from .allocator_base import ResourceAllocator
from log import logger


class DefaultAllocator(ResourceAllocator):
    def __init__(self, cluster):
        super().__init__(cluster)

    def allocate(self, jobs):
        """Allocate resources for the given jobs.
        We place jobs in increasing order of their resource demand (i.e., smallest job first) in order
        to avoid job starvation. If the job uses horovod, the job doesn't require parameter servers, which will
        be taken into account when sorting the jobs. We try allocating resources until all jobs are placed
        or until there aren't enough resources left to allocate a job.
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
        print("Started Allocator" + str(len(jobs)))
        # sort jobs ascending based on num_ps and num_worker (smallest job first)
        sorted_job_queue = PriorityQueue()
        for job in jobs:
            resource_usage = (
                job.resources.worker.num_worker
                if job.metadata.use_horovod
                else job.resources.ps.num_ps + job.resources.worker.num_worker
            )
            sorted_job_queue.put((resource_usage, job))

        ps_placements = dict()
        worker_placements = dict()

        while not sorted_job_queue.empty():
            task_num, job = sorted_job_queue.get()
            # check if node resource can satisfy the job's resource requirements
            ps_nodes, worker_nodes = self.allocate_job(job)

            if (len(ps_nodes) > 0 and len(worker_nodes) > 0):
                # in this case the resources were successfully allocated
                ps_placements[job.uid] = [self.cluster.nodes[node] for node in ps_nodes]
                job.resources.ps.num_ps = len(ps_placements[job.uid])
                worker_placements[job.uid] = [self.cluster.nodes[node] for node in worker_nodes]
                job.resources.worker.num_worker = len(worker_placements[job.uid])
            else:  # Job didn't fit
                break

        logger.debug(f"used cpu: {self.cluster.node_used_cpu_list}")
        toc = time.time()
        logger.info(f"Finished job placement in {toc - tic:.3f} seconds")

        return ps_placements, worker_placements

    def allocate_job(self, job):
        """Allocate resources for the given job.
        Given the numbers of workers and parameter servers in a synchronous training job,
        the optimal worker/parameter server placement principle to achieve the maximal training speed
        for the job, in a cluster of homogeneous servers, is to use the smallest number of servers to
        host the job, such that the same number of parameter servers and the same number of workers
        are deployed on each of these servers.

        We sort all servers in the cluster in descending order of their current resource availability
        (available CPU capacity is used in our experiments). Firstly, we check whether the resources
        on the first k servers are sufficient to host the job (starting with k = 1).
        If so, we place parameter servers and workers in the job evenly on the k servers; otherwise,
        we check the first k + 1,k + 2,... servers until we find enough servers to place the job.
        If the resources for a job cannot be allocated using the maximum amount of servers available, ps_nodes
        and worker_nodes are returned empty.

        Args:
            job (dl_job): job to be allocated
        Returns:
            ps_nodes (list of int): node indexes used to allocate the resources for the parameter servers
            worker_nodes (list of int): node indexes used to allocate the resources for the workers
        """
        ps_nodes = [], worker_nodes = []
        sorted_nodes_list = self.cluster.sort_nodes_list("cpu")
        available_resources = np.array(shape=(len(sorted_nodes_list), 4))
        for i in range(len(sorted_nodes_list)):
            used_cpus, node_index = sorted_nodes_list[i]
            available_resources.append([self.cluster.get_available_resources(node_index)], axis=0)

        required_resources = job.get_total_required_resources()

        min_req_nodes = np.ceil(np.array([required_resources[0] / self.config.CPU_PER_NODE,
                    required_resources[1] / self.config.MEM_PER_NODE, required_resources[2] / self.config.BW_PER_NODE,
                    required_resources[3] / self.config.GPU_PER_NODE]))

        # Now we can compute the amount of nodes we at least need to host the job, this saves unnecessary computations
        minimal_required_node_amount = np.max(min_req_nodes)

        for node_amount in range(minimal_required_node_amount, len(sorted_nodes_list) + 1):
            limit = max(job.resources.ps.num_ps, job.resources.worker.num_worker)
            available_resources_minus_job_resources = available_resources.copy()
            for i in range(limit):
                condition_worker = i < job.resources.worker.num_worker
                if condition_worker:
                    available_resources_minus_job_resources[i % node_amount] -= np.array([
                        job.resources.worker.worker_cpu, job.resources.worker.worker_mem,
                        job.resources.worker.worker_bw, job.resources.worker.worker_cpu])

                condition_ps = (i < job.resources.ps.num_ps and not job.metadata.use_horovod)
                if condition_ps:  # if horovod is used we don't need the ps
                    available_resources_minus_job_resources[i % node_amount] -= np.array([job.resources.ps.ps_cpu,
                            job.resources.ps.ps_mem, job.resources.ps.ps_bw, 0])

                if np.min(available_resources_minus_job_resources) >= 0: # allocate the resources in the cluster
                    if condition_worker:
                        worker_nodes.append(sorted_nodes_list[i % node_amount])
                    if condition_ps:
                        ps_nodes.append(sorted_nodes_list[i % node_amount])
                elif node_amount != len(sorted_nodes_list):     # if we are using all nodes, we don't reset the progress
                    ps_nodes = []
                    worker_nodes = []
                    break  # no resources left to allocate another worker and ps, try with one more node

            if ((len(ps_nodes) == job.resources.ps.num_ps or not job.metadata.use_horovod)
                and len(worker_nodes) == job.resources.worker.num_worker or node_amount == len(sorted_nodes_list)):
                # in this case the job fits or we used the maximum node amount, so we allocate the resources
                for node in ps_nodes:
                    self.cluster.assign_resources(job, "ps", 1, node)
                for node in worker_nodes:
                    self.cluster.assign_resources(job, "worker", 1, node)
                break

        return ps_nodes, worker_nodes