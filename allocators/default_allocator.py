import time
from queue import PriorityQueue

from .allocator_base import ResourceAllocator
from log import logger


class DefaultAllocator(ResourceAllocator):

    def __init__(self, cluster):
        super().__init__(cluster)

    def free_job_resources(self, job):
        for node in job.ps_placement:
            self.cluster.free_resources(job, "ps", 1, self.cluster.get_node_index(node))
        for node in job.worker_placement:
            self.cluster.free_resources(job, "worker", 1, self.cluster.get_node_index(node))
        logger.debug(f'[{self.module_name}] freed up job resources')

    def allocate(self, jobs):
        """Allocate resources for the given jobs.
        Given the numbers of workers and parameter servers in a synchronous training job, 
        the optimal worker/parameter server placement principle to achieve the maximal training speed 
        for the job, in a cluster of homogeneous servers, is to use the smallest number of servers to 
        host the job, such that the same number of parameter servers and the same number of workers 
        are deployed on each of these servers.

        We sort all servers in the cluster in descending order of their current resource availability 
        (available CPU capacity is used in our experiments). We place jobs in increasing order oftheir 
        resource demand (i.e., smallest job first) in order to avoid job starvation. For each job, we 
        check whether the resources on the first k servers are sufficient to host the job (starting with k = 1).
        If so, we place parameter servers and workers in the job evenly on the k servers; otherwise, 
        we check the first k + 1,k + 2,... servers until we find enough servers to place the job. 
        We then update available resources on the k servers and sort the server list again. The above 
        procedure repeats until all jobs are placed or no sufficient resources on the servers are left to 
        host more jobs. 
        Note that the number of jobs the servers can accommodate might be smaller than the number of 
        jobs we allocate resource to through the resource allocation algorithm (which considers overall 
        resource capacity in the entire cluster). 
        Jobs which are not placed will be temporarily paused and rescheduled in the next scheduling interval.

        Args:
            jobs (list[DLJobs]): list of jobs to be allocated on the resources.
        """
        tic = time.time()

        # sort jobs based on num_ps and num_worker
        job_sort_queue = PriorityQueue()
        for job in jobs:
            job_sort_queue.put(((job.resources.ps.num_ps + job.resources.worker.num_worker)*(-1), job))

        cpu_avail_queue = self.cluster.sort_nodes('cpu')

        ps_placements = dict()
        worker_placements = dict()

        while not job_sort_queue.empty():
            task_num, job = job_sort_queue.get()
            # check if node resource can satisfy the job's resource requirements
            cand_place_nodes = []
            while not cpu_avail_queue.empty():
                used_cpus, node_index = cpu_avail_queue.get()
                cand_place_nodes.append(node_index)

                # try to place the job on cand_place_nodes
                fit_flag = True  # whether these nodes can hold the job
                ps_nodes = []
                ps_already_deduct = False

                for i in range(job.resources.ps.num_ps):
                    # place ps evenly
                    node = cand_place_nodes[i % len(cand_place_nodes)]
                    # check whether resource is enough to place this ps
                    suff_resr = self.cluster.check_node_resource_full(node, job.resources.ps.ps_cpu,
                                                                      job.resources.ps.ps_mem,
                                                                      job.resources.ps.ps_bw)
                    if suff_resr:
                        ps_nodes.append(node)
                        # minus temporary resources
                        self.cluster.assign_resources(job, "ps", 1, node)
                    else:
                        # since node is already sorted based on resources,
                        # if a larger node can not place the task, the following one can not too
                        fit_flag = False
                        # add the deducted resource back
                        for node in ps_nodes:
                            self.cluster.free_resources(job, "ps", 1, node)
                        ps_already_deduct = True
                        break

                worker_nodes = []
                for i in range(job.resources.worker.num_worker):
                    # also place worker evenly
                    node = cand_place_nodes[i % len(cand_place_nodes)]
                    # check whether resource is enough to place this ps
                    suff_resr = self.cluster.check_node_resource_full(node, job.resources.worker.worker_cpu,
                                                                      job.resources.worker.worker_mem,
                                                                      job.resources.worker.worker_bw,
                                                                      job.resources.worker.worker_gpu)
                    if suff_resr:
                        worker_nodes.append(node)
                        self.cluster.assign_resources(job, "worker", 1, node)
                    else:
                        fit_flag = False

                        # add the deducted resource back
                        for node in worker_nodes:
                            self.cluster.free_resources(job, "worker", 1, node)
                        if not ps_already_deduct:
                            for node in ps_nodes:
                                self.cluster.free_resources(job, "ps", 1, node)
                        break

                if fit_flag:
                    ps_placements[job.uid] = [self.cluster.nodes[node] for node in ps_nodes]
                    worker_placements[job.uid] = [self.cluster.nodes[node] for node in worker_nodes]
                    for node in cand_place_nodes:  # enqueue them back
                        cpu_avail_queue.put((self.cluster.node_used_cpu_list[node], node))
                    break
                else:
                    if not cpu_avail_queue.empty():
                        # add one more node to see if the job can be fitted
                        continue
                    else:
                        # have try all nodes, but still can not place, then check if we can place some tasks
                        # and place ps and worker alternatively
                        logger.debug(f'[{self.module_name}] last placed job: {job.name}')
                        ps_nodes = []
                        worker_nodes = []
                        flag_place_ps = True
                        for i in range(job.resources.ps.num_ps + job.resources.worker.num_worker):
                            flag_no_resource = True
                            if flag_place_ps:
                                # place ps task
                                for node in range(len(self.cluster.nodes)):
                                    suff_resr = self.cluster.check_node_resource_full(node, job.resources.ps.ps_cpu,
                                                                                      job.resources.ps.ps_mem,
                                                                                      job.resources.ps.ps_bw)
                                    if suff_resr:
                                        ps_nodes.append(node)
                                        self.cluster.assign_resources(job, "ps", 1, node)
                                        flag_no_resource = False
                                        break
                            else:
                                # place worker task
                                for node in range(len(self.cluster.nodes)):
                                    suff_resr = self.cluster.check_node_resource_full(node,
                                                                                      job.resources.worker.worker_cpu,
                                                                                      job.resources.worker.worker_mem,
                                                                                      job.resources.worker.worker_bw,
                                                                                      job.resources.worker.worker_gpu)
                                    if suff_resr:
                                        worker_nodes.append(node)
                                        self.cluster.assign_resources(job, "worker", 1, node)
                                        flag_no_resource = False
                                        break
                            if flag_no_resource:
                                break
                            flag_place_ps = not flag_place_ps  # change to place the other task
                            if len(ps_nodes) >= job.resources.ps.num_ps:  # all ps tasks have been placed
                                flag_place_ps = False
                            if len(worker_nodes) >= job.resources.worker.num_worker:  # all workers have been placed
                                flag_place_ps = True

                        if len(ps_nodes) > 0 and len(worker_nodes) > 0:
                            ps_placements[job.uid] = [self.cluster.nodes[node] for node in ps_nodes]
                            job.resources.ps.num_ps = len(ps_placements[job.uid])
                            worker_placements[job.uid] = [self.cluster.nodes[node] for node in worker_nodes]
                            job.resources.worker.num_worker = len(worker_placements[job.uid])
                        else:
                            for node in ps_nodes:
                                self.cluster.free_resources(job, "ps", 1, node)
                            for node in worker_nodes:
                                self.cluster.free_resources(job, "worker", 1, node)
                        # break the while loop
                        break

        logger.debug(f'[{self.module_name}] used cpu: {self.cluster.node_used_cpu_list}')
        toc = time.time()
        logger.info(f'[{self.module_name}] Finished job placement in {toc - tic:.3f} seconds')

        return ps_placements, worker_placements
