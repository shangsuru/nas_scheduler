import numpy as np
from .allocator_base import ResourceAllocator


class DefaultAllocator(ResourceAllocator):
    def __init__(self, cluster):
        super().__init__(cluster)

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
            job (DLJob): job to be allocated
        Returns:
            ps_nodes (list of int): node indexes used to allocate the resources for the parameter servers
            worker_nodes (list of int): node indexes used to allocate the resources for the workers
        """
        ps_nodes = [], worker_nodes = []    # these will be filled with the used nodes for hosting the job
        sorted_nodes_list = self.cluster.sort_nodes("cpu")   # get nodes sorted by ascending cpu-resource capacity

        available_resources = np.array(shape=(len(sorted_nodes_list), 4))     # matrix to contain available resources
        for i in range(len(sorted_nodes_list)):
            used_cpus, node_index = sorted_nodes_list[i]
            # each row of the matrix available_resources contains the resources available for a node in the cluster
            # available_resources[0] e.g. is a list containing 4  ints (cpu, mem, bw, gpu)
            available_resources.append([self.cluster.get_available_resources(node_index)], axis=0)

        # Now we can compute the amount of nodes we at least need to host the job, this saves unnecessary computations
        minimal_required_node_amount = self.get_min_req_node_amount()

        for node_amount in range(minimal_required_node_amount, len(sorted_nodes_list) + 1):
            limit = max(job.resources.ps.num_ps, job.resources.worker.num_worker)
            available_resources_minus_job_resources = available_resources.copy()

            for i in range(limit):
                # in each iteration we will try allocating a worker and a ps while enough resources are left
                if i < job.resources.worker.num_worker:     # only enter this block if more workers were requested
                    res_usage = np.array([job.resources.worker.worker_cpu, job.resources.worker.worker_mem,
                                          job.resources.worker.worker_bw, job.resources.worker.worker_cpu])
                    available_resources_minus_job_resources[i % node_amount] -= res_usage   # update available resources

                if i < job.resources.ps.num_ps:             # only enter this block if more ps were requested
                    res_usage = np.array([job.resources.ps.ps_cpu, job.resources.ps.ps_mem, job.resources.ps.ps_bw, 0])
                    available_resources_minus_job_resources[i % node_amount] -= res_usage   # update available resources

                if np.min(available_resources_minus_job_resources) >= 0:  # allocate the resources in the cluster
                    if i < job.resources.worker.num_worker:
                        worker_nodes.append(sorted_nodes_list[i % node_amount])
                    if i < job.resources.ps.num_ps:
                        ps_nodes.append(sorted_nodes_list[i % node_amount])
                elif node_amount != len(sorted_nodes_list):  # if we are using all nodes, we don't reset the progress
                    ps_nodes = []           # empty the lists
                    worker_nodes = []
                    break  # no resources left to allocate another worker and ps, try with one more node

            if (len(ps_nodes) == job.resources.ps.num_ps and len(worker_nodes) == job.resources.worker.num_worker) or \
                    node_amount == len(sorted_nodes_list):
                # in this case the job fits or we used the maximum node amount, so we assign the resources
                for node in ps_nodes:
                    self.cluster.assign_resources(job, "ps", 1, node)
                for node in worker_nodes:
                    self.cluster.assign_resources(job, "worker", 1, node)
                break

        return ps_nodes, worker_nodes

    def get_min_req_node_amount(self, job):
        """Given a job, this method will return the amount of empty nodes required to host this job.
        Args:
            job (DLJob): job to be allocated
        Returns:
            int: minimal required node amount to host the job
        """
        required_resources = job.get_total_required_resources()

        min_req_nodes = np.ceil(np.array([required_resources['cpu'] / self.config.CPU_PER_NODE,
                                          required_resources['mem'] / self.config.MEM_PER_NODE,
                                          required_resources['bw'] / self.config.BW_PER_NODE,
                                          required_resources['gpu'] / self.config.GPU_PER_NODE]))

        return np.max(min_req_nodes)