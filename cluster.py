from queue import PriorityQueue

import config
from log import logger

class Cluster():
    def __init__(self):
        self.nodes = config.NODE_LIST
        self.used_cpu = 0
        self.used_mem = 0
        self.used_gpu = 0
        self.used_bw = 0

        self.node_used_cpu_list = [0 for i in range(self.num_nodes)]
        self.node_used_mem_list = [0 for i in range(self.num_nodes)]
        self.node_used_bw_list = [0 for i in range(self.num_nodes)]
        self.node_used_gpu_list = [0 for i in range(self.num_nodes)]

    @property
    def num_nodes(self):
        """The number of computing nodes in the cluster."""
        return len(self.nodes)

    @property
    def nodes(self):
        """The list of computing nodes in the cluster."""
        return self._nodes

    @nodes.setter
    def nodes(self, nodes):
        self._nodes = nodes

    @property
    def num_cpu(self):
        """The amount of central processing units in the cluster."""
        return self.num_nodes * config.CPU_PER_NODE

    @property
    def num_mem(self):
        """The amount of memory in the cluster. TODO: unit?"""
        return self.num_nodes * config.MEM_PER_NODE

    @property
    def num_bw(self):
        """The amount of bandwidth in the cluster. TODO: unit?"""
        return self.num_nodes * config.BW_PER_NODE

    @property
    def num_gpu(self):
        """The amount of graphical processing units in the cluster."""
        return self.num_nodes * config.GPU_PER_NODE

    def add_node(self, node):
        """Adds a node to the cluster at run-time.

        Args:
            node (str): The node to add to the cluster.
        """
        self.nodes.append(node)
        self.node_used_cpu_list.append(0)
        self.node_used_mem_list.append(0)
        self.node_used_bw_list.append(0)
        self.node_used_gpu_list.append(0)

    def remove_node(self, node):
        """Removes a node from the cluster at run-time.

        Args:
            node (str): The node to remove from the cluster.
        """
        try:
            node_index = self.get_node_index(node)
        except ValueError as e:
            logger.error("Tried to take a non-online node offline")
            return

        self.nodes.pop(node_index)
        self.node_used_cpu_list.pop(node_index)
        self.node_used_mem_list.pop(node_index)
        self.node_used_bw_list.pop(node_index)
        self.node_used_gpu_list.pop(node_index)

    def get_node_index(self, node):
        """Get the index of a node in the nodes list.

        Args:
            node (str): The node to return the index for.
        Returns:
            The index of the node int the nodes list.
        """
        return self.nodes.index(node)

    def check_cluster_resource_full(self, cpu_req, mem_req, bw_req=0, gpu_req=0):
        """Check whether cluster resources are sufficient.

        Args:
            cpu_req (int): number of cpus needed
            mem_req (int): amount of memory needed
            bw_req (int): amount of bandwidth needed (default=0)
            gpu_req (int): number of gpu cards needed (default=0)

        Returns:
            bool: True if available resources are sufficient for the job, False otherwise.
        """
        return not any([self.used_cpu + cpu_req > self.num_cpu,
            self.used_mem + mem_req > self.num_mem,
            self.used_bw + bw_req > self.num_bw,
            self.used_gpu + gpu_req > self.num_gpu
            ])

    def check_node_resource_full(self, node_id, cpu_req, mem_req, bw_req=0, gpu_req=0):
        """Check whether resources on a given node is full.

        Args:
            node_id (int): index of the target node
            cpu_req (int): number of cpus needed
            mem_req (int): amount of memory needed
            bw_req (int): amount of bandwidth needed (default=0)
            gpu_req (int): number of gpu cards needed (default=0)

        Returns:
            bool: True if available resources are sufficient for the job, False otherwise.
        """
        return not any([self.node_used_cpu_list[node_id] + cpu_req > config.CPU_PER_NODE,
            self.node_used_mem_list[node_id] + mem_req > config.MEM_PER_NODE,
            self.node_used_bw_list[node_id] + bw_req > config.BW_PER_NODE,
            self.node_used_gpu_list[node_id] + gpu_req > config.BW_PER_NODE
            ])

    def assign_resources(self, job, task_type, task_num, node_id):
        """Assign available resources to a node for a given job.

        Args:
            job (DLJob): Job instance
            task_type (str): type of task, i.e. ps or worker
            task_num (int): number of replicas for the task
            node_id (int): node index in the cluster
        """

        if task_type == "ps":
            self.node_used_cpu_list[node_id] += job.resources.ps.ps_cpu * task_num
            self.node_used_mem_list[node_id] += job.resources.ps.ps_mem * task_num
            self.node_used_bw_list[node_id] += job.resources.ps.ps_bw * task_num
        elif task_type == "worker":
            self.node_used_cpu_list[node_id] += job.resources.worker.worker_cpu * task_num
            self.node_used_mem_list[node_id] += job.resources.worker.worker_mem * task_num
            self.node_used_bw_list[node_id] += job.resources.worker.worker_bw * task_num
            self.node_used_gpu_list[node_id] += job.resources.worker.worker_gpu * task_num

    def free_resources(self, job, task_type, task_num, node_id):
        """Assign available resources to a node for a given job.

        Args:
            job (DLJob): Job instance
            task_type (str): type of task, i.e. ps or worker
            task_num (int): number of replicas for the task
            node_id (int): node index in the cluster
        """
        # add resources on the node
        if task_type == "ps":
            self.node_used_cpu_list[node_id] -= job.resources.ps.ps_cpu * task_num
            self.node_used_mem_list[node_id] -= job.resources.ps.ps_mem * task_num
            self.node_used_bw_list[node_id] -= job.resources.ps.ps_bw * task_num
        elif task_type == "worker":
            self.node_used_cpu_list[node_id] -= job.resources.worker.worker_cpu * task_num
            self.node_used_mem_list[node_id] -= job.resources.worker.worker_mem * task_num
            self.node_used_bw_list[node_id] -= job.resources.worker.worker_bw * task_num
            self.node_used_gpu_list[node_id] -= job.resources.worker.worker_gpu * task_num

    def sort_nodes(self, resource):
        """Sort nodes based on available resource.
        Args:
            resource (str): name of the resource. e.g. gpu, cpu
        """
        sorted_queue = PriorityQueue()
        for i in range(self.num_nodes):
            if resource == 'cpu':
                sorted_queue.put((self.node_used_cpu_list[i], i))
            elif resource == 'gpu':
                sorted_queue.put((self.node_used_gpu_list[i], i))
            elif resource == 'mem':
                sorted_queue.put((self.node_used_mem_list[i], i))

        return sorted_queue



