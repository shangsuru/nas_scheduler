from queue import PriorityQueue

import config
from log import logger

class Cluster():
    def __init__(self):
        self.num_cpu = None
        self.num_mem = None
        self.num_bw = None
        self.num_gpu = None
        self.used_cpu = 0
        self.used_mem = 0
        self.used_gpu = 0
        self.used_bw = 0
        self._set_cluster_config()

        self.node_used_cpu_list = [0 for i in range(len(config.NODE_LIST))]
        self.node_used_mem_list = [0 for i in range(len(config.NODE_LIST))]
        self.node_used_bw_list = [0 for i in range(len(config.NODE_LIST))]
        self.node_used_gpu_list = [0 for i in range(len(config.NODE_LIST))]

    def _set_cluster_config(self):
        """Sets the cluster details, such as nodes, memory, bandwidth and gpus.
        """
        self.num_nodes = len(config.NODE_LIST)
        self.nodes = config.NODE_LIST
        cpu_per_node = config.CPU_PER_NODE
        mem_per_node = config.MEM_PER_NODE
        bw_per_node = config.BW_PER_NODE
        gpu_per_node = config.GPU_PER_NODE
        self.num_cpu = self.num_nodes * cpu_per_node
        self.num_mem = self.num_nodes * mem_per_node
        self.num_bw = self.num_nodes * bw_per_node
        self.num_gpu = self.num_nodes * gpu_per_node

    def get_node_index(self, node):
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

    def check_node_resource_full(self, node_id, cpu_req, mem_req, bw_req=0, gpu_req=0, num=1):
        """Check whether resources on a given node is full.

        Args:
            node_id (int): index of the target node
            cpu_req (int): number of cpus needed
            mem_req (int): amount of memory needed
            bw_req (int): amount of bandwidth needed (default=0)
            gpu_req (int): number of gpu cards needed (default=0)
            num (int): amount of workers of parameter servers to be placed on node (default=1)
        Returns:
            bool: True if available resources are sufficient for the job, False otherwise.
        """
        return not any([self.node_used_cpu_list[node_id] + num*cpu_req > config.CPU_PER_NODE,
            self.node_used_mem_list[node_id] + num*mem_req > config.MEM_PER_NODE,
            self.node_used_bw_list[node_id] + num*bw_req > config.BW_PER_NODE,
            self.node_used_gpu_list[node_id] + num*gpu_req > config.BW_PER_NODE
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

    def sort_nodes_queue(self, resource):
        """Sort nodes based on available resource.
        Args:
            resource (str): name of the resource. e.g. gpu, cpu
        Returns:
            PriorityQueue containing nodes with descending resources
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

    def sort_nodes_list(self, resource):
        """Sort nodes based on available resource.
        Args:
            resource (str): name of the resource. e.g. gpu, cpu
        Returns:
            List containing nodes with descending resources
        """
        sorted_list = []
        for i in range(self.num_nodes):
            if resource == 'cpu':
                sorted_list.append((self.node_used_cpu_list[i], i))
            elif resource == 'gpu':
                sorted_list.append((self.node_used_gpu_list[i], i))
            elif resource == 'mem':
                sorted_list.append((self.node_used_mem_list[i], i))

        return sorted_list.sort(key= lambda x: x[0])

    def get_available_resources(self, node_index):
        unused_cpu = self.config.CPU_PER_NODE - self.node_used_cpu_list[node_index]
        unused_memory = self.config.MEM_PER_NODE - self.cluster.node_used_mem_list[node_index]
        unused_bw = self.config.BW_PER_NODE - self.cluster.node_used_bw_list[node_index]
        unused_gpu = self.config.GPU_PER_NODE - self.cluster.node_used_gpu_list[node_index]
        return [unused_cpu, unused_memory, unused_bw, unused_gpu]



