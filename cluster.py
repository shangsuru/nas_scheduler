import config
from log import logger
from dl_job import DLJob
from typing import List, Tuple


class Cluster:
    def __init__(self) -> None:
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
    def num_nodes(self) -> int:
        """The number of computing nodes in the cluster."""
        return len(self.nodes)

    @property
    def nodes(self) -> List[str]:
        """The list of computing nodes in the cluster."""
        return self._nodes

    @nodes.setter
    def nodes(self, nodes: List[str]) -> None:
        self._nodes = nodes

    @property
    def num_cpu(self) -> int:
        """The amount of central processing units in the cluster."""
        return self.num_nodes * config.CPU_PER_NODE

    @num_cpu.setter
    def num_cpu(self, num_cpu: int) -> None:
        """The amount of central processing units in the cluster."""
        self._num_cpu = num_cpu

    @property
    def num_mem(self) -> int:
        """The amount of memory in the cluster. TODO: unit?"""
        return self.num_nodes * config.MEM_PER_NODE

    @property
    def num_bw(self) -> int:
        """The amount of bandwidth in the cluster. TODO: unit?"""
        return self.num_nodes * config.BW_PER_NODE

    @property
    def num_gpu(self) -> int:
        """The amount of graphical processing units in the cluster."""
        return self.num_nodes * config.GPU_PER_NODE

    def add_node(self, node: str) -> None:
        """Adds a node to the cluster at run-time.

        Args:
            node (str): The node to add to the cluster.
        """
        self.nodes.append(node)
        self.node_used_cpu_list.append(0)
        self.node_used_mem_list.append(0)
        self.node_used_bw_list.append(0)
        self.node_used_gpu_list.append(0)

    def remove_node(self, node: str) -> None:
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

    def _set_cluster_config(self) -> None:
        """
        Sets the cluster details, such as nodes, memory, bandwidth and gpus.
        """
        self.nodes = config.NODE_LIST

    def get_node_index(self, node: str) -> int:
        """Get the index of a node in the nodes list.

        Args:
            node (str): The node to return the index for.
        Returns:
            The index of the node int the nodes list.
        """
        return self.nodes.index(node)

    def check_cluster_resource_full(self, cpu_req: int, mem_req: int, bw_req: int = 0, gpu_req: int = 0) -> bool:
        """
        Check whether cluster resources are sufficient.

        Args:
            cpu_req (int): number of cpus needed
            mem_req (int): amount of memory needed
            bw_req (int): amount of bandwidth needed (default=0)
            gpu_req (int): number of gpu cards needed (default=0)

        Returns:
            bool: True if available resources are sufficient for the job, False otherwise.
        """
        return not any(
            [
                self.used_cpu + cpu_req > self.num_cpu,
                self.used_mem + mem_req > self.num_mem,
                self.used_bw + bw_req > self.num_bw,
                self.used_gpu + gpu_req > self.num_gpu,
            ]
        )

    def check_node_resource_full(
        self, node_id: int, cpu_req: int, mem_req: int, bw_req: int = 0, gpu_req: int = 0, num: int = 1
    ) -> bool:
        """
        Check whether resources on a given node is full.

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
        return not any(
            [
                self.node_used_cpu_list[node_id] + num * cpu_req > config.CPU_PER_NODE,
                self.node_used_mem_list[node_id] + num * mem_req > config.MEM_PER_NODE,
                self.node_used_bw_list[node_id] + num * bw_req > config.BW_PER_NODE,
                self.node_used_gpu_list[node_id] + num * gpu_req > config.BW_PER_NODE,
            ]
        )

    def assign_resources(self, job: DLJob, task_type: str, task_num: int, node_id: int) -> None:
        """
        Assign available resources to a node for a given job.

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

    def free_resources(self, job: DLJob, task_type: str, task_num: int, node_id: int) -> None:
        """
        Assign available resources to a node for a given job.

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

    def sort_nodes(self, resource: str) -> List[Tuple[int, int]]:
        """
        Sort nodes based on available resource.
        Args:
            resource (str): name of the resource. e.g. gpu, cpu
        Returns:
            List containing nodes with descending resources
        """
        sorted_list = []
        for i in range(self.num_nodes):
            if resource == "cpu":
                sorted_list.append((self.node_used_cpu_list[i], i))
            elif resource == "gpu":
                sorted_list.append((self.node_used_gpu_list[i], i))
            elif resource == "mem":
                sorted_list.append((self.node_used_mem_list[i], i))
        sorted_list.sort(key=lambda x: x[0])
        return sorted_list

    def get_available_resources(self, node_index: int) -> List[float]:
        """
        Sort nodes based on available resource.
        Args:
            node_index (str): index of the node
        Returns:
            dictionary containing the amount of unused resources on the node
        """
        unused_cpu = config.CPU_PER_NODE - self.node_used_cpu_list[node_index]
        unused_memory = config.MEM_PER_NODE - self.node_used_mem_list[node_index]
        unused_bw = config.BW_PER_NODE - self.node_used_bw_list[node_index]
        unused_gpu = config.GPU_PER_NODE - self.node_used_gpu_list[node_index]
        return [unused_cpu, unused_memory, unused_bw, unused_gpu]
