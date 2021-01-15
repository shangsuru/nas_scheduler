import asyncio
import time

from k8s.api import KubeAPI
from log import logger

k8s_api = KubeAPI()


class Heartbeat:
    """A heartbeat handler that regularly checks the availability of k8s as well as the pods.

    Attributes:
        cluster: The cluster of pods managing resources.
        heartbeat_interval: The interval (in sec.) in which to send out heartbeats.
        last_beat: The time when the last heartbeat was emitted.
        scheduler: The NAS scheduler managing jobs.
    """

    heartbeat_interval = 10

    def __init__(self, scheduler, cluster, daemon=True):
        self.cluster = cluster
        self.scheduler = scheduler
        self.last_beat = 0

    async def on_iteration(self):
        """Emits a heartbeat if the last heartbeat was at least heartbeat_interval seconds ago."""
        if time.perf_counter() - self.last_beat < self.heartbeat_interval:
            return

        await self.heartbeat()
        self.last_beat = time.perf_counter()

    async def heartbeat(self):
        """Performs a single heartbeat and updates the cluster and scheduler accordingly.

        We detect dead nodes by checking whether the phase of all pods on the node is set to "Failed".
        (See: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase)
        """

        nodes = k8s_api.get_nodes()
        erroneous_pods = []     # List of pod names (pod.metadata.name) for pods that have failed.
        erroneous_nodes = []    # List of kubernetes node objects for nodes that have failed.

        # Collect nodes that may have experienced an error as well as erroneous pods.
        for node in nodes:
            pods = k8s_api.get_pods(field_selector={"spec.nodeName": node.metadata.name})
            node_failed = True

            for pod in pods:
                if pod.status.phase != "Failed":
                    node_failed = False
                    continue

                if pod.metadata.name in erroneous_pods:
                    continue

                erroneous_pods.append(pod.metadata.name)

            if not node_failed:
                continue

            erroneous_nodes.append(node)

        # Handle erroneous pods
        for pod in erroneous_pods:
            logger.warn(f"Pod {pod} has failed.")

            # Remove jobs that run on the pod from scheduler (running & uncompleted) so they are not rescheduled.

            for job in self.scheduler.uncompleted_jobs:
                job.__get_pods_names()

                if pod not in job.worker_pods:
                    continue

                self.scheduler.remove_job(job, reschedule=False)

        # Handle erroneous nodes
        for node in erroneous_nodes:
            logger.warn(f"Node {node.metadata.name} has failed.")

            # Retrieve node address
            try:
                address = next(address for address in node.status.addresses if address.type == "InternalIP").address
            except KeyError:
                logger.error(f"Failed to get internal IP address of node {node.metadata.name}!")
                continue

            if address not in self.cluster.nodes:
                logger.warn(f"Node {node.metadata.name} ({address}) has failed but is not in the cluster nodes list.")
                continue

            # Remove jobs running on node from running jobs in scheduler
            # They will then be rescheduled in the next scheduling round.
            for job in self.scheduler.uncompleted_jobs:
                if address not in job.worker_placement:
                    continue

                self.scheduler.remove_job(job)

            # Remove node from cluster
            self.cluster.remove_node(address)
