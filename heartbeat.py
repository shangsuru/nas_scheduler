import asyncio
import time
import config

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

    heartbeat_interval = config.HEARTBEAT_INTERVAL_SEC

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

    @staticmethod
    def collect_failed_pods():
        """Collects all failed pods.

        Returns:
            A list of kubernetes pod objects that have failed.
        """
        pods = k8s_api.get_pods()
        failed_pods = []

        for pod in pods:
            if pod.status.phase == "Failed":
                failed_pods.append(pod)

        return failed_pods

    async def heartbeat(self):
        """Performs a single heartbeat for failed pods and updates the cluster and scheduler accordingly."""
        failed_pods = self.collect_failed_pods()

        # Handle erroneous pods
        for pod in failed_pods:
            pod_name = pod.metadata.name

            logger.warn(f"Pod {pod_name} has failed.")

            # Remove jobs that run on the pod from scheduler (running & uncompleted) so they are not rescheduled.
            for job in self.scheduler.uncompleted_jobs:
                job.__get_pods_names()

                if pod_name in job.worker_pods:
                    self.scheduler.remove_job(job, reschedule=False)

            k8s_api.kill_pod(pod_name)
