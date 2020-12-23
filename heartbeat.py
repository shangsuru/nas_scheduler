import time

from communication import Handler, hub
from k8s.api import KubeAPI
from log import logger

k8s_api = KubeAPI()


class Heartbeat(Handler):
    """A heartbeat handler that regularly checks the availability of k8s as well as the pods.

    Attributes:
        cluster: The cluster of pods managing resources.
        heartbeat_interval: The interval (in sec.) in which to send out heartbeats.
        last_beat: The time when the last heartbeat was emitted.
        scheduler: The NAS scheduler managing jobs.
    """

    heartbeat_interval = 10

    def __init__(self, scheduler, cluster, daemon=True):
        super().__init__(connection=hub.connection, entity="heartbeat")

        self.cluster = cluster
        self.scheduler = scheduler
        self.last_beat = 0

    def on_iteration(self):
        """Emits a heartbeat if the last heartbeat was at least heartbeat_interval seconds ago."""
        if time.perf_counter() - self.last_beat < self.heartbeat_interval:
            return

        self.heartbeat()
        self.last_beat = time.perf_counter()

    def process(self, message):
        """Apart from the stop message (processed by handler), the Heartbeat class does not process any messages."""
        pass

    def heartbeat(self):
        """Performs a single heartbeat and updates the cluster and scheduler accordingly.

        TODO: implement cluster heartbeat.
        """
        logger.debug(f"Heartbeat")
