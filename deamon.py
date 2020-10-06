import sys
import signal
import time

import config
from k8s.api import KubeAPI
from communication import hub, Payload
from timer import Timer
from log import logger
from cluster import Cluster
from schedulers.optimus import OptimusScheduler
from progressor import Progressor
from statsor import Statsor

k8s_api = KubeAPI()


def exit_gracefully(signum, frame):
    hub.broadcast('stop')


signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)


def main():
    k8s_api.clear_jobs()

    cluster = Cluster()

    # start the modules/workers
    timer = Timer()
    scheduler = OptimusScheduler(cluster, timer)
    progressor = Progressor(timer)
    statsor = Statsor(timer, scheduler, progressor, cluster)

    # check for kill signal
    signal.pause()
    time.sleep(2)


if __name__ == '__main__':
    if len(sys.argv) != 1:
        sys.exit(1)
    main()
