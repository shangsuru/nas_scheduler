import sys
import os
import threading

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


def main():
    k8s_api.clear_jobs()

    cluster = Cluster()

    # start the modules/workers
    timer = Timer()
    scheduler = OptimusScheduler(cluster, timer)
    progressor = Progressor(timer)
    statsor = Statsor(timer, scheduler, progressor, cluster)


    #check for kill signal


    while True:
        try:
            pass
        except KeyboardInterrupt:
            print('bye bye')
 


if __name__ == '__main__':
    if len(sys.argv) != 1:
        sys.exit(1)
    main()