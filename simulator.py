import os
import sys
import time
import signal
import random
import yaml
from pathlib import Path

import config
from communication import Handler, Payload, hub
from k8s.api import KubeAPI
from log import logger
from dl_job import DLJob

k8s_api = KubeAPI()

def exit_gracefully(signum, frame):
    hub.broadcast('stop')


signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)

class Simulator(Handler):
    def __init__(self):
        super().__init__(hub.connection, entity='simulator')
        self.job_dict = dict()
        self.counter = 0
        self.module_name = 'simulator'
        self.generate_jobs()
        hub.push(Payload(None, self.module_name, 'reset'), 'timer')
        self.start()

    def stop(self):
        super().stop()
        logger.debug(f'[simulator] submitted {self.counter} jobs')
        logger.debug(f'[simulator] exited.')

    def __prepare_job_repo(self):
        job_repo = list()
        for filename in Path('job_repo').glob('*.yaml'):
            with open(filename, 'r') as f:
                job_repo.append(yaml.full_load(f))

        return job_repo

    def generate_jobs(self):
        tic = time.time()
        jobrepo = self.__prepare_job_repo()

        random.seed(config.RANDOM_SEED)  # make each run repeatable

        for i in range(config.TOT_NUM_JOBS):
            # uniform randomly choose one
            index = random.randint(0, len(jobrepo) - 1)
            job_conf = jobrepo[index]
            job = DLJob(i, index, os.getcwd(), job_conf)

            # randomize job arrival time
            t = random.randint(1, config.T)  # clock start from 1
            job.arrival_slot = t
            if job.arrival_slot in self.job_dict:
                self.job_dict[job.arrival_slot].append(job)
            else:
                self.job_dict[job.arrival_slot] = [job]

        toc = time.time()
        logger.debug(f'[simulator] has generated {config.TOT_NUM_JOBS} jobs')
        logger.debug(f'[simulator] time to generate jobs: {toc - tic:.3f} seconds.')


    def submit_job(self, t):
        # put jobs into queue
        logger.info(f'-------*********-------- starting timeslot {t} --------*********-------')
        if t in self.job_dict:
            for job in self.job_dict[t]:
                job.arrival_time = time.time()
                msg = Payload(t, 'simulator', 'submission', {'job': job})
                hub.push(msg, 'scheduler')  # enqueue jobs at the beginning of each time slot
                self.counter += 1

        # notify the scheduler that all jobs in this timeslot have been submitted
        msg = Payload(t, 'simulator', 'submission', None)
        hub.push(msg, 'scheduler')

        if self.counter == config.TOT_NUM_JOBS:
            logger.debug(f'[simulator] initiated stop')
            hub.broadcast('stop')

    def process(self, msg):
        self.submit_job(msg.timestamp)


def main():
    sim = Simulator()

    # check for kill signal
    signal.pause()
    time.sleep(2)

if __name__ == '__main__':
    if len(sys.argv) != 1:
        print("Description: job scheduling simulator")
        print("Usage: python simulator.py")
        sys.exit(1)
    main()
