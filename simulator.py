"""NASS Simulator.

Usage:
    simulator.py [--vhost=<vhost>]

Options:
    --vhost=<vhost>             RabbitMQ Vhost name [default: /]
    -h --help                   Show this screen
"""

import os
import time
import signal
import random
import yaml
from pathlib import Path
import threading
from docopt import docopt
from schema import Schema, Use, SchemaError

import config
from communication import Handler, Payload, hub
from log import logger
from dl_job import DLJob

docopt_schema = Schema({
    '--vhost': Use(str),
})


def exit_gracefully(signum, frame):
    frame.f_locals['self'].stop()


signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)


def prepare_job_repo():
    job_repo = list()
    for filename in Path('job_repo').glob('*.yaml'):
        with open(filename, 'r') as f:
            job_repo.append(yaml.full_load(f))

    return job_repo


class Simulator(Handler):
    def __init__(self):
        super().__init__(hub.connection, entity='simulator', daemon=False)
        self.job_dict = dict()
        self.counter = 0
        self.generate_jobs()
        self.initiated = threading.Event()
        hub.push(Payload(None, self.entity, 'reset'), 'timer')

    def stop(self):
        super().stop()
        logger.debug(f'submitted {self.counter} jobs')
        logger.debug(f'exited.')

    def generate_jobs(self):
        tic = time.time()
        jobrepo = prepare_job_repo()

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
        logger.debug(f'has generated {config.TOT_NUM_JOBS} jobs')
        logger.debug(f'time to generate jobs: {toc - tic:.3f} seconds.')

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
            logger.debug(f'initiated stop')
            self.stop()

    def process(self, msg):
        if msg.type == 'reset':
            self.initiated.set()

        if self.initiated.wait():
            self.submit_job(msg.timestamp)


def main():
    arguments = docopt(__doc__, version=f'v0.1')

    try:
        arguments = docopt_schema.validate(arguments)
    except SchemaError as e:
        exit(e)

    hub.connect(arguments['--vhost'])

    sim = Simulator()
    sim.start()
    sim.join()


if __name__ == '__main__':
    main()
