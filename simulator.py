import os
import sys
import time
import random
import yaml
from pathlib import Path
import threading

import config
from log import logger
from dl_job import DLJob
from payload import Payload
import redis
import json


def prepare_job_repo():
    job_repo = list()
    for filename in Path('job_repo').glob('*.yaml'):
        job_repo.append(str(filename))

    return job_repo


class Simulator():
    def __init__(self):
        self.redis_connection = redis.Redis()
        self.channel = self.redis_connection.pubsub()
        self.channel.psubscribe(['daemon', 'timer'])
        self.job_dict = dict()
        self.counter = 0
        self.generate_jobs()
        self.initiated = threading.Event()

        self.send("reset")
        logger.debug('Simulator sent reset signal')
        # wait for ack
        for msg in self.channel.listen():
            if msg['pattern'] is None:  # TODO
                continue
            if msg['channel'] == b'timer':
                self.submit_job(int(msg['data']))
            else: 
                payload = json.loads(msg["data"])
                self.submit_job(int(payload["args"][0])) 


    def generate_jobs(self):
        tic = time.time()
        jobrepo = prepare_job_repo()

        random.seed(config.RANDOM_SEED)  # make each run repeatable

        for i in range(config.TOT_NUM_JOBS):
            # uniform randomly choose one
            index = random.randint(0, len(jobrepo) - 1)
            job_config_file = jobrepo[index]

            # randomize job arrival time
            timeslot = random.randint(1, config.T)  # clock start from 1
            if timeslot in self.job_dict:
                self.job_dict[timeslot].append(job_config_file)
            else:
                self.job_dict[timeslot] = [job_config_file]

        toc = time.time()
        logger.debug(f'has generated {config.TOT_NUM_JOBS} jobs')
        logger.debug(f'time to generate jobs: {toc - tic:.3f} seconds.')

    def submit_job(self, t):
        # put jobs into queue
        logger.info(f'-------*********-------- starting timeslot {t} --------*********-------')
        if t in self.job_dict:
            self.counter += len(self.job_dict[t])
            self.send("submit", args=self.job_dict[t])

        # notify the scheduler that all jobs in this timeslot have been submitted
        self.send("init")

        if self.counter == config.TOT_NUM_JOBS:
            logger.debug(f'initiated stop')
            sys.exit(0)


    def send(self, command, args=None):
        self.redis_connection.publish(
            "client", json.dumps({'command': command, 'args': args})
        )

    def process(self, msg):
        if msg.type == 'reset' or msg.type == 'update':
            self.submit_job(msg.timestamp)


def main():
    Simulator()


if __name__ == '__main__':
    main()