"""NASS Scheduler Frontend.
Usage:
    client.py submit <file>
    client.py init
    client.py top
    client.py delete <uid>
    client.py status <uid>
    client.py simulate
    client.py test

Options:
  -h --help                   Show this screen.
"""

from docopt import docopt
import json
import redis
from tabulate import tabulate
import random
import config
from log import logger
from pathlib import Path
import threading
import sys
import time


class Client:
    def __init__(self):
        """
        Initializes a client object. Sets up connection to redis server and subscribes to the daemon channel.
        """
        self.redis_connection = redis.Redis()
        self.channel = self.redis_connection.pubsub()
        self.channel.psubscribe("daemon")

    def submit(self, jobfile):
        """
        Sends a submit message to the daemon

        Args:
            jobfile (str): yaml file containing the job data
        """
        self.redis_connection.publish(
            "client", json.dumps({"command": "submit", "args": [jobfile]})
        )
        for msg in self.channel.listen():
            if msg["type"] != "psubscribe":
                args = _get_args(msg["data"])
                print(args)
                break

    def init(self):
        """
        Sends an init message to the daemon
        """
        self.redis_connection.publish(
            "client", json.dumps({"command": "init", "args": None})
        )

    def delete(self, uid):
        """
        Sends a delete message to the daemon

        Args:
            uid (int): uid of the job to be deleted
        """
        self.redis_connection.publish(
            "client", json.dumps({"command": "delete", "args": uid})
        )
        for msg in self.channel.listen():
            if msg["type"] != "psubscribe":
                args = _get_args(msg["data"])
                print(args)
                break

    def top(self):
        """
        Sends a top message to the daemon and prints a single view for all the jobs running on the cluster
        """
        self.redis_connection.publish(
            "client", json.dumps({"command": "top", "args": None})
        )
        for msg in self.channel.listen():
            if msg["type"] != "psubscribe":
                data = _get_args(msg["data"])
                break
        headers = ["id", "name", "total progress/total epochs", "sum_speed(batches/second)"]
        print(tabulate(data, headers=headers))

    def status(self, uid):
        """
        Sends a status message to the daemon and prints in-depth metrics of the job with the given id

        Args:
            uid (int): uid of the job
        """
        self.redis_connection.publish(
            "client", json.dumps({"command": "status", "args": uid})
        )
        for msg in self.channel.listen():
            if msg["type"] != "psubscribe":
                data = _get_args(msg["data"])
                break
        if data[0][2] == "ps":
            metrics = [
                "job id",
                "job name",
                "job strategy",
                "batch_size",
                "num_ps",
                "num_worker",
                "total progress/total epochs",
                "sum_speed(batches/second)",
                "ps cpu usage diff",
                "worker cpu usage diff",
            ]
            print(tabulate(data, headers=metrics))
        elif data[0][2] == "allreduce":
            metrics = [
                "job id",
                "job name",
                "job strategy" "batch_size",
                "num_worker",
                "total progress/total epochs",
                "sum_speed(batches/second)",
                "ps cpu usage diff",
                "worker cpu usage diff",
            ]
            print(tabulate(data, headers=metrics))

def _get_args(message):
    """Parses received message from daemon into arguments part

    Args:
        message (str): Message received from daemon

    Returns:
        payload['args'] (str): Arguments part of the daemon message
    """
    payload = json.loads(message)
    return payload["args"]

class Simulator:
    def __init__(self):
        self.redis_connection = redis.Redis()
        self.channel = self.redis_connection.pubsub()
        self.channel.psubscribe(["daemon", "timer"])
        self.job_dict = dict()
        self.counter = 0
        self.generate_jobs()
        self.initiated = threading.Event()

        self.send("reset")
        logger.debug("Simulator sent reset signal")
        # wait for ack
        for msg in self.channel.listen():
            if msg["pattern"] is None:  # TODO
                continue
            if msg["channel"] == b"timer":
                self.submit_job(int(msg["data"]))
            else:
                payload = json.loads(msg["data"])
                if payload["response"] == "submit":
                    continue
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
        logger.debug(f"has generated {config.TOT_NUM_JOBS} jobs")
        logger.debug(f"time to generate jobs: {toc - tic:.3f} seconds.")

    def submit_job(self, t):
        # put jobs into queue
        logger.info(
            f"-------*********-------- starting timeslot {t} --------*********-------"
        )
        if t in self.job_dict:
            self.counter += len(self.job_dict[t])
            self.send("submit", args=self.job_dict[t])

        # notify the scheduler that all jobs in this timeslot have been submitted
        self.send("init")

        if self.counter == config.TOT_NUM_JOBS:
            logger.debug(f"initiated stop")
            sys.exit(0)

    def send(self, command, args=None):
        self.redis_connection.publish(
            "client", json.dumps({"command": command, "args": args})
        )

    def process(self, msg):
        if msg.type == "reset" or msg.type == "update":
            self.submit_job(msg.timestamp)


def prepare_job_repo():
    job_repo = list()
    for filename in Path("job_repo").glob("*.yaml"):
        job_repo.append(str(filename))

    return job_repo


def main():
    args = docopt(__doc__, version="Client 1.0")
    client = Client()
    if args["submit"]:
        jobfile = args["<file>"]
        client.submit(jobfile)
    elif args["init"]:
        client.init()
    elif args["top"]:
        client.top()
    elif args["delete"]:
        job_id = args["<uid>"]
        client.delete(job_id)
    elif args["status"]:
        job_id = args["<uid>"]
        client.status(job_id)
    elif args["test"]:
        Simulator()
    elif args["simulate"]:
        pass


if __name__ == "__main__":
    main()