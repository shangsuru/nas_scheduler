"""NASS Scheduler Frontend.

Usage:
    client.py submit <file>
    client.py top
    client.py delete <uid>
    client.py status <uid>
    client.py simulate
    client.py test

Description:
    submit:     submit a job (as a yaml file) to the scheduler
    top:        a single view for all the jobs running on the cluster (limited metrics)
    delete:     delete a job by its uid
    status:     list the status of the job with a given id (in-depth metrics)
    simulate:   runs simulation scenario
    test:       runs end-to-end testing

Options:
  -h --help                   Show this screen.

"""

import json
import redis
from docopt import docopt
from tabulate import tabulate
from tests.end_to_end import EndToEndTest


class Client:
    def __init__(self):
        """Initializes a client object. Sets up connection to redis server and subscribes to the daemon channel."""
        self.redis_connection = redis.Redis()
        self.channel = self.redis_connection.pubsub()
        self.channel.psubscribe("daemon")

    def listen(self):
        """Listens for a response from the daemon and returns the data"""
        for msg in self.channel.listen():
            if msg["type"] != "psubscribe":
                payload = json.loads(msg["data"])
                data = payload["args"]
                break
        return data

    def submit(self, jobfile):
        """Sends a submit message to the daemon

        Args:
            jobfile (str): yaml file containing the job data
        """
        self.redis_connection.publish("client", json.dumps({"command": "submit", "args": [jobfile]}))
        print(self.listen())

    def delete(self, uid):
        """Sends a delete message to the daemon

        Args:
            uid (int): uid of the job to be deleted
        """
        self.redis_connection.publish("client", json.dumps({"command": "delete", "args": uid}))
        print(self.listen())

    def top(self):
        """Sends a top message to the daemon and prints a single view for all the jobs running on the cluster"""
        self.redis_connection.publish("client", json.dumps({"command": "top", "args": None}))
        headers = ["id", "name", "total progress/total epochs", "sum_speed(batches/second)"]
        print(tabulate(self.listen(), headers=headers))

    def status(self, uid):
        """Sends a status message to the daemon and prints in-depth metrics of the job with the given id

        Args:
            uid (int): uid of the job
        """
        self.redis_connection.publish("client", json.dumps({"command": "status", "args": uid}))
        data = self.listen()
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


def main():
    args = docopt(__doc__, version="Client 1.0")
    client = Client()
    if args["submit"]:
        jobfile = args["<file>"]
        client.submit(jobfile)
    elif args["top"]:
        client.top()
    elif args["delete"]:
        job_id = args["<uid>"]
        client.delete(job_id)
    elif args["status"]:
        job_id = args["<uid>"]
        client.status(job_id)
    elif args["test"]:
        EndToEndTest()
    elif args["simulate"]:
        pass


if __name__ == "__main__":
    main()
