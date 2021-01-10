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
from tests.end_to_end import EndToEndTest


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
        EndToEndTest()
    elif args["simulate"]:
        pass


if __name__ == "__main__":
    main()