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
import aioredis
import asyncio
import config
import json

from docopt import docopt
from tabulate import tabulate
from tests.end_to_end import EndToEndTest
from typing import Any


class Client:
    async def listen(self) -> Any:
        """Listens for a response from the daemon and returns the data"""
        async for receiver, msg in self.channel.iter():
            if receiver == b"client":
                payload = json.loads(msg)
                data = payload["args"]
                break
        return data

    async def init_redis(self):
        self.redis_connection = await aioredis.create_redis_pool(
            (config.REDIS_HOST_DAEMON_CLIENT, config.REDIS_PORT_DAEMON_CLIENT)
        )
        self.channel = (await self.redis_connection.psubscribe("client"))[0]

    async def submit(self, jobfile: str) -> int:
        """Sends a submit message to the daemon

        Args:
            jobfile: yaml file containing the job data
        Returns:
            job id: needed for running integration tests
        """
        await self.redis_connection.publish("daemon", json.dumps({"command": "submit", "args": [jobfile]}))
        jobid = await self.listen()
        print(f"submitted job with uid {jobid}")
        return jobid

    async def delete(self, uid: int) -> None:
        """Sends a delete message to the daemon

        Args:
            uid: uid of the job to be deleted
        """
        await self.redis_connection.publish("daemon", json.dumps({"command": "delete", "args": uid}))
        print(await self.listen())

    async def top(self) -> None:
        """Sends a top message to the daemon and prints a single view for all the jobs running on the cluster"""
        await self.redis_connection.publish("daemon", json.dumps({"command": "top", "args": None}))
        headers = ["id", "name", "total progress/total epochs", "sum_speed(batches/second)"]
        print(tabulate(await self.listen(), headers=headers))

    async def status(self, uid: int) -> None:
        """Sends a status message to the daemon and prints in-depth metrics of the job with the given id

        Args:
            uid: uid of the job
        """
        await self.redis_connection.publish("daemon", json.dumps({"command": "status", "args": uid}))
        data = await self.listen()
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


async def main() -> None:
    args = docopt(__doc__, version="Client 1.0")
    client = Client()
    await client.init_redis()

    if args["submit"]:
        jobfile = args["<file>"]
        await client.submit(jobfile)
    elif args["top"]:
        await client.top()
    elif args["delete"]:
        job_id = args["<uid>"]
        await client.delete(job_id)
    elif args["status"]:
        job_id = args["<uid>"]
        await client.status(job_id)
    elif args["test"]:
        EndToEndTest()
    elif args["simulate"]:
        pass


if __name__ == "__main__":
    asyncio.run(main())
