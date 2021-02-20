import aioredis
from aioredis import ConnectionsPool
import asyncio
import config
import json
import os
from cluster import Cluster
from dl_job import DLJob
from k8s.api import KubeAPI
from log import logger
from progressor import Progressor
from typing import Any, List, Tuple, Optional, Union
from schedulers.drf import DRFScheduler
from schedulers.fifo import FIFOScheduler
from schedulers.optimus import OptimusScheduler
from schedulers.scheduler_base import SchedulerBase
from statsor import Statsor
from heartbeat import Heartbeat
from timer import Timer

k8s_api = KubeAPI()


class Daemon:
    def __init__(self) -> None:
        k8s_api.clear_jobs()
        cluster = Cluster()

        # start the modules/workers
        if config.JOB_SCHEDULER == "optimus":
            self.scheduler = OptimusScheduler(cluster)
        elif config.JOB_SCHEDULER == "fifo":
            self.scheduler = FIFOScheduler(cluster)
        elif config.JOB_SCHEDULER == "drf":
            self.scheduler = DRFScheduler(cluster)
        else:
            logger.error(f"Scheduler {config.JOB_SCHEDULER} not found.")
        Statsor.set_cluster_and_scheduler(cluster, self.scheduler)

        self.heartbeat = Heartbeat(self.scheduler, cluster)

    async def listen(self) -> None:
        """Main loop of the daemon waiting for client input
        Args:
            scheduler (SchedulerBase): Scheduler instance managing and scheduling
                jobs submitted by client
            heartbeat (Heartbeat): Heartbeat instance to monitor unavailable nodes in the cluster.
        """
        self.redis_connection = await aioredis.create_redis_pool(
            (config.REDIS_HOST_DAEMON_CLIENT, config.REDIS_PORT_DAEMON_CLIENT)
        )
        channel = (await self.redis_connection.psubscribe("daemon"))[0]

        # listen for messages
        while True:
            await self.heartbeat.on_iteration()

            try:
                receiver, message = await asyncio.wait_for(channel.get(), 0.001)
            except asyncio.TimeoutError:
                continue

            # get command and arguments from redis message
            payload = json.loads(message)
            command = payload["command"]
            args = payload["args"]

            # execute commands from client
            if receiver == b"daemon":
                if command == "submit":
                    for jobfile in args:
                        job = DLJob.create_from_config_file(os.getcwd(), jobfile)
                        self.scheduler.submit_job(job)
                        asyncio.create_task(
                            self.send(
                                "submit",
                                f"{job.uid}",
                            )
                        )
                    asyncio.create_task(self.scheduler.init_schedule())
                elif command == "delete":
                    deleted = False
                    for job in self.scheduler.running_jobs:
                        if job.uid == int(args):
                            Progressor.remove_from_running_jobs(job)
                            self.scheduler.running_jobs.remove(job)
                            await job.delete(True)
                            deleted = True
                            break
                    if deleted:
                        await self.send("delete", "job was deleted successfully")
                    else:
                        await self.send(
                            "delete",
                            "job with given uid is currently not running",
                        )
                elif command == "status":
                    for job in self.scheduler.running_jobs:
                        if job.uid == int(args):
                            if job.metadata["dist_strategy"] == "ps":
                                data = [
                                    [
                                        job.uid,
                                        job.name,
                                        job.metadata["dist_strategy"],
                                        job.metadata.batch_size,
                                        job.resources.ps.num_ps,
                                        job.resources.worker.num_worker,
                                        f"{job.progress}/{job.total_num_epochs}",
                                        job.training_speeds[
                                            (
                                                job.resources.ps.num_ps,
                                                job.resources.worker.num_worker,
                                            )
                                        ]
                                        if (
                                            job.resources.ps.num_ps,
                                            job.resources.worker.num_worker,
                                        )
                                        in job.training_speeds
                                        else 0,
                                        job.ps_cpu_diff,
                                        job.worker_cpu_diff,
                                    ]
                                ]
                            elif job.metadata["dist_strategy"] == "allreduce":
                                data = [
                                    [
                                        job.uid,
                                        job.name,
                                        job.metadata["dist_strategy"],
                                        job.metadata.batch_size,
                                        job.resources.worker.num_worker,
                                        f"{job.progress}/{job.total_num_epochs}",
                                        0,  # add the speed when we know how the speed is calculated when using Horovod
                                        job.ps_cpu_diff,
                                        job.worker_cpu_diff,
                                    ]
                                ]
                            await self.send("status", data)
                            break

                elif command == "top":
                    data = [
                        [
                            job.uid,
                            job.name,
                            f"{job.progress}/{job.total_num_epochs}",
                            job.training_speeds[
                                (
                                    job.resources.ps.num_ps,
                                    job.resources.worker.num_worker,
                                )
                            ]
                            if (
                                job.resources.ps.num_ps,
                                job.resources.worker.num_worker,
                            )
                            in job.training_speeds
                            else 0,
                        ]
                        for job in self.scheduler.running_jobs
                    ]
                    await self.send("top", data)
                elif command == "reset":
                    Timer.reset_clock()
                    await self.send("reset-ack", [1])
                elif command == "pod_finished":
                    for job in self.scheduler.running_jobs:
                        if job.uid == int(args):
                            job.finished_pods += 1
                elif command == "time":
                    pass  # TODO

    async def send(self, response: str, args: Any = None) -> None:
        """
        Sends given response with given args from daemon back to client

        Args:
        response: Type of the response to client command
        args: Arguments associated with response, if None no arguments
                are given to the command
        """
        await self.redis_connection.publish("client", json.dumps({"response": response, "args": args}))


async def main():
    daemon = Daemon()
    await daemon.listen()


if __name__ == "__main__":
    asyncio.run(main())
