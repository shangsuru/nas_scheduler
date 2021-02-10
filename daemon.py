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


async def main() -> None:
    k8s_api.clear_jobs()
    cluster = Cluster()

    # start the modules/workers
    if config.JOB_SCHEDULER == "optimus":
        scheduler = OptimusScheduler(cluster)
    elif config.JOB_SCHEDULER == "fifo":
        scheduler = FIFOScheduler(cluster)
    elif config.JOB_SCHEDULER == "drf":
        scheduler = DRFScheduler(cluster)
    else:
        logger.error(f"Scheduler {config.JOB_SCHEDULER} not found.")
    Statsor.set_cluster_and_scheduler(cluster, scheduler)

    heartbeat = Heartbeat(scheduler, cluster)

    await listen(scheduler, heartbeat)


async def setup_redis_connection() -> Tuple[aioredis.ConnectionsPool, aioredis.Channel]:
    """
    Creates a a connection to the redis database and subscribes to
    the client channel.

    Returns:
        redis_connection (aioredis.ConnectionsPool): A coroutine instance that
            can be used to communicate with redis asynchronously
        channel (aioredis.Channel): the channel is the object from which all
            messages sent by the client are received
    """
    redis_connection = await aioredis.create_redis_pool(
        (config.REDIS_HOST_DAEMON_CLIENT, config.REDIS_PORT_DAEMON_CLIENT)
    )
    channel = (await redis_connection.psubscribe("daemon"))[0]
    return redis_connection, channel


async def listen(scheduler: SchedulerBase, heartbeat: Heartbeat) -> None:
    """Main loop of the daemon waiting for client input
    Args:
        scheduler (SchedulerBase): Scheduler instance managing and scheduling
            jobs submitted by client
        heartbeat (Heartbeat): Heartbeat instance to monitor unavailable nodes in the cluster.
    """
    redis_connection, channel = await setup_redis_connection()

    # listen for messages
    while True:
        await heartbeat.on_iteration()

        try:
            receiver, message = await asyncio.wait_for(channel.get(), 0.001)
        except asyncio.TimeoutError:
            continue

        command, args = _get_command_args(message)

        # execute commands from client
        if receiver == b"daemon":
            if command == "submit":
                for jobfile in args:
                    job = DLJob.create_from_config_file(os.getcwd(), jobfile)
                    scheduler.submit_job(job)
                    asyncio.create_task(
                        send(
                            redis_connection,
                            "submit",
                            f"job successfully submitted with uid: {job.uid}",
                        )
                    )
                asyncio.create_task(scheduler.init_schedule())
            elif command == "delete":
                deleted = False
                for job in scheduler.running_jobs:
                    if job.uid == int(args):
                        Progressor.remove_from_running_jobs(job)
                        scheduler.running_jobs.remove(job)
                        await job.delete(True)
                        deleted = True
                        break
                if deleted:
                    await send(redis_connection, "delete", "job was deleted successfully")
                else:
                    await send(
                        redis_connection,
                        "delete",
                        "job with given uid is currently not running",
                    )
            elif command == "status":
                for job in scheduler.running_jobs:
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
                        await send(redis_connection, "status", data)
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
                    for job in scheduler.running_jobs
                ]
                await send(redis_connection, "top", data)
            elif command == "reset":
                Timer.reset_clock()
                await send(redis_connection, "reset-ack", [1])
            elif command == "pod_finished":
                for job in scheduler.running_jobs:
                    if job.uid == int(args):
                        job.finished_pods += 1
            elif command == "time":
                pass  # TODO


async def send(redis_connection: ConnectionsPool, response: str, args: Any = None) -> None:
    """
    Sends given response with given args from daemon back to client

    Args:
    redis_connection: Redis connection instance which the redis server
        can be interacted with
    response: Type of the response to client command
    args: Arguments associated with response, if None no arguments
            are given to the command
    """
    await redis_connection.publish("client", json.dumps({"response": response, "args": args}))


def _get_command_args(message: str) -> Tuple[str, str]:
    """
    Parses received message from client into command and arguments part
    Args:
        messsage: Message received from client
    Returns:
        payload['command']: Command part of the client message
        payload['args']: Arguments part of the client message
    """
    payload = json.loads(message)
    return payload["command"], payload["args"]


if __name__ == "__main__":
    asyncio.run(main())
