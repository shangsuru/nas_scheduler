import config
from log import logger
from cluster import Cluster
from schedulers.optimus import OptimusScheduler
from schedulers.fifo import FIFOScheduler
from schedulers.drf import DRFScheduler
from schedulers.scheduler_base import SchedulerBase
from k8s.api import KubeAPI
from timer import Timer
from statsor import Statsor
import asyncio
import aioredis
import json
import os
from dl_job import DLJob


k8s_api = KubeAPI()


async def main():
    k8s_api.clear_jobs()

    cluster = Cluster()

    # start the modules/workers
    if config.JOB_SCHEDULER == 'optimus':
        scheduler = OptimusScheduler(cluster)
    elif config.JOB_SCHEDULER == 'fifo':
        scheduler = FIFOScheduler(cluster)
    elif config.JOB_SCHEDULER == 'drf':
        scheduler = DRFScheduler(cluster)
    else:
        logger.error(f'Scheduler {config.JOB_SCHEDULER} not found.')
    Statsor.scheduler = scheduler
    Statsor.cluster = cluster

    await listen(scheduler)


async def setup_redis_connection():
    """Creates a a connection to the redis database and subscribes to
    the client channel.
        
    Returns:
        redis_connection (aioredis.ConnectionsPool): A coroutine instance that
            can be used to communicate with redis asynchronously
        channel (aioredis.Channel): the channel is the object from which all
            messages sent by the client are received
    """
    redis_connection = await aioredis.create_redis_pool("redis://localhost")
    channel = (await redis_connection.psubscribe("client"))[0]
    return redis_connection, channel


async def listen(scheduler: SchedulerBase):
    """Main loop of the daemon waiting for client input
    Args:
        scheduler (SchedulerBase): Scheduler instance managing and scheduling
            jobs submitted by client
    """

    redis_connection, channel = await setup_redis_connection()

    # listen for messages
    async for sender, message in channel.iter():
        command, args = _get_command_args(message)

        # execute commands from client
        if sender == b'client':
            if command == "init":
                asyncio.create_task(scheduler.init_schedule())
            elif command == "submit":
                for jobfile in args:
                    job = DLJob.create_from_config_file(os.getcwd(), jobfile)
                    scheduler.submit_job(job)
                    asyncio.create_task(
                        send(redis_connection, 'submit', f'job successfully submitted with uid: {job.uid}'))
            elif command == 'delete':
                deleted = False
                for job in scheduler.running_jobs:
                    if job.uid == int(args):
                        job.delete(True)
                        #scheduler.allocator.free_job_resources(job)
                        #scheduler.running_jobs.remove(job)
                        deleted = True
                        break
                if deleted:
                    await send(redis_connection, 'delete', 'job was deleted successfully')
                else:
                    await send(redis_connection, 'delete', 'job with given uid is not currently running')
            elif command == 'status':
                for job in scheduler.running_jobs:
                    if job.uid == int(args):
                        if job.metadata["dist_strategy"] == "ps":
                            data = [
                                [job.uid],
                                [job.name],
                                [job.metadata["dist_strategy"]],
                                [job.metadata.batch_size],
                                [job.resources.ps.num_ps],
                                [job.resources.worker.num_worker],
                                f"{job.progress}/{job.num_epochs}",
                                [
                                    job.training_speeds[
                                        (job.resources.ps.num_ps, job.resources.worker.num_worker)
                                    ]
                                ],
                                [job.ps_cpu_diff],
                                [job.worker_cpu_diff],
                            ]
                        elif job.metadata["dist_strategy"] == "allreduce":
                            data = [
                                [job.uid],
                                [job.name],
                                [job.metadata["dist_strategy"]],
                                [job.metadata.batch_size],
                                [job.resources.worker.num_worker],
                                f"{job.progress}/{job.num_epochs}",
                                0,  # add the speed when we know how the speed is calculated when using Horovod
                                [job.ps_cpu_diff],
                                [job.worker_cpu_diff],
                            ]
                        await send(redis_connection, 'status', data)
                        break

            elif command == 'top':
                data = [
                    [job.uid, job.name, f"{job.progress}/{job.num_epochs}"] for job in scheduler.running_jobs
                ]
                await send(redis_connection, 'top', data)
            elif command == 'reset':
                Timer.reset_clock()
                await send(redis_connection, 'reset-ack', [1])
            elif command == 'time':
                pass # TODO


async def send(redis_connection, response, args: list = None):
    """Sends given response with given args from daemon back to client

    Args:
    redis_connection (aioredis.ConnectionsPool): Redis connection instance which the redis server
        can be interacted with
    response (str): Type of the response to client command
    args (list): Arguments associated with response, if None no arguments
            are given to the command
    """
    await redis_connection.publish("daemon", json.dumps({'response': response, 'args': args}))


def _get_command_args(message: str): # -> tuple[str, str]
    """Parses received message from client into command and arguments part
    Args:
        messsage (str): Message received from client
    Returns:
        payload['command'] (str): Command part of the client message
        payload['args'] (str): Arguments part of the client message
    """
    payload = json.loads(message)
    return payload['command'], payload['args']


if __name__ == '__main__':
    asyncio.run(main())