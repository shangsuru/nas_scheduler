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
            if command == 'init':
                asyncio.create_task(scheduler.init_schedule())
            elif command == 'submit':
                for i, job_config_file in enumerate(args):
                    job = DLJob.create_from_config_file(i, 0, os.getcwd(), job_config_file)
            elif command == 'delete':
                pass # TODO
            elif command == 'list':
                pass # TODO
            elif command == 'reset':
                Timer.reset_clock()
                await send(redis_connection, 'reset-ack', [1])
            elif command == 'time':
                pass # TODO


async def send(redis_connection, response, args: list = None):
    """
    Sends given response with given args from daemon back to client

    Args:
    redis_connection: Redis connection instance which the redis server
        can be interacted with
    response: Type of the response to client command
    args: Arguments associated with response
    """
    asyncio.create_task(redis_connection.publish("daemon", json.dumps({'response': response, 'args': args})))


def _get_command_args(message: str): # -> tuple[str, str]
    """
    Parses received message from client into command and arguments part
    Args:
        messsage: Message received from client
    Returns:
        payload['command']: Command part of the client message
        payload['args']: Arguments part of the client message
    """
    payload = json.loads(message)
    return payload['command'], payload['args']


if __name__ == '__main__':
    asyncio.run(main())