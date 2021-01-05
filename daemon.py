import jsonpickle
from schedulers.scheduler_base import SchedulerBase
import config
from log import logger
from cluster import Cluster
from schedulers.optimus import OptimusScheduler
from schedulers.fifo import FIFOScheduler
from schedulers.drf import DRFScheduler
from k8s.api import KubeAPI
from timer import Timer
from payload import Payload
from statsor import Statsor
import asyncio
import aioredis


k8s_api = KubeAPI()


async def main():
    k8s_api.clear_jobs()

    cluster = Cluster()

    # start the modules/workers
    """
    if config.JOB_SCHEDULER == 'optimus':
        scheduler = OptimusScheduler(cluster)
    elif config.JOB_SCHEDULER == 'fifo':
        scheduler = FIFOScheduler(cluster)
    elif config.JOB_SCHEDULER == 'drf':
        scheduler = DRFScheduler(cluster)
    else:
        logger.error(f'Scheduler {config.JOB_SCHEDULER} not found.')
    """
    scheduler = FIFOScheduler(cluster)
    Statsor.scheduler = scheduler
    Statsor.cluster = cluster

    await listen(scheduler)


async def setup_redis_connection():
    '''Creates a a connection to the redis database and subscribes to
    the client channel.
        
    Returns:
        redis_connection (aioredis.ConnectionsPool): A coroutine instance that
            can be used to communicate with redis asynchronously
        channel (aioredis.Channel): the channel is the object from which all
            messages sent by the client are received
    '''
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
                for job in args:
                    scheduler.submit_job(job)
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
    """
    asyncio.create_task(redis_connection.publish("daemon", jsonpickle.encode(Payload(response, args))))


def _get_command_args(message):
    payload = jsonpickle.decode(message)
    return payload.command, payload.args


if __name__ == '__main__':
    asyncio.run(main())