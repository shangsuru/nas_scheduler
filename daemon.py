import pickle
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


async def listen(scheduler: SchedulerBase):
    """
    Main loop of the daemon waiting for client input
    """

    # setup redis connection
    r = await aioredis.create_redis_pool('redis://localhost')
    p, = await r.psubscribe('client')

    # listen for messages
    async for sender, message in p.iter():
        payload = pickle.loads(message)

        # execute commands from client
        if sender == b'client':
            if payload.command == 'init':
                await scheduler.init_schedule()
            elif payload.command == 'submit':
                for job in payload.args:
                    scheduler.submit_job(job)
            elif payload.command == 'delete':
                pass # TODO
            elif payload.command == 'list':
                pass # TODO
            elif payload.command == 'reset':
                Timer.reset_clock()
                await send(r, 'reset-ack', [1])
            elif payload.command == 'time':
                pass # TODO


async def send(r, response, args: list = None):
    """
    Sends given response with given args from daemon back to client
    """
    await r.publish('daemon', pickle.dumps(Payload(response, args)))

if __name__ == '__main__':
    asyncio.run(main())