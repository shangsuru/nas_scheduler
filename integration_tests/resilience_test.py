import aioredis
import asyncio
import config
import pytest
import random
import re
import time

from client import Client
from daemon import Daemon
from functools import partial
from progressor import Progressor
from k8s.api import KubeAPI


k8s_api = KubeAPI()
RESILIENCE_TEST_FAILING_POD_NUM = 1


@pytest.mark.asyncio
async def test_resilience():
    """
    Run a horovod job and kill the pod and test if the job is still running through successfully
    """
    daemon = Daemon()
    task = asyncio.create_task(daemon.listen())
    client = Client()
    await client.init_redis()
    job_id = await asyncio.create_task(client.submit("job_repo/experiment_mnist_horovod.yaml"))

    redis_connection = await aioredis.create_redis_pool(
        (config.REDIS_HOST_DAEMON_CLIENT, config.REDIS_PORT_DAEMON_CLIENT)
    )
    channel = (await redis_connection.psubscribe("timer"))[0]

    job = daemon.scheduler.running_jobs[0]

    # check if pods get created
    pods = k8s_api.get_pods()

    num_worker = job.resources.worker.num_worker
    num_ps = job.resources.ps.num_ps
    assert num_worker + num_ps >= RESILIENCE_TEST_FAILING_POD_NUM

    job_pods = []

    for pod in pods:
        if re.match(f"{job_id}-experiment-mnist-mxnet-mnist-worker.*", pod.metadata.name):
            job_pods.append(pod)

    failing_pods = []
    while len(failing_pods) < RESILIENCE_TEST_FAILING_POD_NUM:
        choice = random.choice(job_pods)
        if choice not in failing_pods:
            failing_pods.append(choice)

    await asyncio.sleep(30)

    for fail in failing_pods:
        print(fail.metadata.name)
        k8s_api.kill_pod(fail.metadata.name)

    # see if job is in finished job list of progressor
    time_to_wait = 300
    while True:
        tic = time.time()
        await asyncio.wait_for(channel.wait_message(), time_to_wait)
        receiver, msg = await channel.get()
        if receiver == b"timer":
            break
        toc = time.time()
        time_to_wait -= toc - tic

    assert job in daemon.scheduler.completed_jobs

    task.cancel()
