import aioredis
import asyncio
import config
import pytest
import random
import re
import subprocess
import time

from client import Client
from daemon import Daemon
from k8s.api import KubeAPI


k8s_api = KubeAPI()

RESILIENCE_TEST_FAILING_POD_NUM = 1


@pytest.mark.asyncio
async def test_parameter_server_job():
    """
    Tests, if a server that runs with parameter servers is submitted succesfully, i.e.
    pods in the k8s cluster are created, metrics are saved to redis and the job got
    marked as completed in scheduler
    """
    daemon = Daemon()
    task = asyncio.create_task(daemon.listen())
    client = Client()
    await client.init_redis()
    job_name = "job_repo/experiment-cifar10-resnext110.yaml"
    job_id = await asyncio.create_task(client.submit(job_name))

    job = daemon.scheduler.running_jobs[0]

    num_worker = job.resources.worker.num_worker
    num_ps = job.resources.ps.num_ps
    await asyncio.sleep(0.5)

    # Checking if pods get created...
    num_ps_pods = 0
    num_worker_pods = 0
    pods = k8s_api.get_pods()
    for pod in pods:
        if re.match(f"{job_id}-experiment-cifar10-resnet110-ps.*", pod.metadata.name):
            num_ps_pods += 1
        if re.match(f"{job_id}-experiment-cifar10-resnet110-worker.*", pod.metadata.name):
            num_worker_pods += 1
            await asyncio.sleep(30)
            # check if the directory for the training data got mounted on the pod
            output = subprocess.Popen(
                [f"microk8s kubectl exec {pod.metadata.name} -- ls ../data/job"],
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            filenames = output.stdout.read()
            assert "cifar10_train.rec" in str(filenames)

    assert num_ps == num_ps_pods
    assert num_worker == num_worker_pods

    # check if redis keys got set(only need to test one as monitor did not crash if at least one key was set,
    # the correctness of monitor can be tested with unit test)
    redis_connection = await aioredis.create_redis_pool(
        (config.REDIS_HOST_DAEMON_CLIENT, config.REDIS_PORT_DAEMON_CLIENT)
    )
    channel = (await redis_connection.psubscribe("timer"))[0]
    await asyncio.sleep(45)
    for i in range(num_worker):
        key_value = await redis_connection.execute("get", f"{job.name}-{i}-avg_speed")
        assert key_value != -1

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


@pytest.mark.asyncio
async def test_horovod_job():
    """
    Tests, if a server that runs with horovod is submitted succesfully, i.e.
    pods in the k8s cluster are created, metrics are saved to redis and
    the job got marked as completed in scheduler.
    """
    daemon = Daemon()
    task = asyncio.create_task(daemon.listen())
    client = Client()
    await client.init_redis()
    job_id = await asyncio.create_task(client.submit("job_repo/experiment_mnist_horovod.yaml"))

    job = daemon.scheduler.running_jobs[0]

    num_worker = job.resources.worker.num_worker
    await asyncio.sleep(0.5)

    # check if pods get created
    num_worker_pods = 0
    pods = k8s_api.get_pods()
    for pod in pods:
        if re.match(f"{job_id}-experiment-mnist-mxnet-mnist-worker.*", pod.metadata.name):
            num_worker_pods += 1

    assert num_worker == num_worker_pods

    # check if redis keys got set(only need to test one as monitor did not crash if at least one key was set,
    # the correctness of monitor can be tested with unit test)
    redis_connection = await aioredis.create_redis_pool(
        (config.REDIS_HOST_DAEMON_CLIENT, config.REDIS_PORT_DAEMON_CLIENT)
    )
    channel = (await redis_connection.psubscribe("timer"))[0]
    await asyncio.sleep(45)
    for i in range(num_worker):
        key_value = await redis_connection.execute("get", f"{job.name}-{i}-avg_speed")
        assert key_value != -1

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

    for fail in failing_pods:
        print(fail.metadata.name)
        k8s_api.kill_pod(fail.metadata.name)

    # see if job is in finished job list of progressor
    time_to_wait = 300
    while time_to_wait > 0:
        t1 = time.time()
        await asyncio.wait_for(channel.wait_message(), time_to_wait)
        receiver, _ = await channel.get()
        if receiver == b"timer":
            break
        t2 = time.time()
        waited = t2 - t1
        time_to_wait -= waited

    assert job in daemon.scheduler.completed_jobs

    task.cancel()
