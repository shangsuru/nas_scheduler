import asyncio
import aiohttp
import time
from timer import Timer
from datetime import datetime
import os
import redis
import shutil
from munch import munchify
from uuid import uuid1
import concurrent

import utils
import yaml

from k8s.api import KubeAPI
from k8s.job import Job
from log import logger
from utils import fetch_with_timeout

k8s_api = KubeAPI()
redis_connection = redis.Redis()


class DLJob:
    """Job class defines the structure of a DL training job.

    Attributes:
        uid (int): job unique id -- incremental style
        tag (int): unique index for the job. useful for identifying
            the job characteristic in the future.
        name (str): job name as in '{uid}-{name}-{model_name}'. e.g. '1-measurement-imagenet-vgg16'
        timestamp (str): job creation time as in '%Y-%m-%d-%H:%M:%S'
        dir (str): job working directory as in '{dir_prefix}/{name}-{timestamp}/}'

    """

    ps_placement: None

    def __init__(self, uid, tag, dir_prefix, conf):
        """Initializes a job object.

        Args:
            uid (int): job unique id -- incremental style
            tag (int): unique index for the job. useful for identifying
                the job characteristic in the future.
            dir_prefix (str): job working directory
            conf (dict): job configuration dictionary
        """
        self.metadata = munchify(conf.get("metadata"))
        self.resources = munchify(conf.get("resources"))
        self.container = munchify(conf.get("container"))
        self.data = munchify(conf.get("data"))
        self.envs = munchify(conf.get("envs"))

        self.uid = uid
        self.tag = tag
        self.name = f"{uid}-{self.metadata.name}-{self.metadata.modelname}"

        self.timestamp = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
        self.dir = os.path.join(dir_prefix, f"{self.name}-{self.timestamp}")

        self.ps_placement = None
        self.worker_placement = None

        self.speed_list = []

        self.running_tasks = []

        # [(epoch, batch)]
        self.progress_list = None
        self.ps_metrics = []
        self.worker_metrics = []
        self.ps_pods = []
        self.worker_pods = []

        # for experiment
        self.arrival_slot = None
        self.arrival_time = None
        self.end_slot = None
        self.end_time = None
        self.status = "initialized"
        self.progress = 0

        # (num_ps, num_worker): speed
        self.training_speeds = dict()

        # epoch : validation_loss
        self.val_losses = dict()
        self.num_epochs = 0
        self.epoch_size = 0

        self.ps_cpu_diff = None
        self.worker_cpu_diff = None

        self.worker_mount_dirs = []

    def __lt__(self, other):
        if not hasattr(other, "uid"):
            return NotImplemented
        return self.uid < other.uid

    def __eq__(self, other):
        return self.uid == other.uid

    def __hash__(self):
        return hash(self.uid)

    def __repr__(self):
        return f"DLJob(name={self.name})"

    @staticmethod
    def create_from_config_file(working_directory, config_file):
        """Creates a DLJob by reading its configuration from a yaml file.
        Args:
            working_directory (str): working directory of the job
            config_file (str): yaml file containing the job configuration
        """
        with open(config_file, "r") as f:
            job_config = yaml.full_load(f)
        job = DLJob(uuid1().int % 99999, job_config["metadata"]["tag"], working_directory, job_config)
        job.arrival_slot = Timer.get_clock()
        job.arrival_time = time.time()
        return job

    def set_ps_placement(self, ps_placement):
        """Setting the placement of parameter servers.

        Args:
            ps_placement (list): list of parameter servers ip addresses
        """
        if isinstance(ps_placement, list):
            if len(ps_placement) == self.resources.ps.num_ps:
                self.ps_placement = ps_placement
            else:
                raise RuntimeError("ps_placement is not consistent with num_ps")
        else:
            raise TypeError("ps_placement is not a list")

    def set_worker_placement(self, worker_placement):
        """Setting the placement of workers.

        Args:
            worker_placement (list): list of workers ip addresses
        """
        if isinstance(worker_placement, list):
            if len(worker_placement) == self.resources.worker.num_worker:
                self.worker_placement = worker_placement
            else:
                raise RuntimeError("worker_placement is not consistent with num_worker")
        else:
            raise TypeError("worker_placement is not a list")

    def __set_mount_dirs(self, type, host_workdir_prefix):
        """Setting the directories on hosts to be mounted on containers

        Args:
            type (str): 'ps' or 'worker'
            host_workdir_prefix (str): host cwd prefix
        """
        mount_dirs = []
        if type == "ps":
            for i in range(self.resources.ps.num_ps):
                postfix = f"{self.name}-ps-{i}"
                mount_dir = os.path.join(host_workdir_prefix, postfix)
                mount_dirs.append(mount_dir)
                cmd = f'ssh {self.ps_placement[i]} "rm -rf {mount_dir}; mkdir -p {mount_dir}"'
                os.system(cmd)

        elif type == "worker":
            for i in range(self.resources.worker.num_worker):
                postfix = f"{self.name}-worker-{i}"
                mount_dir = os.path.join(host_workdir_prefix, postfix)
                mount_dirs.append(mount_dir)
                cmd = f'ssh {self.worker_placement[i]} "rm -rf {mount_dir}; mkdir -p {mount_dir}"'
                os.system(cmd)

        return mount_dirs

    def __set_batch_size(self):
        """Sets the batch size for training job.
        The batch size of each worker for sync training may be different
        """
        if self.envs.kv_store == "dist_async":
            self.batch_sizes = [str(self.metadata.batch_size) for i in range(self.resources.worker.num_worker)]
        elif self.envs.kv_store == "dist_sync" or self.envs.kv_store == "dist_device_sync":
            # will change global batch size during training.
            if self.metadata.scale_bs:
                self.batch_sizes = [str(self.metadata.batch_size) for i in range(self.resources.worker.num_worker)]
            else:
                avg_batch_size = self.metadata.batch_size / self.resources.worker.num_worker
                rem_batch_size = self.metadata.batch_size % self.resources.worker.num_worker
                batch_sizes = [avg_batch_size for i in range(self.resources.worker.num_worker)]
                for i in range(rem_batch_size):
                    batch_sizes[i] = batch_sizes[i] + 1
                self.batch_sizes = [str(i) for i in batch_sizes]

        if self.envs.kv_store == "dist_sync":
            self.epoch_size = self.metadata.num_examples / self.metadata.batch_size
        elif self.envs.kv_store == "dist_async":
            self.epoch_size = self.metadata.num_examples / self.metadata.batch_size / self.resources.worker.num_worker

    def _create_jobs(self):
        """Create Kubernetes job object"""
        job_conf_base = {
            "script": self.container.init_script,
            "prog": self.envs.prog_cmd,
            "work_dir": self.data.work_dir,
            "work_volume": "k8s-mxnet-work-volume",
            "data_dir": self.data.data_dir,
            "data_volume": "k8s-mxnet-data-volume",
            "host_data_dir": self.data.host_data_dir,
            "num_ps": self.resources.ps.num_ps,
            "ps_cpu": self.resources.ps.ps_cpu,
            "ps_mem": str(self.resources.ps.ps_mem) + "Gi",
            "num_worker": self.resources.worker.num_worker,
            "worker_cpu": self.resources.worker.worker_cpu,
            "worker_mem": str(self.resources.worker.worker_mem) + "Gi",
            "worker_gpu": self.resources.worker.worker_gpu,
            "kv_store": self.envs.kv_store,
            "MXNET_KVSTORE_BIGARRAY_BOUND": self.envs.kv_store_big_array_bound,
            "ps_verbose": self.envs.ps_verbose,
        }

        jobs = []
        for j in range(self.resources.worker.num_worker):
            worker_job_conf = {
                "host_mount_dir": self.worker_mount_dirs[j],
                "worker_placement": self.worker_placement[j],
                "batch_size": self.batch_sizes[j],
            }
            job = Job(
                name=self.name,
                type="worker",
                replica_id=j,
                image=self.container.image,
                job_conf={**job_conf_base, **worker_job_conf},
            )
            jobs.append(job)

        for j in range(self.resources.ps.num_ps):
            ps_job_conf = {"host_mount_dir": self.ps_mount_dirs[j], "ps_placement": self.ps_placement[j]}
            job = Job(
                name=self.name,
                type="ps",
                replica_id=j,
                image=self.container.image,
                job_conf={**job_conf_base, **ps_job_conf},
            )
            jobs.append(job)

        return jobs

    async def _read_data(self):
        """Read training data from localhost, otherwise from HDFS.
        A thread is created for each worker and tries to load the data.
        """
        if self.data.data_mounted:
            return
        if self.data.hdfs_data is None or self.data.hdfs_data == "":
            raise ValueError("data is not mounted from localhost and hdfs_data is not specified")
        proc_list = []
        for i in range(self.resources.worker.num_worker):
            node = self.worker_placement[i]

            # get training and validation data from HDFS
            for data in self.data.hdfs_data:
                fn = data.split("/")[-1]
                local_file = self.worker_mount_dirs[i] + fn
                # force copy even exist: some file may be broken due to interruption
                cmd = f'ssh {node} "/usr/local/hadoop/bin/hadoop fs -copyToLocal -f {data} {local_file}"'
                proc_list.append(asyncio.create_task(asyncio.create_subprocess_shell(cmd)))

        await asyncio.gather(*proc_list)

    async def _read_progress_stats(self):
        """Get the job progress from each worker."""
        progress_fn = "progress.txt"

        # create a new one each time, since the number of workers will change, hence the size of progress list
        self.progress_list = [(0, 0) for i in range(self.resources.worker.num_worker)]
        self.val_loss_list = [(0, 0) for i in range(self.resources.worker.num_worker)]

        async def run(i):
            try:
                progress_epoch, progress_batch, val_loss = await asyncio.gather(
                    fetch_with_timeout(redis_connection, f"{self.name}-progress_epoch", 1000),
                    fetch_with_timeout(redis_connection, f"{self.name}-progress_batch", 1000),
                    fetch_with_timeout(redis_connection, f"{self.name}-val_loss", 1000),
                )

                self.progress_list[i] = (progress_epoch, progress_batch)
                self.val_loss_list[i] = val_loss
            except Exception as e:
                logger.error("Failed to read training metrics from redis:", str(e))

        coroutine_list = []
        for i in range(self.resources.worker.num_worker):
            coroutine_list.append(run(i))

        await asyncio.gather(*coroutine_list)

    async def get_training_progress_stats(self):
        await self._read_progress_stats()
        return (list(self.progress_list), list(self.val_loss_list))

    async def _read_training_speed(self):
        """Get the job training speed from each worker"""
        self.speed_list = [0 for i in range(self.resources.worker.num_worker)]

        async def run(i):
            try:
                stb_speed = await fetch_with_timeout(redis_connection, f"{self.name}-stb_speed", 1000)
                self.speed_list[i] = float(str(stb_speed.decode("utf-8")))
            except Exception as e:
                logger.error("Failed to read training metrics from redis:", str(e))

        coroutine_list = []
        for i in range(self.resources.worker.num_worker):
            coroutine_list.append(run(i))

        await asyncio.gather(*coroutine_list)

    async def get_training_speed(self):
        await self._read_training_speed()
        return list(self.speed_list)

    def __get_pods_names(self):
        """Get the names of the pods belonging to the task

        NAME                                    READY     STATUS    RESTARTS   AGE
        1-measurement-imagenet-ps-0-mzv2z       1/1       Running   0          1m
        """
        self.ps_pods = []
        self.worker_pods = []

        pods_name = k8s_api.get_pods_attribute("metadata.name", label_selector={"name": self.name})
        for pod_name in pods_name:
            if "ps" in pod_name:
                self.ps_pods.append(pod_name)
            elif "worker" in pod_name:
                self.worker_pods.append(pod_name)

    async def _read_metrics(self):
        """Get the metrics of the pods for the job"""
        self.__get_pods_names()

        # get heapster cluster ip
        heapster_service = k8s_api.get_services("kube-system", field_selector={"metadata.name": "heapster"})[0]
        heapster_cluster_ip = utils.rgetattr(heapster_service, "spec.cluster_ip")

        self.ps_metrics = []
        self.worker_metrics = []

        # TODO: heapster is deprecated, we need to move to metrics API
        # cpu: milli core, mem: bytes, net: bytes/second
        metric_keys = ["cpu/usage_rate", "memory/usage", "network/tx_rate", "network/rx_rate"]
        for pod in self.ps_pods + self.worker_pods:
            pod_metrics = {}
            for metric_key in metric_keys:
                url = f"http://{heapster_cluster_ip}/api/v1/model/namespaces/default/pods/{pod}/metrics/{metric_key}"
                try:
                    async with aiohttp.ClientSession as session:
                        output = await session.get(url).json()
                        # get latest value, maybe empty since heapster update metrics per minute
                        metric_value = int(output["metrics"][-1]["value"])
                except:
                    # print "ERROR when requesting pod metrics!"
                    metric_value = 0
                pod_metrics[metric_key] = metric_value
            if pod in self.ps_pods:
                self.ps_metrics.append(pod_metrics)
            else:
                self.worker_metrics.append(pod_metrics)

    async def get_metrics(self):
        """Get job metrics"""
        await self._read_metrics()
        return list(self.ps_metrics), list(self.worker_metrics)

    async def start(self):
        """Start the job in k8s, with these steps:
        - Creating job directory
        - Mounting data on ps and workers
        - Create the YAML file
        - Reading data
        - Submitting job to k8s
        """
        logger.info(f"starting job {self.name} ...")

        # job working dir on host
        os.makedirs(self.dir)

        self.ps_mount_dirs = self.__set_mount_dirs("ps", self.data.host_workdir_prefix)  # ps container mount
        self.worker_mount_dirs = self.__set_mount_dirs(
            "worker", self.data.host_workdir_prefix
        )  # worker container mount
        self.__set_batch_size()

        self.running_tasks = self._create_jobs()

        # prepare data
        await self._read_data()

        # start pods in k8s. equivalent to microk8s kubectl create -f jobs.yaml
        for job in self.running_tasks:
            k8s_api.submit_job(job.k8s_job_obj)

    async def delete(self, del_all=False):
        """Delete the job.

        Args:
            del_all (bool): whether to delete all, including histories.
        """

        # shutdown job in k8s
        executor = concurrent.futures.ThreadPoolExecutor()
        loop = asyncio.get_event_loop()
        # TODO: self.name for k8s task name? maybe wrong?
        blocking_tasks = [loop.run_in_executor(executor, k8s_api.delete_job, task.uname) for task in self.running_tasks]

        await asyncio.wait(blocking_tasks)

        blocking_tasks = []
        # remove mounted dirs on hosts
        if self.worker_mount_dirs and del_all:
            for i in range(self.resources.worker.num_worker):
                node = self.worker_placement[i]
                worker_mount_dir = self.worker_mount_dirs[i]
                cmd = f'timeout 10 ssh {node} "rm -r {worker_mount_dir}"'
                blocking_tasks.append(loop.run_in_executor(executor, os.system, cmd))

            for i in range(self.resources.ps.num_ps):
                node = self.ps_placement[i]
                ps_mount_dir = self.ps_mount_dirs[i]
                cmd = f'timeout 10 ssh {node} "rm -r {ps_mount_dir}"'
                blocking_tasks.append(loop.run_in_executor(executor, os.system, cmd))

            await asyncio.wait(blocking_tasks)

            # delete job working dir
            shutil.rmtree(self.dir)

    def get_total_required_resources(self):
        """Returns: dict containing the required amount of resources to host this job."""
        # if we use the dist_strategy ps we also need to count the resources required by the parameter servers
        required_cpu = (
            self.resources.worker.num_worker * self.resources.worker.worker_cpu
            + self.resources.ps.num_ps * self.resources.ps.ps_cpu
        )
        required_mem = (
            self.resources.worker.num_worker * self.resources.worker.worker_mem
            + self.resources.ps.num_ps * self.resources.ps.ps_mem
        )
        required_bw = (
            self.resources.worker.num_worker * self.resources.worker.worker_bw
            + self.resources.ps.num_ps * self.resources.ps.ps_bw
        )
        required_gpu = self.resources.worker.num_worker * self.resources.worker.worker_gpu

        return {"cpu": required_cpu, "mem": required_mem, "bw": required_bw, "gpu": required_gpu}
