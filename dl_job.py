import time
from datetime import datetime
import os
import sys
import threading
import subprocess
import requests
import ast
import logging
import shutil

import config
import utils

from k8s.api import KubeAPI
from k8s.job import Job

logger = logging.getLogger(config.LOGGER_NAME)
k8s_api = KubeAPI()

class DLJob():
    """Job class defines the structure of a DL training job.

    Attributes:
        id (int): job unique id -- incremental style
        tag (str): eg., measurement-imagenet, i.e., category-dataset
        model_name (str): deep-learning network name
        workload_id (int): unique index for the job. useful for identifying 
            the job characteristic in the future.
        name (str): job name as in '{id}-{tag}-{model_name}'. e.g. '1-measurement-imagenet-vgg16'
        timestamp (str): job creation time as in '%Y-%m-%d-%H:%M:%S'
        dir (str): job working directory as in '{dir_prefix}/{name}-{timestamp}/}'

    """
    def __init__(self, id, workload_id, dir_prefix, conf):
        """Initializes a job object.
        
        Args:
            id (int): job unique id -- incremental style
            tag (str): eg., measurement-imagenet, i.e., category-dataset
            model_name (str): deep-learning network name
            workload_id (int): unique index for the job. useful for identifying 
                the job characteristic in the future.
            dir_prefix (str): job working directory
        """
        self.metadata = utils.objectview(conf['metadata'])
        self.resources = utils.objectview(conf['resources'])
        self.container = utils.objectview(conf['container'])
        self.data = utils.objectview(conf['data'])
        self.envs = utils.objectview(conf['envs'])

        self.id = id
        self.tag = tag
        self.workload_id = workload_id
        self.name = f'{id}-{self.metadata.tag}-{self.metadata.modelname}'

        self.timestamp = datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
        self.dir = os.path.join(dir_prefix, f'{self.name}-{self.timestamp}')

        self.ps_placement = None
        self.worker_placement = None

        self.speed_list = []

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
        self.status = 'initialized'
        self.progress = 0

        # (num_ps, num_worker): speed
        self.training_speeds = dict()

        # epoch : validation_loss
        self.val_losses = dict()
        self.num_epochs = 0
        self.epoch_size = 0

    # def set_config(self):
    #     """Set training job config.
    #     image (str): container image URI, e.g. localhost:5000/k8s-mxnet-cu100
    #     script (str): script to execute, e.g. /init.sh
    #     work_dir (str): working directory on container, e.g. /mxnet/example/image-classification/data/
    #     host_workdir_prefix (str): host working directory, e.g. /data/k8s-workdir/experiment/
    #     work_volume (str): TBD

    #     hdfs_data (list): list of hdfs paths, e.g. ['/k8s-mxnet/imagenet/imagenet-train.rec']
    #     data_dir (str): data directory, e.g. /data
    #     host_data_dir (str): data directory on the host, e.g. /data/mxnet-data/imagenet/
    #     data_mounted (bool): Is data mounted?
    #     data_volume (str): TBD

    #     hdfs_data (list): list of hdfs paths, e.g. ['/k8s-mxnet/imagenet/imagenet-train.rec']
    #     data_dir (str): data directory, e.g. /data
    #     host_data_dir (str): data directory on the host, e.g. /data/mxnet-data/imagenet/
    #     data_mounted (bool): Is data mounted?
    #     data_volume (str): TBD

    #     prog (str): command to invoke to start the training process. 
    #         e.g. python train_imagenet.py --network resnet --num-layers 50 --disp-batches 5 --num-epochs 100 --data-train /data/imagenet-train.rec
    #     batch_size (int): batch size, e.g. 32
    #     kv_store (str): MXNet KVStore is a place for data sharing. 
    #         Think of it as a single object shared across different devices (GPUs and computers), 
    #         where each device can push data in and pull data out. Possible values:
    #             *dist_sync*: Behaves similarly to local but with one major difference. With dist_sync, batch-size now means the batch size used on each machine. So if there are n machines and we use batch size b, then dist_sync behaves like local with batch size n * b.
    #             *dist_device_sync*: Identical to dist_sync with the difference similar to device vs local.
    #             *dist_async*: Performs asynchronous updates. The weights are updated whenever gradients are received from any machine. No two updates happen on the same weight at the same time. However, the order is not guaranteed.
    #     scale_bs (bool): adjustable batch size?
    #     num_examples (int): number of training dataset, required for computing the epoch_size
    #     num_epochs (int): number of epochs
    #     ---
    #     Setting dataset directory for the training job.
    #     If data not in local host, fetch from HDFS.
    #     Dataset list including training data and validation data.
    #     ---
    #     Setting dataset directory for the training job.
    #     If data not in local host, fetch from HDFS.
    #     Dataset list including training data and validation data.
    #     """
    #     # setting parameter-server parameters
    #     self.num_ps = conf.num_ps
    #     self.ps_cpu = conf.ps_cpu
    #     self.ps_mem = conf.ps_mem
    #     self.ps_bw = conf.ps_bw

    #     #setting worker parameters
    #     self.num_worker = conf.num_worker
    #     self.worker_cpu = conf.worker_cpu
    #     self.worker_mem = conf.worker_mem
    #     self.worker_bw = conf.worker_bw
    #     self.worker_gpu = conf.worker_gpu

    #     # setting container and work volumes
    #     self.image = conf.image
    #     self.script = conf.script
    #     self.work_dir = conf.work_dir
    #     self.host_workdir_prefix = conf.host_workdir_prefix
    #     self.work_volume = conf.work_volume

    #     # setting training job 
    #     self.prog = conf.prog
    #     self.tot_batch_size = conf.batch_size
    #     self.kv_store = conf.kv_store
    #     self.scale_bs = conf.scale_bs
    #     self.num_examples = conf.num_examples
    #     # for unknown num_epochs, will update it in progressor with estimation
    #     self.num_epochs = conf.num_epochs

    #     #setting dataset
    #     self.hdfs_data = conf.hdfs_data
    #     self.data_dir = conf.data_dir
    #     self.host_data_dir = conf.host_data_dir
    #     self.data_mounted = conf.data_mounted
    #     self.data_volume = conf.data_volume

    #     self.kv_store_big_array_bound = str(conf.kv_store_big_array_bound)
    #     self.ps_verbose = conf.ps_verbose

    def set_ps_placement(self, ps_placement):
        """Setting the placement of parameter servers.

        Args:
            ps_placement (list): list of parameter servers ip addresses
        """
        if isinstance(ps_placement, list):
            if len(ps_placement) == self.resources.ps.num_ps:
                self.ps_placement = ps_placement
            else:
                raise RuntimeError('ps_placement is not consistent with num_ps')
        else:
            raise TypeError('ps_placement is not a list')

    def set_worker_placement(self, worker_placement):
        """Setting the placement of workers.

        Args:
            worker_placement (list): list of workers ip addresses
        """
        if isinstance(worker_placement, list):
            if len(worker_placement) == self.resources.worker.num_worker:
                self.worker_placement = worker_placement
            else:
                raise RuntimeError('worker_placement is not consistent with num_worker')
        else:
            raise TypeError('worker_placement is not a list')

    def __set_mount_dirs(self, type, host_workdir_prefix):
        """Setting the directories on hosts to be mounted on containers

        Args:
            type (str): 'ps' or 'worker'
            host_workdir_prefix (str): host cwd prefix
        """
        mount_dirs = []
        if type == 'ps':
            for i in range(self.resources.ps.num_ps):
                postfix = f'{self.name}-ps-{i}'
                mount_dir = os.path.join(host_workdir_prefix, postfix)
                mount_dirs.append(mount_dir)
                cmd = f'ssh {self.ps_placement[i]} "rm -rf {mount_dir}; mkdir -p {mount_dir}"'
                os.system(cmd)

        elif type == 'worker':
            for i in range(self.resources.worker.num_worker):
                postfix = f'{self.name}-worker-{i}'
                mount_dir = os.path.join(host_workdir_prefix, postfix)
                mount_dirs.append(mount_dir)
                cmd = f'ssh {self.worker_placement[i]} "rm -rf {mount_dir}; mkdir -p {mount_dir}"'
                os.system(cmd)

        return mount_dirs

    def __set_batch_size(self):
        """Sets the batch size for training job.
        The batch size of each worker for sync training may be different
        """
        if self.envs.kv_store == 'dist_async':
            self.batch_sizes = [str(self.metadata.batch_size) for i in range(self.resources.worker.num_worker)]
        elif self.envs.kv_store == 'dist_sync' or self.envs.kv_store == 'dist_device_sync':
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

        if 'sync' in self.envs.kv_store:
            self.epoch_size = self.num_examples / self.metadata.batch_size
        elif 'async' in self.kv_store:
            self.epoch_size = self.num_examples / self.metadata.batch_size / self.resources.worker.num_worker

    def _create_jobs(self):
        """Create Kubernetes job object
        """
        job_conf_base = {
            'script': self.container.init_script,
            'prog': self.envs.prog_cmd,
            'work_dir': self.data.work_dir,
            'work_volume': self.data.work_volume,
            'data_dir': self.data.data_dir,
            'data_mount_dir': self.data.host_data_dir,
            'data_volume': self.data.data_volume,

            'num_ps': self.resources.ps.num_ps,
            'ps_cpu': self.resources.ps.ps_cpu,
            'ps_mem': str(self.resources.ps.ps_mem) + "Gi",

            'num_worker': self.resources.worker.num_worker,
            'worker_cpu': self.resources.worker.worker_cpu,
            'worker_mem': str(self.resources.worker.worker_mem) + "Gi",
            'worker_gpu': self.resources.worker.worker_gpu,

            'kv_store': self.envs.kv_store,
            'MXNET_KVSTORE_BIGARRAY_BOUND': self.envs.kv_store_big_array_bound,
            'ps_verbose': self.envs.ps_verbose,
        }

        jobs = []
        for j in range(num_worker):
            worker_job_conf = {
                'worker_mount_dirs': self.worker_mount_dirs[j],
                'worker_placement': self.worker_placement[j],
                'batch_size': self.batch_sizes[j]
            }
            jobs.append(Job(name=self.name,
                            type='worker',
                            id=j,
                            image=self.container.image,
                            job_conf={**job_conf_base, **worker_job_conf}))

        for j in range(num_ps):
            ps_job_conf = {
                'ps_mount_dirs': self.ps_mount_dirs[j],
                'ps_placement': self.ps_placement[j]
            }
            jobs.append(Job(name=self.name,
                            type='worker',
                            id=j,
                            image=self.container.image,
                            job_conf={**job_conf_base, **ps_job_conf}))

        return jobs


        #create the k8s yaml file by parsing the jinja template
        # jinja_file_path = os.path.join(self.dir, f'{self.name}.jinja')
        # yaml_file_path = os.path.join(self.dir, f'{self.name}.yaml')
        # with open(os.path.join('templates','k8s-mxnet-template.jinja'), 'r') as jinja_tpl_fp:
        #     jinja_content = jinja_fp.read()
        #     for key, value in variables.items():
        #         jinja_content.replace(f'${key}', value)

        #     with open(jinja_file_path, 'w') as jinja_fp:
        #         jinja_fp.write(jinja_content)

        #     with open(yaml_file_path, 'w') as yaml_fp:
        #         yaml_fp.write(jinja2.Template(jinja_content).render())

    def _read_data(self):
        """Read training data from localhost, otherwise from HDFS.
        A thread is created for each worker and tries to load the data.
        """
        if self.data.data_mounted:
            return
        if self.data.hdfs_data is None or self.data.hdfs_data == '':
            raise ValueError('data is not mounted from localhost and hdfs_data is not specified')
        thread_list = []
        for i in range(self.resources.worker.num_worker):
            node = self.worker_placement[i]

            # get training and validation data from HDFS
            for data in self.data.hdfs_data:
                fn = data.split("/")[-1]
                local_file = self.worker_mount_dirs[i] + fn
                # force copy even exist: some file may be broken due to interruption
                cmd = f'ssh {node} "/usr/local/hadoop/bin/hadoop fs -copyToLocal -f {data} {local_file}"'
                thread_train = threading.Thread(target=(lambda cmd=cmd: os.system(cmd)), args=())
                thread_train.start()
                thread_list.append(thread_train)

        for thread in thread_list:
            thread.join()

    def _read_progress_stats(self):
        """Get the job progress from each worker.
        """
        progress_fn = 'progress.txt'

        # create a new one each time, since the number of workers will change, hence the size of progress list
        self.progress_list = [(0,0) for i in range(self.resources.worker.num_worker)]
        self.val_loss_list = [(0,0) for i in range(self.resources.worker.num_worker)]
        thread_list = []
        for i in range(self.resources.worker.num_worker):
            node = self.worker_placement[i]
            local_file = os.path.join(self.worker_mount_dirs[i], progress_fn)
            cmd = f'ssh {node} "cat {local_file}"'

            def run(self, cmd, i):
                try:
                    output = subprocess.check_output(cmd, shell=True)
                    counter = 0
                    while output == '' or output == None:
                        output = subprocess.check_output(cmd, shell=True)
                        time.sleep(0.001 * (10 ** counter))
                        counter = counter + 1
                        if counter > 2:
                            break
                    if output is not None and output != '':
                        # should not be empty, even no progress, there should be initialization values written in files.
                        stat_dict = ast.literal_eval(output.replace('\n', ''))
                        if "progress" in stat_dict and "val-loss" in stat_dict:
                            self.progress_list[i] = stat_dict["progress"]

                            # it is a list of (epoch, loss)
                            self.val_loss_list[i] = stat_dict["val-loss"]
                        else:
                            logger.info("Job:: " + "progress output does not have progress or val-loss value")
                    else:
                        logger.info("Job:: " + "the progress output is empty.")
                except Exception as e:
                    logger.error("Job:: " + "_read_progress_stats: " + str(e) + " : " + output)

            thread = threading.Thread(target=run, args=(self, cmd, i))
            thread.start()
            thread_list.append(thread)
        for thread in thread_list:
            thread.join()

    def get_training_progress_stats(self):
        self._read_progress_stats()
        return (list(self.progress_list), list(self.val_loss_list))

    def _read_training_speed(self):
        """Get the job training speed from each worker
        """
        speed_fn = 'speed.txt'
        self.speed_list = [0 for i in range(self.resources.worker.num_worker)]
        thread_list = []
        for i in range(self.resources.worker.num_worker):
            node = self.worker_placement[i]
            local_file = os.path.join(self.worker_mount_dirs[i], speed_fn)

            cmd = 'ssh {node} "cat {local_file}"'

            def run(self, cmd, i):
                try:
                    output = subprocess.check_output(cmd, shell=True)

                    # the other side is opening and writing the file, try again
                    counter = 0
                    while output == '' or output == None:
                        output = subprocess.check_output(cmd, shell=True)
                        time.sleep(0.001*(10**counter))
                        counter = counter + 1
                        if counter > 2:
                            logger.error("Job:: " + "_read_training_speed: read training speed timeout.")
                            return
                    stb_speed = float(output.replace('\n', '').split(' ')[1])
                    self.speed_list[i] = float('%.3f'%(stb_speed))
                except Exception as e:
                    logger.error("Job:: " + "_read_training_speed: " + str(e))

            thread = threading.Thread(target=run, args=(self, cmd, i))
            thread.start()
            thread_list.append(thread)
        for thread in thread_list:
            thread.join()

    def get_training_speed(self):
        self._read_training_speed()
        return list(self.speed_list)

    def __get_pods_names(self):
        """Get the names of the pods belonging to the task

        NAME                                    READY     STATUS    RESTARTS   AGE
        1-measurement-imagenet-ps-0-mzv2z       1/1       Running   0          1m
        """
        self.ps_pods = []
        self.worker_pods = []

        pods_name = k8s_api.get_pods_attribute('metadata.name', label_selector={'name': self.name})
        for pod_name in pods_name:
            if 'ps' in pod_name:
                self.ps_pods.append(pod_name)
            elif 'worker' in pod_name:
                self.worker_pods.append(pod_name)

    def _read_metrics(self):
        """Get the metrics of the pods for the job
        """
        self.__get_pods_names()

        # get heapster cluster ip
        heapster_service = k8s_api.get_services('kube-system', field_selector={'metadata.name':'heapster'})[0]
        heapster_cluster_ip = utils.rgetattr(heapster_service, 'spec.cluster_ip')
  
        self.ps_metrics = []
        self.worker_metrics = []

        #TODO: heapster is deprecated, we need to move to metrics API
        # cpu: milli core, mem: bytes, net: bytes/second
        metric_keys = ['cpu/usage_rate', 'memory/usage', 'network/tx_rate', 'network/rx_rate']
        for pod in (self.ps_pods + self.worker_pods):
            pod_metrics = {}
            for metric_key in metric_keys:
                url = f'http://{heapster_cluster_ip}/api/v1/model/namespaces/default/pods/{pod}/metrics/{metric_key}'
                try:
                    output = requests.get(url, verify=False).json()
                    # get latest value, maybe empty since heapster update metrics per minute
                    metric_value = int(output['metrics'][-1]['value'])
                except:
                    # print "ERROR when requesting pod metrics!"
                    metric_value = 0
                pod_metrics[metric_key] = metric_value
            if pod in self.ps_pods:
                self.ps_metrics.append(pod_metrics)
            else:
                self.worker_metrics.append(pod_metrics)

    def get_metrics(self):
        """Get job metrics
        """
        self._read_metrics()
        return (list(self.ps_metrics), list(self.worker_metrics))

    
    def start(self):
        """Start the job in k8s, with these steps:
            - Creating job directory
            - Mounting data on ps and workers
            - Create the YAML file
            - Reading data
            - Submitting job to k8s
        """
        logger.info("starting job " + self.name + "...")

        # job working dir on host
        os.makedirs(self.dir)

        self.ps_mount_dirs = self.__set_mount_dirs('ps', self.host_workdir_prefix)  # ps container mount
        self.worker_mount_dirs = self.__set_mount_dirs('worker', self.host_workdir_prefix)  # worker container mount
        self.__set_batch_size()

        self.running_jobs = self._create_jobs()

        # prepare data
        self._read_data()

        # start pods in k8s. equivalent to microk8s kubectl create -f jobs.yaml
        for job in self.running_jobs:
            k8s_api.submit_job(job)

    def delete(self, del_all=False):
        """Delete the job.
        
        Args:
            del_all (bool): whether to delete all, including histories.
        """

        # shutdown job in k8s
        for job in self.running_jobs:
            job_name = job.metadata.name
            thread = threading.Thread(target=(lambda name=job_name: k8s_api.delete_job(name)), args=())
            thread.start()
            thread_list.append(thread)

        for thread in thread_list:
            thread.join()
        os.system('rm -rf ' + temp_dir)

        if not del_all:
            return

        # remove mounted dirs on hosts
        thread_list = []
        for i in range(self.resources.worker.num_worker):
            node = self.worker_placement[i]
            worker_mount_dir = self.worker_mount_dirs[i]
            cmd = f'timeout 10 ssh {node} "rm -r {worker_mount_dir}"'
            thread = threading.Thread(target=(lambda cmd=cmd: os.system(cmd)), args=())
            thread.start()
            thread_list.append(thread)

        for i in range(self.resources.ps.num_ps):
            node = self.ps_placement[i]
            ps_mount_dir = self.ps_mount_dirs[i]
            cmd = f'timeout 10 ssh {node} "rm -r {ps_mount_dir}"'
            thread = threading.Thread(target=(lambda cmd=cmd: os.system(cmd)), args=())
            thread.start()
            thread_list.append(thread)

        for thread in thread_list:
            thread.join()

        # delete job working dir
        shutil.rmtree(self.dir)
