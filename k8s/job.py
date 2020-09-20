from kubernetes import client
import yaml

from .api import KubeAPI

k8s_api = KubeAPI()

class Job():
    def __init__(self, name, type, replica_id, image, job_conf, namespace='default'):
        self.name = name
        self.type = type
        self.replica_id = id
        self.image = image
        self.namespace = namespace
        self.conf = job_conf

    def __create_resources(self):
        if self.type == 'worker':
            limits = requests = {
                'cpu': self.conf.get('worker_cpu'),
                'memory': self.conf.get('worker_mem'),
                'nvidia.com/gpu': self.conf.get('worker_gpu')
            }
        else:
            limits = requests = {
                'cpu': self.conf.get('ps_cpu'),
                'memory': self.conf.get('ps_mem')
            }

        return client.V1ResourceRequirements(limits=limits, requests=requests)
    
    def __create_volume_mounts(self):
        return [
            client.V1VolumeMount(mount_path=self.conf.get('work_dir'), name=self.conf.get('work_volume')),
            client.V1VolumeMount(mount_path=self.conf.get('data_dir'), name=self.conf.get('data_volume')),
            client.V1VolumeMount(mount_path='/usr/local/nvidia/lib', name='nvidia-lib'),
            client.V1VolumeMount(mount_path='/usr/local/nvidia/lib64', name='nvidia-lib64')
        ]

    def __create_envs(self):
        return [
            client.V1EnvVar(name='JOB_NAME', value=self.name),
            client.V1EnvVar(name='DMLC_NUM_WORKER', value=self.conf.get('num_worker')),
            client.V1EnvVar(name='DMLC_NUM_SERVER', value=self.conf.get('num_ps')),
            client.V1EnvVar(name='ROLE', value=self.type),
            client.V1EnvVar(name='PROG', value=self.conf.get('prog')),
            client.V1EnvVar(name='WORK_DIR', value=self.conf.get('work_dir')),
            client.V1EnvVar(name='DATA_DIR', value=self.conf.get('data_dir')),
            client.V1EnvVar(name='KV_STORE', value=self.conf.get('kv_store')),
            client.V1EnvVar(name='BATCH_SIZE', value=self.conf.get('batch_size', 0)),
            client.V1EnvVar(name='MXNET_KVSTORE_BIGARRAY_BOUND', value=self.conf.get('MXNET_KVSTORE_BIGARRAY_BOUND')),
            client.V1EnvVar(name='PS_VERBOSE', value=self.conf.get('ps_verbose'))
        ]

    def __create_containers(self):
        return [
            client.V1Container(
                name=self.name,
                image=self.image,
                imagePullPolicy='IfNotPresent',
                command=["/bin/bash"],
                args=["/init.sh"],
                env=self.__create_envs(),
                resources=self.__create_resources(),
                ports=[client.V1ContainerPort(container_port=6006)],
                volumeMounts=self.__create_volume_mounts())
        ]
        

    def __create_volumes(self):
        return [
            client.V1Volume(
                name=self.conf.work_volume, 
                #TODO: fix the mount dir (ps, worker)?
                host_path=client.V1HostPathVolumeSource(path=self.conf.get('mount_dir'))),
            client.V1Volume(
                name=self.conf.data_volume, 
                #TODO: fix the mount dir (ps, worker)?
                host_path=client.V1HostPathVolumeSource(path=self.conf.get('host_data_dir'))),
            client.V1Volume(
                name='nvidia-lib',
                #TODO: fix the nvidia path
                host_path=client.V1HostPathVolumeSource(path='/usr/lib/nvidia-384/')),
            client.V1Volume(
                name='nvidia-lib64',
                #TODO: fix the nvidia path
                host_path=client.V1HostPathVolumeSource(path='/usr/lib/x86_64-linux-gnu/'))
        ]

    def _create_job_obj(self):
        # Configureate Pod template container
        container = self.__create_containers()
        volumes = self.__create_volumes()

        # Create and configurate a spec section
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"name": self.name, "type":self.type, "id":self.replica_id}),
            spec=client.V1PodSpec(restart_policy="Never", containers=container, volumes=volumes))
        
        # Create the specification of deployment
        spec = client.V1JobSpec(
            template=template,
            backoff_limit=4,
            ttl_seconds_after_finished=600
            )
        
        # Instantiate the job object
        self.job_obj = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=self.name, namespace=namespace),
            spec=spec,
            status=client.V1JobStatus()
            )

        return self.job_obj

    def to_yaml(self, output='stdout'):
        yaml_content = yaml.dump(self.job_obj.to_dict())
        if output == 'stdout':
            print(yaml_content)
        else:
            with open(f'{self.name}.yaml', 'w') as fp:
                fp.write(yaml_content)

