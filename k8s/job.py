from kubernetes.client.models.v1_env_var import V1EnvVar
from munch import munchify, Munch
from typing import Any, Dict, List
import yaml
from .api import KubeAPI
import config
from kubernetes import client
import utils
import os


k8s_api = KubeAPI()


class Job:
    def __init__(
        self,
        name: str,
        type: str,
        replica_id: int,
        image: str,
        job_conf: Munch,
        namespace: dict = config.k8s_params["namespace"],
    ) -> None:
        self.name = name
        self.uname = f"{name}-{type}-{replica_id}"
        self.type = type
        self.replica_id = replica_id
        self.image = image
        self.namespace = namespace
        self.conf = job_conf

        self.k8s_job_obj = self._create_job_obj()

    def __create_resources(self) -> client.V1ResourceRequirements:
        if self.type == "worker":
            limits = requests = {
                "cpu": self.conf.get("worker_cpu"),
                "memory": self.conf.get("worker_mem"),
                "nvidia.com/gpu": self.conf.get("worker_gpu"),
            }
        else:
            limits = requests = {"cpu": self.conf.get("ps_cpu"), "memory": self.conf.get("ps_mem")}

        return client.V1ResourceRequirements(limits=limits, requests=requests)

    def __create_volume_mounts(self) -> List[client.V1VolumeMount]:
        return [
            client.V1VolumeMount(mount_path=config.JOB_MOUNT_POD, name="job-volume"),
            client.V1VolumeMount(mount_path="/usr/local/nvidia/lib", name="nvidia-lib"),
            client.V1VolumeMount(mount_path="/usr/local/nvidia/lib64", name="nvidia-lib64"),
        ]

    def __create_envs(self) -> List[client.V1EnvVar]:
        return [
            client.V1EnvVar(name="JOB_NAME", value=self.name),
            client.V1EnvVar(name="DMLC_NUM_WORKER", value=str(self.conf.get("num_worker"))),
            client.V1EnvVar(name="DMLC_NUM_SERVER", value=str(self.conf.get("num_ps"))),
            client.V1EnvVar(name="REPLICA_ID", value=str(self.replica_id)),
            client.V1EnvVar(name="ROLE", value=self.type),
            client.V1EnvVar(name="PROG", value=self.conf.get("prog")),
            client.V1EnvVar(name="WORK_DIR", value=self.conf.get("work_dir")),
            client.V1EnvVar(name="DATA_DIR", value=self.conf.get("data_dir")),
            client.V1EnvVar(name="KV_STORE", value=self.conf.get("kv_store")),
            client.V1EnvVar(name="BATCH_SIZE", value=str(self.conf.get("batch_size", 0))),
            client.V1EnvVar(name="MXNET_KVSTORE_BIGARRAY_BOUND", value=self.conf.get("MXNET_KVSTORE_BIGARRAY_BOUND")),
            client.V1EnvVar(name="PS_VERBOSE", value=self.conf.get("ps_verbose")),
            client.V1EnvVar(name="REDIS_HOST", value=config.REDIS_HOST_WORKER),
            client.V1EnvVar(name="REDIS_PORT", value=str(config.REDIS_PORT_WORKER)),
            client.V1EnvVar(name="TRAINING_LOG_DIR", value=str(config.TRAINING_LOG_DIR)),
            client.V1EnvVar(name="TRAINING_LOG_FILE", value=str(config.TRAINING_LOG_FILE)),
            client.V1EnvVar(name="FRAMEWORK", value=str(self.conf.get("framework"))),
        ]

    def __create_containers(self) -> List[client.V1Container]:
        return [
            client.V1Container(
                name=self.name,
                image=self.image,
                image_pull_policy="Always",
                command=["/bin/bash"],
                args=["/init.sh"],
                env=self.__create_envs(),
                resources=self.__create_resources(),
                ports=[client.V1ContainerPort(container_port=6006)],
                volume_mounts=self.__create_volume_mounts(),
            )
        ]

    def __create_volumes(self) -> List[client.V1Volume]:
        return [
            client.V1Volume(
                name="job-volume",
                host_path=client.V1HostPathVolumeSource(
                    path=os.path.join(config.JOB_MOUNT_HOST, self.name), type="Directory"
                ),
            ),
            client.V1Volume(
                name="nvidia-lib",
                # TODO: fix the nvidia path
                host_path=client.V1HostPathVolumeSource(path="/usr/lib/nvidia-384/"),
            ),
            client.V1Volume(
                name="nvidia-lib64",
                # TODO: fix the nvidia path
                host_path=client.V1HostPathVolumeSource(path="/usr/lib/x86_64-linux-gnu/"),
            ),
        ]

    def _create_job_obj(self) -> client.V1Job:
        # Configure Pod template container
        volumes = self.__create_volumes()
        container = self.__create_containers()

        # Create and configure a spec section
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"name": self.name, "type": self.type, "id": str(self.replica_id)}),
            spec=client.V1PodSpec(restart_policy="Never", containers=container, volumes=volumes),
        )

        # Create the specification of deployment
        spec = client.V1JobSpec(template=template, backoff_limit=4, ttl_seconds_after_finished=600)

        # Instantiate the job object
        job_obj = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=self.uname, namespace=config.k8s_params["namespace"]),
            spec=spec,
            status=client.V1JobStatus(),
        )

        return job_obj

    def to_yaml(self, output: str = "stdout") -> None:
        # This attribute map is to temporarily address the openapi bug in k8s. Should be fixed with next releases.
        correct_attribute_map = {
            "api_version": "apiVersion",
            "cluster_name": "clusterName",
            "creation_timestamp": "creationTimestamp",
            "deletion_grace_period_seconds": "deletionGracePeriodSeconds",
            "deletion_timestamp": "deletionTimestamp",
            "generate_name": "generateName",
            "managed_fields": "managedFields",
            "owner_references": "ownerReferences",
            "resource_version": "resourceVersion",
            "self_link": "selfLink",
            "active_deadline_seconds": "activeDeadlineSeconds",
            "backoff_limit": "backoffLimit",
            "manual_selector": "manualSelector",
            "ttl_seconds_after_finished": "ttlSecondsAfterFinished",
            "completion_time": "completionTime",
            "start_time": "startTime",
            "mount_path": "mountPath",
            "mount_propagation": "mountPropagation",
            "read_only": "readOnly",
            "sub_path": "subPath",
            "sub_path_expr": "subPathExpr",
            "value_from": "valueFrom",
            "env_from": "envFrom",
            "image_pull_policy": "imagePullPolicy",
            "liveness_probe": "livenessProbe",
            "readiness_probe": "readinessProbe",
            "security_context": "securityContext",
            "startup_probe": "startupProbe",
            "stdin_once": "stdinOnce",
            "termination_message_path": "terminationMessagePath",
            "termination_message_policy": "terminationMessagePolicy",
            "volume_devices": "volumeDevices",
            "volume_mounts": "volumeMounts",
            "working_dir": "workingDir",
            "aws_elastic_block_store": "awsElasticBlockStore",
            "azure_disk": "azureDisk",
            "azure_file": "azureFile",
            "config_map": "configMap",
            "downward_api": "downwardAPI",
            "empty_dir": "emptyDir",
            "flex_volume": "flexVolume",
            "gce_persistent_disk": "gcePersistentDisk",
            "git_repo": "gitRepo",
            "host_path": "hostPath",
            "persistent_volume_claim": "persistentVolumeClaim",
            "photon_persistent_disk": "photonPersistentDisk",
            "portworx_volume": "portworxVolume",
            "scale_io": "scaleIO",
            "vsphere_volume": "vsphereVolume",
            "affinity": "affinity",
            "automount_service_account_token": "automountServiceAccountToken",
            "dns_config": "dnsConfig",
            "dns_policy": "dnsPolicy",
            "enable_service_links": "enableServiceLinks",
            "ephemeral_containers": "ephemeralContainers",
            "host_aliases": "hostAliases",
            "host_ipc": "hostIPC",
            "host_network": "hostNetwork",
            "host_pid": "hostPID",
            "image_pull_secrets": "imagePullSecrets",
            "init_containers": "initContainers",
            "node_name": "nodeName",
            "node_selector": "nodeSelector",
            "preemption_policy": "preemptionPolicy",
            "priority_class_name": "priorityClassName",
            "readiness_gates": "readinessGates",
            "restart_policy": "restartPolicy",
            "runtime_class_name": "runtimeClassName",
            "scheduler_name": "schedulerName",
            "service_account": "serviceAccount",
            "service_account_name": "serviceAccountName",
            "share_process_namespace": "shareProcessNamespace",
            "termination_grace_period_seconds": "terminationGracePeriodSeconds",
            "topology_spread_constraints": "topologySpreadConstraints",
            "container_port": "containerPort",
            "host_ip": "hostIP",
            "host_port": "hostPort",
        }
        obj_dict = self.k8s_job_obj.to_dict()
        sanitized_obj_dict = utils.update_dict_keys(obj_dict, correct_attribute_map)
        yaml_content = yaml.dump(sanitized_obj_dict)
        if output == "stdout":
            print(yaml_content)
        else:
            with open(f"{self.name}.yaml", "w+") as fp:
                fp.write(yaml_content)
