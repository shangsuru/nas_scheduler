from __future__ import annotations
from typing import Any
from config import k8s_params
import utils

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dl_job import DLJob
from kubernetes import client, config
from kubernetes.stream import stream
from log import logger
from typing import Any


class KubeAPI:
    def __init__(self) -> None:
        # simple way is to follow https://microk8s.io/docs/working-with-kubectl
        configuration = client.Configuration()
        configuration.host = k8s_params["host"]
        configuration.verify_ssl = False
        # configuration.ssl_ca_cert = k8s_params['ssl_ca_cert']
        # configuration.key_file = k8s_params['key_file']
        configuration.debug = k8s_params["debug"]
        configuration.api_key["authorization"] = k8s_params["api_token"]
        configuration.api_key_prefix["authorization"] = "Bearer"
        client.Configuration.set_default(configuration)
        self.kube_api_obj = client.CoreV1Api()
        self.batch_v1 = client.BatchV1Api()

    def clear_jobs(self) -> None:
        api_response = self.batch_v1.delete_collection_namespaced_job(namespace=k8s_params["namespace"])
        logger.debug("Jobs deleted. status='%s'" % str(api_response.status))

    def get_pods(
        self, namespace: dict = k8s_params["namespace"], field_selector: dict = None, label_selector: dict = None
    ) -> any:
        """
        E.g. microk8s kubectl get pods --selector=name=x,job=y --namespace=default
        """
        if label_selector is None:
            label_selector = {}
        if field_selector is None:
            field_selector = {}

        fields_str = utils.dict_to_str(field_selector)
        labels_str = utils.dict_to_str(label_selector)
        return self.kube_api_obj.list_namespaced_pod(
            namespace, field_selector=fields_str, label_selector=labels_str
        ).items

    def get_pods_attribute(self, attribute: str, **kwargs) -> list:  # TODO
        pods = self.get_pods(**kwargs)
        return [utils.rgetattr(pod, attribute) for pod in pods]

    def kill_pod(self, name: str, namespace: dict = k8s_params["namespace"]) -> None:
        """Kills a k8s pod.

        Args:
            name (str): The name of the pod to kill.
            namespace (str): The k8s namespace
        """
        self.kube_api_obj.delete_namespaced_pod(name, namespace)

    def get_nodes(self) -> Any:
        """Queries k8s for all worker nodes.

        Returns:
            A list of all worker nodes connected to k8s.
        """
        return self.kube_api_obj.list_node().items

    def get_services(
        self, namespace: dict = k8s_params["namespace"], field_selector: dict = None, label_selector: dict = None
    ) -> Any:
        """Get k8s services for a given namespace.
        E.g.
            microk8s kubectl get services --namespace=kube-system
        """
        if label_selector is None:
            label_selector = {}
        if field_selector is None:
            field_selector = {}

        fields_str = utils.dict_to_str(field_selector)
        labels_str = utils.dict_to_str(label_selector)
        return self.kube_api_obj.list_namespaced_service(
            namespace, field_selector=fields_str, label_selector=labels_str
        ).items

    def submit_job(self, job: DLJob) -> None:
        api_response = self.batch_v1.create_namespaced_job(body=job, namespace=k8s_params["namespace"])
        logger.debug(f"Job {job.metadata.name} created.")

    def delete_job(self, name: str) -> None:
        api_response = self.batch_v1.delete_namespaced_job(
            name=name,
            namespace=k8s_params["namespace"],
            body=client.V1DeleteOptions(propagation_policy="Foreground", grace_period_seconds=5),
        )
        logger.debug(f"Job {name} deleted.")
