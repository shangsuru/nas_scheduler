import kubernetes
from kubernetes import client, config
from kubernetes.client import ApiClient

import utils
import config

from log import logger

class KubeAPI:
    def __init__(self):
        # simple way is to follow https://microk8s.io/docs/working-with-kubectl
        configuration = kubernetes.client.Configuration()
        configuration.host = config.k8s_params['host']
        configuration.verify_ssl=False
        # configuration.ssl_ca_cert = k8s_params['ssl_ca_cert']
        # configuration.key_file = k8s_params['key_file']
        configuration.debug = config.k8s_params['debug']
        configuration.api_key['authorization'] = config.k8s_params['api_token']
        configuration.api_key_prefix['authorization'] = 'Bearer'
        client.Configuration.set_default(configuration)
        self.kube_api_obj = client.CoreV1Api()
        self.batch_v1 = client.BatchV1Api()
    
    def clear_jobs(self):
        api_response = self.batch_v1.delete_collection_namespaced_job(namespace="default")
        logger.debug("Jobs deleted. status='%s'" % str(api_response.status))

    def get_pods(self, namespace='default', field_selector={}, label_selector={}):
        """
        E.g. microk8s kubectl get pods --selector=name=x,job=y --namespace=default
        """
        fields_str = utils.dict_to_str(field_selector)
        labels_str = utils.dict_to_str(label_selector)
        return self.kube_api_obj.list_namespaced_pod(namespace, field_selector=fields_str,
                                                                label_selector=labels_str).items
    def get_pods_attribute(self, attribute, **kwargs):
        pods = self.get_pods(**kwargs)
        return [utils.rgetattr(pod, attribute) for pod in pods]

    def get_services(self, namespace, field_selector={}, label_selector={}):
        """Get k8s services for a given namespace.
        E.g.
            microk8s kubectl get services --namespace=kube-system
        """
        fields_str = utils.dict_to_str(field_selector)
        labels_str = utils.dict_to_str(label_selector)
        return self.kube_api_obj.list_namespaced_service(namespace, field_selector=fields_str,
                                                                    label_selector=labels_str).items
    def submit_job(self, job):
        api_response = self.kube_api_obj.create_namespaced_job(
            body=job,
            namespace="default")
        logger("Job created. status='%s'" % str(api_response.status))

    def delete_job(self, name):
        api_response = self.kube_api_obj.delete_namespaced_job(
            name=name,
            namespace="default",
            body=client.V1DeleteOptions(
                propagation_policy='Foreground',
                grace_period_seconds=5))
        logger("Job deleted. status='%s'" % str(api_response.status))

