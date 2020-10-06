# k8s config params

# api token can be obtained by:
# 	$ kubectl get secrets -n kube-system 
# 	$ kubectl describe secret/default-token-2mpmc -n kube-system
k8s_params = {
	"api_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6IlRSTnRlRTh5ODlnY0FHSFVVZFRyVE1sb0szRHRpX1JzTFZDUEt2UVZTXzQifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJrdWJlLXN5c3RlbSIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VjcmV0Lm5hbWUiOiJkZWZhdWx0LXRva2VuLTJtcG1jIiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQubmFtZSI6ImRlZmF1bHQiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC51aWQiOiJiMjMwODdlMy1hZWQ1LTQ5NDAtOGZiYS1mZGZlYmRkMTZkODgiLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6a3ViZS1zeXN0ZW06ZGVmYXVsdCJ9.AiEre5WXaxPbnETsfEXajMHHyZ9e2-KfRtAuQKz02U95xIZpW2DE3BeL2Je3yZAY7iLnHeiEWEl7N4NRlGGSuNfce8RDglIkgj0a4I0NUheLWsd4rSsdB01OMdnb06k00aexVZ9jrzMHxNT6v1aMRjowMzWz2tiJwv-xBZJPsMqzAXsrUUJP-cgsckriDzLqcFbcUb2IqveISOnVpl7vchd21GU51UjbxU3_ETso9cKvbDkr1g4uWTBWKqvho7-EyYNjlPMFHreHKw7DoW1XbSvDltklxc1ZM9IflZ72Xm3X44yogu31YDCBJ0J-dViiQFvS_buVWAjukPxPAyWTGg",
	"host": "https://localhost:16443",
	"debug": False,
	"ssl_ca_cert": "ca.crt",
	"key_file": "ca.key",
	"namespace": "default"
}

LOGGER_NAME = "NAS_scheduler"
LOG_LEVEL = "DEBUG"

AMQP_URI = "amqp://guest:guest@localhost:5672//"

#Cluster detail
DEFAULT_NUM_PS = 1
DEFAULT_NUM_WORKER = 1

BW_PER_NODE = 10
MEM_PER_NODE = 48
CPU_PER_NODE = 8
GPU_PER_NODE = 1
NODE_LIST = ['127.0.0.1']


#Job details
TOT_NUM_JOBS = 3
T = 20
MIN_SLEEP_UNIT = 10**-3

TS_INTERVAL = 600

LOSS_LITTLE_CHANGE_EPOCH_NUM = 10
LOSS_CONVERGENCE = 0.05

JOB_SCHEDULER='optimus'

RANDOM_SEED = 9973