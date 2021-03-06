# NAS Scheduler

Because the complexity of neural networks keeps increasing, the amount of time required to train and evaluate them has become a serious bottleneck. But there has been a second development lately: Cloud Computing. Distributed computing resources have never been cheaper and, when required, additional resources can be spun up at the click of a button. This is where our project comes into play. The NAS Scheduler combines these two advancements, Deep Learning and Cloud Computing, to accelerate the training process and manage the training of several models at a time. We are developing a scheduler for deep learning jobs to provide streamlined resource scaling using Docker and Kubernetes as well as integrating support for the distributed training library, Horovod. 

Our final architecture consists of a client for interacting with the system, a scheduler for managing resources, a progressor for retrieving status updates and a cluster of workers responsible for performing the actual training work and a command-line client through which a user may interact with the system. The scheduler enables us to efficiently train several deep learning jobs at once. Based on different scheduling algorithms, it decides which jobs will be executed at which point in time and how much resources they may use. The progressor monitors the worker cluster in real-time to inform the user as well as the scheduler about the progress on the running jobs and resources used in the cluster.

### Installing the dependencies

```
pip3 install -r requirements.txt
```

### Running the scheduler

First to start the scheduler, run the daemon with
```
python daemon.py
```
Then you may interact with the scheduler by submitting commands through the client, for example to run a specific job:
```
python submit job_repo/experiment-cifar10-resnext110.yaml
```
See client.py for a more detailed description of the available commands.

### Running the tests

You can run the unit tests via
```
python -m pytest tests --ignore=tests/integration_test.py
```
and the integration tests via
```
python -m pytest tests/integration_test.py
```

### Coding Conventions

To follow our coding conventions, run 
```
black --line-length 120 . 
```
to format your code. Also add type annotations and comments to every function you add to the code base.

### Troubleshooting

If you experience errors, this might be due to the fact that there are too many dangling pods and jobs on the cluster. Run
```
microk8s kubectl delete pods --all
microk8s kubectl delete jobs --all
```
to fix this. Another reason might be that there are too many dangling docker images, which drain the server's storage:
```
docker image prune
```