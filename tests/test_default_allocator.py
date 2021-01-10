import pytest
from cluster import Cluster
from tests.end_to_end import prepare_job_repo
from allocators.default_allocator import DefaultAllocator
from pathlib import Path
import yaml
import config
from dl_job import DLJob
import os


def setup_cluster_and_allocator():
    cluster = Cluster()
    allocator = DefaultAllocator(cluster)
    return cluster, allocator


def setup_config(cpu_per_node, mem_per_node, bw_per_node, gpu_per_node, num_nodes):
    config.NODE_LIST = [f'node{i}' for i in range(num_nodes)]
    config.CPU_PER_NODE = cpu_per_node
    config.MEM_PER_NODE = mem_per_node
    config.BW_PER_NODE = bw_per_node
    config.GPU_PER_NODE = gpu_per_node


def prepare_job_repo():
    job_repo = list()
    for filename in Path('../job_repo').glob('*.yaml'):
        with open(filename, 'r') as f:
            job_repo.append(yaml.full_load(f))
    return job_repo


def get_jobs_list(amount):
    """
    Fetches job configs from job repo, creates DLJobs and returns them as list.
    Args:
        amount: integer stating how many jobs should be generated
    Returns:
         jobs: list of DLJob
    """
    jobs = []
    job_repo = prepare_job_repo()
    for i in range(amount):
        job = DLJob(uid=i, tag=i, dir_prefix=os.getcwd(), conf=job_repo[i % len(job_repo)])
        jobs.append(job)
    return jobs


def test_one_job_per_node():
    """ In this testcase each of the 3 jobs perfectly fits on one node,
        leaving no more resources on the node. We will verify, that:
        1. each job allocates the correct amount of resources
        2. each job is allocated on the correct node (on the first node in the sorted_nodes_list)
        3. the blackbox function allocate from allocator_base is correct (use of allocate_job has to be correct)
    """

    jobs = get_jobs_list(amount=3)  # get 3 jobs from job repo
    setup_config(cpu_per_node=8, mem_per_node=8, bw_per_node=8, gpu_per_node=4, num_nodes=3)  # set the cluster config

    # set ps parameters and worker parameters such that one job completely fills one node
    for i in range(len(jobs)):
        jobs[i].resources.ps.num_ps = 2
        jobs[i].resources.ps.ps_cpu = 2
        jobs[i].resources.ps.ps_mem = 2
        jobs[i].resources.ps.ps_bw = 2
        jobs[i].resources.worker.num_worker = 2
        jobs[i].resources.worker.worker_cpu = 2
        jobs[i].resources.worker.worker_mem = 2
        jobs[i].resources.worker.worker_bw = 2
        jobs[i].resources.worker.worker_gpu = 2
        # sanity check: num_ps*ps_cpu+num_worker*worker_cpu == 8 == config.CPU_PER_NODE, same goes for mem, bw and gpu

    # instantiate cluster and DefaultAllocator
    cluster1, da1 = setup_cluster_and_allocator()

    # This loop checks the atomic correctness of the allocate_job function for each job.
    for i in range(len(jobs)):
        # resort the nodes, DefaultAllocator should use the node with the least used resources (index 0 of sorted_nodes)
        sorted_nodes = cluster1.sort_nodes('cpu')
        used_cpus, node_index = sorted_nodes[0]
        ps_nodes, worker_nodes = da1.allocate_job(jobs[i])

        assert ps_nodes == jobs[i].resources.ps.num_ps * [node_index]   # allocate_job should use the first node
        assert [cluster1.nodes[n_idx] for n_idx in ps_nodes] == jobs[i].resources.ps.num_ps * [
            config.NODE_LIST[node_index]]
        assert worker_nodes == jobs[i].resources.worker.num_worker * [node_index]
        assert [cluster1.nodes[n_idx] for n_idx in worker_nodes] == jobs[i].resources.worker.num_worker * [
            config.NODE_LIST[node_index]]

    # reset cluster and da
    cluster1, da1 = setup_cluster_and_allocator()
    cluster2, da2 = setup_cluster_and_allocator()

    # At this point we know that allocate_job works correct, so we can check if allocate also is correct
    ps_placements, worker_placements = da1.allocate(jobs)
    for i in range(len(jobs)):
        ps_nodes, worker_nodes = da2.allocate_job(jobs[i])  # use allocate_job as gold standard (already tested above)
        assert ps_placements[jobs[i].uid] == [config.NODE_LIST[node_index] for node_index in ps_nodes]
        assert worker_placements[jobs[i].uid] == [config.NODE_LIST[node_index] for node_index in worker_nodes]


def test_one_jobs_fills_all_nodes():
    """ In this testcase only the first job in the list of jobs should fit on the cluster.
        The other jobs should not be allocated. We will verify, that:
        1. the first allocates the correct amount of resources
        2. the individual workers and ps are allocated on the correct node (on the first node in the sorted_nodes_list)
        3. the blackbox function allocate from allocator_base is correct, dictionary should only contain one key.
    """
    jobs = get_jobs_list(amount=3)  # get 3 jobs from job repo

    setup_config(cpu_per_node=2, mem_per_node=2, bw_per_node=2, gpu_per_node=1, num_nodes=2)    # set the cluster config

    # reset cluster and da
    cluster1, da1 = setup_cluster_and_allocator()
    sorted_nodes = cluster1.sort_nodes('cpu')

    for i in range(len(jobs)):
        # set ps parameters
        jobs[i].resources.ps.num_ps = 2
        jobs[i].resources.ps.ps_cpu = 1
        jobs[i].resources.ps.ps_mem = 1
        jobs[i].resources.ps.ps_bw = 1
        # set worker parameters
        jobs[i].resources.worker.num_worker = 2
        jobs[i].resources.worker.worker_cpu = 1
        jobs[i].resources.worker.worker_mem = 1
        jobs[i].resources.worker.worker_bw = 1
        jobs[i].resources.worker.worker_gpu = 1

    # each job should fit on a single node
    ps_nodes, worker_nodes = da1.allocate_job(jobs[0])  # check that allocate_job method works correct

    # first ps and worker should be allocated together on first node
    assert ps_nodes[0] == sorted_nodes[0][1]
    assert worker_nodes[0] == sorted_nodes[0][1]
    # second ps and worker should be allocated together on second node
    assert ps_nodes[1] == sorted_nodes[1][1]
    assert worker_nodes[1] == sorted_nodes[1][1]

    assert [cluster1.nodes[node] for node in ps_nodes] == [config.NODE_LIST[sorted_nodes[0][1]],
                                                           config.NODE_LIST[sorted_nodes[1][1]]]
    assert [cluster1.nodes[node] for node in worker_nodes] == [config.NODE_LIST[sorted_nodes[0][1]],
                                                               config.NODE_LIST[sorted_nodes[1][1]]]

    # reset cluster and da
    cluster1, da1 = setup_cluster_and_allocator()
    cluster2, da2 = setup_cluster_and_allocator()

    # At this point we know that allocate_job works correct, so we can check if allocate also is correct
    ps_placements, worker_placements = da1.allocate(jobs)

    ps_nodes, worker_nodes = da2.allocate_job(jobs[0])
    assert ps_placements[jobs[0].uid] == [config.NODE_LIST[node_index] for node_index in ps_nodes]
    assert worker_placements[jobs[0].uid] == [config.NODE_LIST[node_index] for node_index in worker_nodes]

    # since all nodes are full, the job 2 and 3 should not be allocated and thus not in the dict
    for i in range(1, len(jobs)):
        assert jobs[i].uid not in ps_placements.keys()
        assert jobs[i].uid not in worker_placements.keys()