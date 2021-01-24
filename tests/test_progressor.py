import pytest
import os
from progressor import Progressor
from dl_job import DLJob


@pytest.mark.asyncio
async def test_update_progress_ps_no_running_jobs():
    finished_jobs = await Progressor.update_progress()
    assert len(finished_jobs) == 0
    assert len(Progressor.ps_cpu_occupations) == 0
    assert len(Progressor.worker_cpu_occupations) == 0
    assert len(Progressor.running_jobs) == 0
    assert len(Progressor.num_running_tasks) == 0


def test_compute_cpu_occupations():
    Progressor.ps_cpu_occupations = []
    Progressor.worker_cpu_occupations = []

    job1 = DLJob.create_from_config_file(os.getcwd(), "job_repo/experiment-cifar10-resnext110.yaml")
    job2 = DLJob.create_from_config_file(os.getcwd(), "job_repo/experiment-cifar10-resnext110.yaml")
    cpu_usage_dict = dict()
    cpu_usage_dict[job1] = (3, 1)
    cpu_usage_dict[job2] = (2, 1)

    Progressor._compute_cpu_occupations(cpu_usage_dict)
    assert round(Progressor.ps_cpu_occupations[0], 2) == 0.83
    assert Progressor.worker_cpu_occupations[0] == 0.5


def test_get_average_cpu_usage():
    job = DLJob.create_from_config_file(os.getcwd(), "job_repo/experiment-cifar10-resnext110.yaml")
    ps_metrics = [{"cpu/usage_rate": 3900}, {"cpu/usage_rate": 5600}, {"cpu/usage_rate": 8900}]
    worker_metrics = [{"cpu/usage_rate": 2300}, {"cpu/usage_rate": 1000}, {"cpu/usage_rate": 0}]
    avg_ps_cpu, avg_worker_cpu = Progressor._get_average_cpu_usage(job, ps_metrics, worker_metrics)
    assert round(avg_ps_cpu, 2) == 6.13
    assert round(avg_worker_cpu, 2) == 1.10


def test_set_job_as_finished():
    job = DLJob.create_from_config_file(os.getcwd(), "job_repo/experiment-cifar10-resnext110.yaml")
    Progressor.add_to_running_jobs(job)
    finished_jobs = []
    Progressor._set_job_as_finished(finished_jobs, job)
    assert job.status == "completed"
    assert job.end_slot >= job.arrival_slot
    assert job.end_time > job.arrival_time
    assert len(Progressor.running_jobs) == 0
    assert len(finished_jobs) == 1
    assert finished_jobs[0] == job


def test_compute_job_progress():
    job = DLJob.create_from_config_file(os.getcwd(), "job_repo/experiment-cifar10-resnext110.yaml")
    job.epoch_size = 2
    progress_list = [(2, 4), (5, 4), (2, 5)]
    Progressor._compute_job_progress(job, progress_list)
    assert job.progress == 4.5
