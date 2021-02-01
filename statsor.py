from __future__ import annotations
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from schedulers.scheduler_base import SchedulerBase

import config
import json
import redis
import time
from cluster import Cluster
from log import logger
from progressor import Progressor


class Statsor:
    """
    Gathers cluster metrics
    """

    tic = time.time()
    end: Optional[float] = None
    cluster: Cluster
    scheduler: SchedulerBase

    @staticmethod
    def set_cluster_and_scheduler(cluster: Cluster, scheduler: SchedulerBase) -> None:
        """
        This method is called by daemon.py to give Statsor
        references to the scheduler and cluster objects.

        Args:
            cluster: used by statsor to have access to the resource utilization metrics
            scheduler: used by statsor to have access to submitted and completted jobs
        """
        Statsor.cluster = cluster
        Statsor.scheduler = scheduler

    @staticmethod
    async def stats(t: int) -> None:
        logger.info(f"time slot: {t}")
        num_submit_jobs = len(Statsor.scheduler.uncompleted_jobs) + len(Statsor.scheduler.completed_jobs)
        num_completed_jobs = len(Statsor.scheduler.completed_jobs)
        num_uncompleted_jobs = len(Statsor.scheduler.uncompleted_jobs)
        logger.info(
            f"submitted jobs: {num_submit_jobs}, completed jobs: {num_completed_jobs}, \
                        uncompleted_jobs: {num_uncompleted_jobs}"
        )

        cluster_cpu_util = float("%.3f" % (1.0 * Statsor.cluster.used_cpu / Statsor.cluster.num_cpu))
        cluster_mem_util = float("%.3f" % (1.0 * Statsor.cluster.used_mem / Statsor.cluster.num_mem))
        cluster_bw_util = float("%.3f" % (1.0 * Statsor.cluster.used_bw / Statsor.cluster.num_bw))
        cluster_gpu_util = float("%.3f" % (1.0 * Statsor.cluster.used_gpu / Statsor.cluster.num_gpu))

        logger.info(
            f'CPU utilization: {(100.0 * cluster_cpu_util):.3f}%, \
                         MEM utilization: {(100.0 * cluster_mem_util):.3f}%,\
                         "BW utilization: {(100.0 * cluster_bw_util):.3f}%,\
                         "GPU utilization: {(100.0 * cluster_gpu_util):.3f}%'
        )

        # get total number of running tasks
        tot_num_running_tasks = Progressor.num_running_tasks

        completion_time_list = []
        completion_slot_list = []
        for job in Statsor.scheduler.completed_jobs:
            completion_time_list.append(job.end_time - job.arrival_time)
            completion_slot_list.append(job.end_slot - job.arrival_slot + 1)
        try:
            avg_completion_time = 1.0 * sum(completion_time_list) / len(completion_time_list)
            avg_completion_slot = sum(completion_slot_list) / len(completion_slot_list)
        except:
            logger.debug(f"No jobs are finished!!!")
        else:
            logger.debug(
                f"average completion time (including speed measurement): {avg_completion_time:.3f} seconds, \
                average completion slots: {avg_completion_slot}"
            )

        redis_connection = redis.Redis(config.REDIS_HOST_DAEMON_CLIENT, config.REDIS_PORT_DAEMON_CLIENT)

        redis_connection.set("JOB_SCHEDULER", config.JOB_SCHEDULER)
        redis_connection.set("timeslot", t)
        redis_connection.set("num_submit_jobs", num_submit_jobs)
        redis_connection.set("num_completed_jobs", num_completed_jobs)
        redis_connection.set("num_uncompleted_jobs", num_uncompleted_jobs)
        redis_connection.set("cluster_cpu_util", cluster_cpu_util)
        redis_connection.set("cluster_mem_util", cluster_mem_util)
        redis_connection.set("cluster_bw_util", cluster_bw_util)
        redis_connection.set("cluster_gpu_util", cluster_gpu_util)
        for num_running_task in tot_num_running_tasks:
            redis_connection.rpush("tot_num_running_tasks", num_running_task)

        if Statsor.scheduler.name == "optimus_scheduler":
            redis_connection.set("scaling_overhead", Statsor.scheduler.scaling_overhead)
            redis_connection.set("testing_overhead", Statsor.scheduler.testing_overhead)
        if len(completion_time_list) > 0:
            redis_connection.set("avg_completion_time", float("%.3f" % (avg_completion_time)))
        else:
            redis_connection.set("avg_completion_time", -1)
        try:
            ps_cpu_usage = Progressor.ps_cpu_occupations
            worker_cpu_usage = Progressor.worker_cpu_occupations
            redis_connection.set("ps_cpu_usage", json.dumps(ps_cpu_usage))
            redis_connection.set("worker_cpu_usage", json.dumps(worker_cpu_usage))
        except Exception as e:
            logger.debug(f"[statsor] {e}")

        toc = time.time()
        runtime = toc - Statsor.tic
        redis_connection.set("runtime", float("%.3f" % (runtime)))
        if len(Statsor.scheduler.completed_jobs) == config.TOT_NUM_JOBS:
            logger.info(f"All jobs are completed!")
        if Statsor.end is None:
            Statsor.end = runtime
            redis_connection.set("makespan", float("%.3f" % (Statsor.end)))
        else:
            redis_connection.set("makespan", -1)
