import aiofiles
import config
from log import logger
import time
from progressor import Progressor


class Statsor():
    """
    Gathers cluster metrics
    """
    tic = time.time()
    end = None
    open("exp-stats.txt", 'w').close()

    stats_txt = "exp-stats.txt"

     @staticmethod
    async def stats(t):
        logger.info(f'time slot: {t}')
        num_submit_jobs = len(Statsor.scheduler.uncompleted_jobs) + len(Statsor.scheduler.completed_jobs)
        num_completed_jobs = len(Statsor.scheduler.completed_jobs)
        num_uncompleted_jobs = len(Statsor.scheduler.uncompleted_jobs)
        logger.info(f'submitted jobs: {num_submit_jobs}, completed jobs: {num_completed_jobs}, \
                        uncompleted_jobs: {num_uncompleted_jobs}')

        cluster_cpu_util = float('%.3f' % (1.0 * Statsor.cluster.used_cpu / Statsor.cluster.num_cpu))
        cluster_mem_util = float('%.3f' % (1.0 * Statsor.cluster.used_mem / Statsor.cluster.num_mem))
        cluster_bw_util = float('%.3f' % (1.0 * Statsor.cluster.used_bw / Statsor.cluster.num_bw))
        cluster_gpu_util = float('%.3f' % (1.0 * Statsor.cluster.used_gpu / Statsor.cluster.num_gpu))

        logger.info(f'CPU utilization: {(100.0 * cluster_cpu_util):.3f}%, \
                         MEM utilization: {(100.0 * cluster_mem_util):.3f}%,\
                         "BW utilization: {(100.0 * cluster_bw_util):.3f}%,\
                         "GPU utilization: {(100.0 * cluster_gpu_util):.3f}%')

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
            logger.debug(f'No jobs are finished!!!')
        else:
            logger.debug(f'average completion time (including speed measurement): {avg_completion_time:.3f} seconds, \
                average completion slots: {avg_completion_slot}')

        stats_dict = dict()
        stats_dict["JOB_SCHEDULER"] = config.JOB_SCHEDULER
        stats_dict["timeslot"] = t
        stats_dict["num_submit_jobs"] = num_submit_jobs
        stats_dict["num_completed_jobs"] = num_completed_jobs
        stats_dict["num_uncompleted_jobs"] = num_uncompleted_jobs
        stats_dict["cluster_cpu_util"] = cluster_cpu_util
        stats_dict["cluster_mem_util"] = cluster_mem_util
        stats_dict["cluster_bw_util"] = cluster_bw_util
        stats_dict["cluster_gpu_util"] = cluster_gpu_util
        stats_dict["tot_num_running_tasks"] = tot_num_running_tasks
        if Statsor.scheduler.name == "optimus_scheduler":
            stats_dict["scaling_overhead"] = Statsor.scheduler.scaling_overhead
            stats_dict["testing_overhead"] = Statsor.scheduler.testing_overhead
        if len(completion_time_list) > 0:
            stats_dict["avg_completion_time"] = float('%.3f' % (avg_completion_time))
        else:
            stats_dict["avg_completion_time"] = -1
        try:
            ps_cpu_usage = Progressor.ps_cpu_occupations
            worker_cpu_usage = Progressor.worker_cpu_occupations
            stats_dict["ps_cpu_usage"] = ps_cpu_usage
            stats_dict["worker_cpu_usage"] = worker_cpu_usage
        except Exception as e:
            logger.debug(f'[statsor] {e}')

        toc = time.time()
        runtime = toc - Statsor.tic
        stats_dict["runtime"] = float('%.3f' % (runtime))
        if len(Statsor.scheduler.completed_jobs) == config.TOT_NUM_JOBS:
            logger.info(f'All jobs are completed!')
        if Statsor.end is None:
                Statsor.end = runtime
            stats_dict["makespan"] = float('%.3f' % (Statsor.end))
        else:
            stats_dict["makespan"] = -1

        async with aiofiles.open(Statsor.stats_txt, 'a') as f:
            await f.write(str(stats_dict) + "\n")
