import time
import threading
import config

from communication import Handler, hub, Payload
from log import logger

class Statsor(Handler):
    def __init__(self, timer, scheduler, progressor, cluster):
        super().__init__(connection=hub.connection, entity='statsor')
        self.module_name = 'statsor'
        self.timer = timer
        self.scheduler = scheduler
        self.progressor = progressor
        self.cluster = cluster

        self.tic = time.time()
        self.end = None

        self.stats_txt = "exp-stats.txt"
        open(self.stats_txt, 'w').close()
        self.start()

    def process(self, msg):
        logger.debug(f'[{self.module_name}] received msg: {str(msg)}')

        if msg.type == "control" and msg.source == "scheduler":
            # signal that the scheduler has finished its timeslot and we can start getting statistics
            self._stats(msg.timestamp)
        else:
            raise RuntimeError

    def _stats(self, t):
        logger.info(f'[{self.module_name}] time slot: {t}')
        num_submit_jobs = len(self.scheduler.uncompleted_jobs) + len(self.scheduler.completed_jobs)
        num_completed_jobs = len(self.scheduler.completed_jobs)
        num_uncompleted_jobs = len(self.scheduler.uncompleted_jobs)
        logger.info(f'[{self.module_name}] submitted jobs: {num_submit_jobs}, completed jobs: {num_completed_jobs}, \
                        uncompleted_jobs: {num_uncompleted_jobs}')

        cluster_cpu_util = float('%.3f' % (1.0 * self.cluster.used_cpu / self.cluster.num_cpu))
        cluster_mem_util = float('%.3f' % (1.0 * self.cluster.used_mem / self.cluster.num_mem))
        cluster_bw_util = float('%.3f' % (1.0 * self.cluster.used_bw / self.cluster.num_bw))
        cluster_gpu_util = float('%.3f' % (1.0 * self.cluster.used_gpu / self.cluster.num_gpu))

        logger.info(f'[{self.module_name}] CPU utilization: {(100.0 * cluster_cpu_util):.3f}%, \
                         MEM utilization: {(100.0 * cluster_mem_util):.3f}%,\
                         "BW utilization: {(100.0 * cluster_bw_util):.3f}%,\
                         "GPU utilization: {(100.0 * cluster_gpu_util):.3f}%')

        # get total number of running tasks
        tot_num_running_tasks = self.progressor.num_running_tasks

        completion_time_list = []
        completion_slot_list = []
        for job in self.scheduler.completed_jobs:
            completion_time_list.append(job.end_time - job.arrival_time)
            completion_slot_list.append(job.end_slot - job.arrival_slot + 1)
        try:
            avg_completion_time = 1.0 * sum(completion_time_list) / len(completion_time_list)
            avg_completion_slot = sum(completion_slot_list) / len(completion_slot_list)
        except:
            logger.debug(f'[{self.module_name}] No jobs are finished!!!')
        else:
            logger.debug(f'[{self.module_name}] average completion time (including speed measurement): {avg_completion_time:.3f} seconds, \
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
        if self.scheduler.name == "optimus_scheduler":
            stats_dict["scaling_overhead"] = self.scheduler.scaling_overhead
            stats_dict["testing_overhead"] = self.scheduler.testing_overhead
        if len(completion_time_list) > 0:
            stats_dict["avg_completion_time"] = float('%.3f' % (avg_completion_time))
        else:
            stats_dict["avg_completion_time"] = -1
        try:
            ps_cpu_usage = self.progressor.ps_cpu_occupations
            worker_cpu_usage = self.progressor.worker_cpu_occupations
            stats_dict["ps_cpu_usage"] = ps_cpu_usage
            stats_dict["worker_cpu_usage"] = worker_cpu_usage
        except Exception as e:
            logger.debug(f'[statsor] {e}')

        toc = time.time()
        runtime = toc - self.tic
        stats_dict["runtime"] = float('%.3f' % (runtime))
        if len(self.scheduler.completed_jobs) == config.TOT_NUM_JOBS:
            logger.info(f'[{self.module_name}] All jobs are completed!')
            if self.end is None:
                self.end = runtime
            stats_dict["makespan"] = float('%.3f' % (self.end))
        else:
            stats_dict["makespan"] = -1

        with open(self.stats_txt, 'a') as f:
            f.write(str(stats_dict) + "\n")

        msg = Payload(t, 'statsor', 'completion', None)
        hub.push(msg, 'scheduler')

