import time
import sys
import threading
from queue import PriorityQueue
import math
import numpy as np
from scipy.optimize import curve_fit
import random

import config
from communication import hub, Payload
from schedulers.scheduler_base import SchedulerBase
from allocators.default_allocator import DefaultAllocator

from log import logger


class OptimusEstimator():
    def __init__(self, cluster):
        self.module_name = 'optimus_estimator'
        self.cluster = cluster
        self.exit_event = threading.Event()
        self.existing_jobs = []

    def set_exit_flag(self):
        self.exit_event.set()

    def _test_placement(self, jobs):
        """Test training speed to get points for curve fitting

        Args:
            jobs (list(DLJob)): list of jobs
        """
        cur_node_index = 0
        # job_id:placement_list
        placements = dict()

        node_used_cpu_list = [0 for i in range(self.cluster.num_nodes)]
        node_used_mem_list = [0 for i in range(self.cluster.num_nodes)]
        node_used_gpu_list = [0 for i in range(self.cluster.num_nodes)]
        node_used_bw_list = [0 for i in range(self.cluster.num_nodes)]

        for job in jobs:
            placements[job.uid] = []
            for i in range(job.resources.ps.num_ps):  # place each bundle
                # random placement
                cpu_req = job.resources.worker.worker_cpu + job.resources.ps.ps_cpu
                mem_req = job.resources.worker.worker_mem + job.resources.ps.ps_mem
                bw_req = job.resources.worker.worker_bw + job.resources.ps.ps_bw
                gpu_req = job.resources.worker.worker_gpu

                # check whether resources are sufficient
                for j in range(self.cluster.num_nodes):
                    node_index = (cur_node_index + j) % self.cluster.num_nodes
                    suff_resr = True
                    if node_used_cpu_list[node_index] + cpu_req > config.CPU_PER_NODE or \
                            node_used_mem_list[node_index] + mem_req > config.MEM_PER_NODE or \
                            node_used_bw_list[node_index] + bw_req > config.BW_PER_NODE or \
                            node_used_gpu_list[node_index] + gpu_req > config.GPU_PER_NODE:
                        suff_resr = False
                        continue

                    if suff_resr:
                        # node node_index has enough resources
                        break

                if suff_resr:
                    node_used_cpu_list[node_index] += cpu_req
                    node_used_mem_list[node_index] += mem_req
                    node_used_bw_list[node_index] += bw_req
                    node_used_gpu_list[node_index] += gpu_req

                    # placement
                    if job.uid in placements:
                        placements[job.uid].append(self.cluster.nodes[node_index])
                    else:
                        placements[job.uid] = [self.cluster.nodes[node_index]]
                else:
                    break

        return placements

    def _run(self, job, placement):
        # set placement
        logger.debug(f'[{self.module_name}] {job.name}, num_ps: {job.resources.ps.num_ps}, num_worker: \
                        {job.resources.worker.num_worker}, placement: {placement}')
        job.resources.ps.num_ps = len(placement)
        job.resources.worker.worker = job.resources.ps.num_ps
        job.set_ps_placement(placement)
        job.set_worker_placement(placement)
        # start job
        job.start()

    def test_speed(self, new_jobs):
        logger.debug(f'[{self.module_name}] start testing training speed for {len(new_jobs)} jobs...')

        tic = time.time()

        # little improvement: if two jobs are of the same type, they can reuse the training speed points
        for existing_job in self.existing_jobs:
            for new_job in new_jobs:
                if existing_job.workload_id == new_job.workload_id:  # same type of job
                    for key, value in existing_job.training_speeds.items():
                        if key in new_job.training_speeds:  # simply average
                            new_job.training_speeds[key] = (new_job.training_speeds[key] + value) / 2
                        else:
                            new_job.training_speeds[key] = value
        self.existing_jobs += new_jobs
        counter = 0
        while True:
            q = PriorityQueue()
            # less test speed, higher priority
            collected = True
            for job in new_jobs:
                if len(job.training_speeds) < self.cluster.num_nodes:  # collect [1, num_nodes) points
                    collected = False
                    break

            if collected:
                # all job has collected 5 points
                logger.info(f'[{self.module_name}] No need to test speed, all jobs are known workload.')
                break
            else:
                # at least one job does not have 5 speed points
                for job in new_jobs:
                    job.name = f'{job.name}-optimus-estimator-t{counter}'
                    q.put((len(job.training_speeds), job))

            sorted_jobs = []
            while not q.empty():
                num_speed_points, job = q.get()
                if num_speed_points >= 10:
                    continue
                # determine number of ps and number of worker
                while True:
                    job.resources.worker.num_worker = random.randint(1, self.cluster.num_nodes)
                    job.resources.ps.num_ps = job.resources.worker.num_worker
                    if (job.resources.ps.num_ps,
                        job.resources.worker.num_worker) in job.training_speeds:  # avoid repetition
                        # will cause infinite loop if job already has 10 points
                        continue
                    else:
                        sorted_jobs.append(job)
                        break

            counter += 1
            logger.debug(f'[{self.module_name}] No.{counter} time, collecting speed points...')

            placements = self._test_placement(sorted_jobs)
            running_jobs = []
            threads = []
            for job in sorted_jobs:
                placement = placements[job.uid]
                if len(placement) == job.resources.ps.num_ps:
                    running_jobs.append(job)
                    thread = threading.Thread(target=self._run, args=(job, placement,))
                    thread.start()
                    threads.append(thread)
                    # multiple concurrent job starting may cause congestion
                    time.sleep(3)
                else:
                    logger.debug(f'[{self.module_name}] {job.name} does not get resources to test speed')

            for thread in threads:
                thread.join()

            # sleep 1.5 minute to get training speed (better 5 mins, but may cost much time)
            if len(running_jobs) > 0:
                if self.exit_event.wait(90):
                    sys.exit()

            # read training speed, if no, sleep more
            for job in running_jobs:
                flag = True
                while flag:
                    speed_list = job.get_training_speed()
                    if min(speed_list) > 0:
                        job.training_speeds[(job.resources.ps.num_ps, job.resources.worker.num_worker)] = sum(
                            speed_list) / int(job.metadata.batch_size)  # batches/second
                        job.delete(True)
                        flag = False
                    else:
                        logger.debug(f'[{self.module_name}] did not get speed from job {job.name}, {speed_list}, sleep and try again later.')
                        if self.exit_event.wait(10):
                            sys.exit()

            for job in new_jobs:
                logger.debug(f'[{self.module_name}] {job.name}: {job.training_speeds}')

        toc = time.time()
        logger.info(f'[{self.module_name}] time cost of collecting speed points: {(toc - tic):.3f} seconds')
        # clear modifications
        for job in new_jobs:
            job.name = job.name.split('-optimus-estimator')[0]
            job.resources.ps.num_ps = 0
            job.set_ps_placement([])
            job.resources.worker.num_worker = 0
            job.set_worker_placement([])
        return

    '''
    --------------Below is completion epoch estimation----------------
    '''

    def __loss_fit_func(self, x, a, b, c):
        return 1 / (a * (x) + b) + c

    def _loss_curve_fitting(self, epochs_arr, losses_arr):
        param_bounds = ([0, 0, 0], [np.inf, np.inf, np.inf])

        # assign different weights to points, default sigma is ones
        sigma = np.ones(len(epochs_arr))
        NUM_SEGMENTS = 3
        for i in range(len(epochs_arr)):
            exp = int(math.floor(i / (math.ceil(1.0 * len(epochs_arr) / NUM_SEGMENTS))))
            sigma[i] /= 4 ** exp

        params = curve_fit(self.__loss_fit_func, epochs_arr, losses_arr, sigma=np.array(sigma), absolute_sigma=False,
                           bounds=param_bounds)
        return params[0]

    def est_epoch(self, job):
        if job.num_epochs < sys.maxsize:
            return job.num_epochs

        existing_epochs = []
        for existing_job in self.existing_jobs:
            if existing_job.workload_id == job.workload_id:  # same type of job
                if existing_job.num_epochs < sys.maxsize:
                    existing_epochs.append(existing_job.num_epochs)

        if len(existing_epochs) > 0:
            # training epoch is already specified
            return int(sum(existing_epochs) / len(existing_epochs))
        else:
            # we need to estimate the number of required epochs
            if len(job.val_losses) >= 3:
                epoch_list = []
                loss_list = []
                for epoch, loss in job.val_losses.items():
                    epoch_list.append(epoch)
                    loss_list.append(loss)

                # we do not need curve fitting each time, can be further optimized in future
                # also, we can get loss data from previous jobs, optimized in future
                try:
                    [a, b, c] = self._loss_curve_fitting(np.array(epoch_list), np.array(
                        loss_list))  # could throw exception since the loss may not descend at the beginning
                except Exception as e:
                    logger.error(f'[{self.module_name}] loss curve fitting error: {e}')
                    return -1
                # if loss does not change a lot for a certain period, converge.
                epoch = max(0, int(job.progress) - config.LOSS_LITTLE_CHANGE_EPOCH_NUM)
                fitted_losses = []
                while True:
                    fitted_losses.append(self.__loss_fit_func(epoch, a, b, c))
                    flag = True
                    if len(fitted_losses) >= config.LOSS_LITTLE_CHANGE_EPOCH_NUM:
                        for i in reversed(range(config.LOSS_LITTLE_CHANGE_EPOCH_NUM)):
                            if fitted_losses[epoch - i] - fitted_losses[epoch] > config.LOSS_CONVERGENCE:
                                flag = False
                                break
                    if not flag:
                        epoch += 1
                        if epoch > 100:  # each job must have at most 100 epochs
                            return -1
                    else:
                        return epoch
            else:
                return -1

    '''
    --------------Below is speed estimation------------
    '''

    def __async_speed_fit_func(X, a, b, c, d):
        p, w = X
        return w / (a + b * w / p + c * w + d * p)

    # async curve fitting to get a,b,c
    def _async_speed_curve_fitting(self, ps_arr, worker_arr, speed_arr):
        param_bounds = ([0, 0, 0, 0], [np.inf, np.inf, np.inf, np.inf])
        sigma = np.ones(len(ps_arr))
        try:
            params = curve_fit(self.__async_speed_fit_func, (ps_arr, worker_arr), speed_arr, sigma=np.array(sigma),
                               absolute_sigma=False, bounds=param_bounds)
            return params[0]
        except Exception as e:
            logger.error(str(e))

    def __sync_speed_fit_func(self, X, a, b, c, d, e):
        p, w, batch_size = X
        return 1 / (a * batch_size / w + b + c * w / p + d * w + e * p)

    # curve fitting to get a,b,c
    def _sync_speed_curve_fitting(self, ps_arr, worker_arr, batch_arr, speed_arr):
        param_bounds = ([0, 0, 0, 0, 0], [np.inf, np.inf, np.inf, np.inf, np.inf])
        sigma = np.ones(len(ps_arr))
        try:
            params = curve_fit(self.__sync_speed_fit_func, (ps_arr, worker_arr, batch_arr), speed_arr,
                               sigma=np.array(sigma), absolute_sigma=False, bounds=param_bounds)
            return params[0]
        except Exception as e:
            logger.error(f'[{self.module_name}] curve fitting error, {str(e)}')

    def est_speed(self, job, num_ps, num_worker):
        """Give the number of ps and the number of worker, predict the training speed.
        Use the real one if already exists in the dict
        """
        if (num_ps, num_worker) in job.training_speeds:
            return job.training_speeds[(num_ps, num_worker)]
        else:
            # do training speed curve fitting here
            ps_list = []
            worker_list = []
            speed_list = []
            if 'async' in job.envs.kv_store:
                if len(job.training_speeds) >= 4:
                    # do not need curve fitting each time, can be further optimized. future work
                    for key, value in job.training_speeds.items():
                        (ps, worker) = key
                        ps_list.append(float(ps))
                        worker_list.append(float(worker))
                        speed_list.append(value)
                    params = self._async_speed_curve_fitting(np.array(ps_list), np.array(worker_list),
                                                             np.array(speed_list))
                    if params is None:
                        logger.error(f'[{self.module_name}] {job.name} {str((num_ps, num_worker))} speed estimation error')
                        return -1
                    else:
                        [a, b, c, d] = params
                        est_speed = self.__async_speed_fit_func((num_ps, num_worker), a, b, c, d)
                        return est_speed
                else:
                    return -1
            elif 'sync' in job.envs.kv_store:
                if len(job.training_speeds) >= 5:
                    for key, value in job.training_speeds.items():
                        (ps, worker) = key
                        ps_list.append(float(ps))
                        worker_list.append(float(worker))
                        speed_list.append(value)
                    batch_size_list = [float(job.metadata.batch_size) for i in range(len(ps_list))]
                    params = self._sync_speed_curve_fitting(np.array(ps_list), np.array(worker_list),
                                                            np.array(batch_size_list), np.array(speed_list))
                    if params is None:
                        logger.error(f'[{self.module_name}] {job.name} {(num_ps, num_worker)} speed estimation error')
                        return -1
                    else:
                        [a, b, c, d, e] = params
                        est_speed = self.__sync_speed_fit_func((num_ps, num_worker, float(job.metadata.batch_size)), a, b, c,
                                                               d, e)
                        return est_speed
                else:
                    return -1


class OptimusScheduler(SchedulerBase):
    def __init__(self, cluster, timer):
        """
        Args:
            timer (Timer): timer instance
        """
        super().__init__(cluster, timer)
        self.module_name = 'optimus_scheduler'
        self.estimator = OptimusEstimator(cluster)
        self.allocator = DefaultAllocator(cluster)
        self.start()

    def __del__(self):
        super().__del__()
        self.estimator.set_exit_flag()

    def __update_util_queue(self, job, util_queue):
        """compute utility
        allocate 1 ps or 1 worker each time.
        sometimes can allocate multiple ps or worker for optimization, to avoid stuck in local optimal.

        Args:
            job (DLJob): job instance
            util_queue (PriorityQueue): a queue based on job utility
        """

        end_epoch = self.estimator.est_epoch(job)
        if end_epoch <= 0:
            # error when estimating epoch
            end_epoch = job.progress + 20

        rem_epoch = end_epoch - job.progress  # the rem_epoch is negative if estimated epoch return -1
        est_speed = self.estimator.est_speed(job, job.resources.ps.num_ps, job.resources.worker.num_worker)
        logger.debug(f'[{self.module_name}] estimated speed: {est_speed}')

        if est_speed <= 0:
            if job not in self.not_ready_jobs:
                self.not_ready_jobs.append(job)
            return
        rem_time = rem_epoch / est_speed

        est_speed = self.estimator.est_speed(job, job.resources.ps.num_ps + 1, job.resources.worker.num_worker)
        if est_speed <= 0:
            if job not in self.not_ready_jobs:
                self.not_ready_jobs.append(job)
            return
        ps_rem_time = rem_epoch / est_speed
        resource_reqs = (job.resources.ps.ps_cpu, job.resources.ps.ps_mem, job.resources.ps.ps_bw)
        shares = (
        1.0 * job.resources.ps.ps_cpu / self.cluster.num_cpu, 1.0 * job.resources.ps.ps_mem / self.cluster.num_mem,
        1.0 * job.resources.ps.ps_bw / self.cluster.num_bw)
        dom_res = shares.index(max(shares))
        ps_util = (rem_time - ps_rem_time) / resource_reqs[dom_res]

        # if add worker 1
        est_speed = self.estimator.est_speed(job, job.resources.ps.num_ps, job.resources.worker.num_worker + 1)
        if est_speed <= 0:
            if job not in self.not_ready_jobs:
                self.not_ready_jobs.append(job)
            return
        worker_rem_time = rem_epoch / est_speed
        resource_reqs = (
        job.resources.worker.worker_cpu, job.resources.worker.worker_mem, job.resources.worker.worker_bw,
        job.resources.worker.worker_gpu)
        shares = (1.0 * job.resources.worker.worker_cpu / self.cluster.num_cpu,
                  1.0 * job.resources.worker.worker_mem / self.cluster.num_mem,
                  1.0 * job.resources.worker.worker_bw / self.cluster.num_bw,
                  1.0 * job.resources.worker.worker_gpu / self.cluster.num_gpu)
        dom_res = shares.index(max(shares))

        worker_util = (rem_time - worker_rem_time) / resource_reqs[dom_res]
        if ps_util >= worker_util:
            # negative util since we prioritize max util
            util_queue.put((-ps_util, job.arrival_time, job, "ps"))
        else:
            util_queue.put((-worker_util, job.arrival_time, job, "worker"))

    def _schedule(self):
        # first collect speed data points
        new_jobs = []
        while not self.queueing_jobs.empty():
            (arrival_time, job) = self.queueing_jobs.get()
            new_jobs.append(job)

        logger.debug(f'[{self.module_name}] newly arrived jobs: {new_jobs}')

        # first estimate speed
        test_tic = time.time()
        self.estimator.existing_jobs = self.uncompleted_jobs + self.completed_jobs
        self.estimator.test_speed(new_jobs)
        logger.debug(f'[{self.module_name}] Finish testing speed for new jobs.')
        test_toc = time.time()
        self.testing_overhead += (test_toc - test_tic)

        # UTIL
        tic = time.time()

        # a queue based on job utility
        util_queue = PriorityQueue()

        self.cluster.used_cpu = 0
        self.cluster.used_mem = 0
        self.cluster.used_bw = 0
        self.cluster.used_gpu = 0

        # allocate each job a worker and a server to avoid starvation
        for job in self.uncompleted_jobs:
            cpu_req = job.resources.worker.worker_cpu + job.resources.ps.ps_cpu
            mem_req = job.resources.worker.worker_mem + job.resources.ps.ps_mem
            bw_req = job.resources.worker.worker_bw + job.resources.ps.ps_bw
            gpu_req = job.resources.worker.worker_gpu

            suff_resr = self.cluster.check_cluster_resource_full(cpu_req, mem_req, bw_req, gpu_req)
            if suff_resr:
                job.resources.worker.num_worker = 1
                job.resources.ps.num_ps = 1
                self.cluster.used_cpu += cpu_req
                self.cluster.used_mem += mem_req
                self.cluster.used_bw += bw_req
                self.cluster.used_gpu += gpu_req
                # compute initial utility
                self.__update_util_queue(job, util_queue)
            else:
                continue

        # allocate resources based on job utility
        while not util_queue.empty():
            (util, job_arrival, job, task_type) = util_queue.get()

            # increasing resource leads to slower speed
            # also, newly arrived jobs have negative utility, how to handle this
            if util > 0:
                # must be negative
                break
            if task_type == "ps":
                cpu_req = job.resources.ps.ps_cpu
                mem_req = job.resources.ps.ps_mem
                bw_req = job.resources.ps.ps_bw
                gpu_req = 0
            elif task_type == "worker":
                cpu_req = job.resources.worker.worker_cpu
                mem_req = job.resources.worker.worker_mem
                bw_req = job.resources.worker.worker_bw
                gpu_req = job.resources.worker.worker_gpu

            # check whether resources are sufficient
            suff_resr = self.cluster.check_cluster_resource_full(cpu_req, mem_req, bw_req, gpu_req)
            if suff_resr:
                # currently no mechanism to reduce resources
                if task_type == "ps":
                    job.resources.ps.num_ps += 1
                elif task_type == "worker":
                    job.resources.worker.num_worker += 1
                self.cluster.used_cpu += cpu_req
                self.cluster.used_mem += mem_req
                self.cluster.used_bw += bw_req
                self.cluster.used_gpu += gpu_req

                self.__update_util_queue(job, util_queue)
            else:
                # no enough resource
                break

        # TODO: how to handle not_ready_jobs
        logger.debug(f'[{self.module_name}] not ready jobs: {self.not_ready_jobs}')

        # check the scheduling result
        for job in self.uncompleted_jobs:
            logger.debug(f'[{self.module_name}] scheduling results | num_ps:{job.resources.ps.num_ps}, \
            num_worker:{job.resources.worker.num_worker}')

        # how to handle remaining resources? Due to error sometimes allocating resource can still increase speed

        toc = time.time()
        logger.debug(f'[{self.module_name}] scheduling time: {(toc - tic):.3f} seconds.')
