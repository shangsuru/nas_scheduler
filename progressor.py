import asyncio
import config
import time
from log import logger
from timer import Timer


class Progressor:
    running_jobs = set()
    num_running_tasks = []

    @staticmethod
    def add_to_running_jobs(job):
        Progressor.running_jobs.add(job)

    @staticmethod
    def remove_from_running_jobs(job):
        Progressor.running_jobs.discard(job)

    @staticmethod
    async def update_progress():
        freq = 8  # frequency of job progress and training speed updates within a timeslot interval
        finished_jobs = []

        # collect cpu occupations
        Progressor.ps_cpu_occupations = []
        Progressor.worker_cpu_occupations = []

        # collect # of running tasks
        Progressor.num_running_tasks = []

        counter = 0
        while counter < freq and len(Progressor.running_jobs) > 0:  # if no jobs in this timeslot, skip it
            for i in range(config.TS_INTERVAL // freq):
                await asyncio.sleep(1)
            counter += 1

            cpu_usage_dict = dict()  # job, cpu_usage in this interval
            num_tasks = 0

            for job in Progressor.running_jobs.copy():
                try:
                    progress_list, speed_list, ps_metrics, worker_metrics = await Progressor._get_job_metrics(
                        counter, job
                    )

                    if sum(speed_list) == 0 or not progress_list:
                        continue

                    logger.debug(f"got training progress. speed_list={speed_list}")
                except Exception as e:
                    logger.info(f"get training stats error! Exception:\n{str(e)}")
                    continue

                Progressor._compute_job_progress(job, progress_list)

                # update training speed dict
                logger.debug(
                    f"{(job.resources.ps.num_ps, job.resources.worker.num_worker)}: {sum(speed_list) / int(job.metadata.batch_size)} batches/second"
                )

                job.training_speeds[(job.resources.ps.num_ps, job.resources.worker.num_worker)] = sum(speed_list) / int(
                    job.metadata.batch_size
                )

                # get # of running tasks
                num_tasks += job.resources.ps.num_ps + job.resources.worker.num_worker

                avg_ps_cpu, avg_worker_cpu = Progressor._get_average_cpu_usage(job, ps_metrics, worker_metrics)
                cpu_usage_dict[job] = (avg_ps_cpu, avg_worker_cpu)

                Progressor._log_job_attributes(job, progress_list, speed_list)

                if job.progress >= job.num_epochs:
                    Progressor._set_job_as_finished(finished_jobs, job)
                else:
                    continue

            Progressor._compute_cpu_occupations(cpu_usage_dict)
            Progressor.num_running_tasks.append(num_tasks)

        # end of time slot
        return finished_jobs

    @staticmethod
    def _compute_cpu_occupations(cpu_usage_dict):
        """
        Computes the proportion of used to available cpus on parameter servers
        and workers for each job in the current timeslot.

        Args:
            cpu_usage_dict (dict): the job being the key, it contains a tuple of the cpu usage
                                   on the parameter server and the workers for that job
        """
        slot_avg_ps_cpu_list = []
        slot_avg_worker_cpu_list = []
        for job, (ps_cpu, worker_cpu) in cpu_usage_dict.items():
            if job.dist_strategy == "ps":
                norm_ps_cpu = ps_cpu / job.resources.ps.ps_cpu
                slot_avg_ps_cpu_list.append(norm_ps_cpu)
            norm_worker_cpu = worker_cpu / job.resources.worker.worker_cpu
            slot_avg_worker_cpu_list.append(norm_worker_cpu)
        if len(slot_avg_ps_cpu_list) > 0:
            ps_cpu_occupation = sum(slot_avg_ps_cpu_list) / len(slot_avg_ps_cpu_list)
            Progressor.ps_cpu_occupations.append(ps_cpu_occupation)
            logger.debug(f"Normalized ps cpu: {ps_cpu_occupation}")
            worker_cpu_occupation = sum(slot_avg_worker_cpu_list) / len(slot_avg_worker_cpu_list)
            Progressor.worker_cpu_occupations.append(worker_cpu_occupation)
            logger.debug(f"Normalized worker cpu: {worker_cpu_occupation}")

    @staticmethod
    def _get_average_cpu_usage(job, ps_metrics, worker_metrics):
        """
        Computes the average cpu usage for parameter servers and workers and also
        sets the difference between the smallest and biggest usage percentage for
        parameter servers and workers on the job object

        Args:
            job (DLJob): job representing a deep learning job running the cluster
            ps_metrics (dict): different metrics for the job on the parameter server
            worker_metrics (dict): different metrics for the job on the workers

        Returns:
            avg_ps_cpu (float): the average cpu usage for all parameter servers
            avg_worker_cpu (float): the average cpu usage for all workers
        """
        if job.dist_strategy == "ps":
            ps_cpu_usage_list = []
            for metrics in ps_metrics:
                ps_cpu_usage_list.append(metrics["cpu/usage_rate"] / 1000.0)
            job.ps_cpu_diff = max(ps_cpu_usage_list) - min(ps_cpu_usage_list)
            avg_ps_cpu = sum(ps_cpu_usage_list) / len(ps_cpu_usage_list)
        else:
            job.ps_cpu_diff = 0
            avg_ps_cpu = 0

        worker_cpu_usage_list = []
        for metrics in worker_metrics:
            worker_cpu_usage_list.append(metrics["cpu/usage_rate"] / 1000.0)
        job.worker_cpu_diff = max(worker_cpu_usage_list) - min(worker_cpu_usage_list)
        avg_worker_cpu = sum(worker_cpu_usage_list) / len(worker_cpu_usage_list)
        return avg_ps_cpu, avg_worker_cpu

    @staticmethod
    async def _get_job_metrics(counter, job):
        logger.debug(f"Trying to read progress/speed stats #{counter}")
        get_progress = asyncio.create_task(job.get_training_progress_stats())
        get_speed = asyncio.create_task(job.get_training_speed())
        get_metrics = asyncio.create_task(job.get_metrics())

        (progress_list, val_loss_list) = await get_progress
        speed_list = await get_speed
        (ps_metrics, worker_metrics) = await get_metrics

        return progress_list, speed_list, ps_metrics, worker_metrics

    @staticmethod
    def _log_job_attributes(job, progress_list, speed_list):
        """
        Logs all important information about the job together with given progress and speed lists
        """
        logger.info(
            f"job name: {job.name}, model name: {job.metadata.modelname} \
                    , dist_strategy: {job.dist_strategy}\
                    , kv_store: {job.envs.kv_store}\
                    , batch_size: {job.metadata.batch_size}\
                    , num_ps: {job.resources.ps.num_ps}, num_worker: {job.resources.worker.num_worker}\
                    , progress_list: {progress_list}\
                    , job total progress: {job.progress:.3f}\
                    , num_epochs: {job.num_epochs}\
                    , speed_list: {speed_list}, sum_speed (samples/second): {sum(speed_list)}\
                    , sum_speed(batches/second): {sum(speed_list) / int(job.metadata.batch_size)}\
                    , ps cpu usage diff: {job.ps_cpu_diff}\
                    , worker cpu usage diff: {job.worker_cpu_diff}"
        )

    @staticmethod
    def _set_job_as_finished(finished_jobs, job):
        """
        Marks a job as finished by setting its status end slot and end time attributes
        and moving it from running_jobs to the finished jobs list
        """
        job.status = "completed"
        job.end_slot = Timer.get_clock()
        job.end_time = time.time()
        logger.info(
            f"{job.name} has completed. \
                    # of time slots: {job.end_slot - job.arrival_slot} \
                    completion time: {job.end_time - job.arrival_time}"
        )
        Progressor.running_jobs.remove(job)
        finished_jobs.append(job)

    @staticmethod
    def _compute_job_progress(job, progress_list):
        """
        Sets the progress attribute for the given job

        Args:
            job (DLJob): job representing a deep learning job running the cluster
            progress_list (list):
        """
        if job.envs.kv_store == "dist_sync":
            if len(progress_list) > 0:
                epoch_list = []
                batch_list = []
                for i in range(len(progress_list)):
                    (epoch, batch) = progress_list[i]
                    if batch == -1:
                        batch = 0
                        epoch += 1
                    epoch_list.append(epoch)
                    batch_list.append(batch)
                max_batch_index = batch_list.index(max(batch_list))
                logger.debug(f"epoch_size: {job.epoch_size}")
                job.progress += epoch_list[max_batch_index] + batch_list[max_batch_index] / job.epoch_size
        else:
            sum_epochs = sum(epoch for epoch, _ in progress_list)
            sum_batches = sum(batch for _, batch in progress_list)
            job.progress += sum_epochs / len(progress_list) + sum_batches / len(progress_list) / job.epoch_size
