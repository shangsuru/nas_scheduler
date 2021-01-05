import asyncio
import config
from log import logger
import time
import timer


class Progressor():
    running_jobs = set()
    num_running_tasks = []

    @staticmethod
    def add_to_running_jobs(job):
        Progressor.running_jobs.add(job)

    @staticmethod  
    def remove_from_running_jobs(job):
        Progressor.running_jobs.discard(job)

    @staticmethod
    async def _update_progress():
        # update job progress and training speed periodically
        freq = 8
        counter = 0
        finished_jobs = []

        # save progress of each job at the beginning of time slot
        saved_progress_dict = dict()
        for job in Progressor.running_jobs.copy():
            saved_progress_dict[job.uid] = job.progress

        # collect cpu occupations
        Progressor.ps_cpu_occupations = []
        Progressor.worker_cpu_occupations = []

        # collect # of running tasks
        Progressor.num_running_tasks = []

        while counter < freq and len(Progressor.running_jobs) > 0:
            # if no jobs in this timeslot, skip it
            for i in range(config.TS_INTERVAL // freq):
                await asyncio.sleep(1)
                if Progressor.should_stop:
                    return
            counter += 1
            cpu_usage_dict = dict()  # job, cpu_usage in this interval
            num_tasks = 0

            for job in Progressor.running_jobs.copy():
                try:
                    logger.debug(f'Trying to read progress/speed stats #{counter}')
                    (progress_list, val_loss_list) = job.get_training_progress_stats()
                    speed_list = job.get_training_speed()
                    (ps_metrics, worker_metrics) = job.get_metrics()

                    if sum(speed_list) == 0 or not progress_list:
                        continue

                    logger.debug(f'got training progress. speed_list={speed_list}')
                except:
                    logger.info('get training stats error!')
                    continue

                # progress, train_acc, train_loss, val_acc, val_loss
                # assume format [(epoch, batch), ...]
                # assume job epoch size is given
                if job.envs.kv_store == 'dist_sync':
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
                        max_epoch_index = epoch_list.index(max(epoch_list))
                        max_batch_index = batch_list.index(max(batch_list))
                        logger.debug(f'epoch_size: {job.epoch_size}')
                        if max_epoch_index == max_batch_index:
                            job.progress = saved_progress_dict[job.uid] + (epoch_list[max_epoch_index] + batch_list[max_epoch_index]*1.0/job.epoch_size)
                        else:
                            job.progress = saved_progress_dict[job.uid] + (epoch_list[max_batch_index] + batch_list[max_batch_index] * 1.0 / job.epoch_size)
                else:
                    sum1 = 0
                    sum2 = 0
                    for epoch, batch in progress_list:
                        sum1 += epoch
                        sum2 += batch
                    job.progress += sum1 / len(progress_list) + sum2 / len(progress_list) / job.epoch_size

                # update training speed dict
                logger.debug(f'{(job.resources.ps.num_ps, job.resources.worker.num_worker)}: {sum(speed_list) / int(job.metadata.batch_size)} batches/second')
                job.training_speeds[(job.resources.ps.num_ps, job.resources.worker.num_worker)] = sum(speed_list) / int(job.metadata.batch_size)

                # get # of running tasks
                num_tasks += (job.resources.ps.num_ps + job.resources.worker.num_worker)

                # compute cpu usage difference
                ps_cpu_usage_list = []
                for metrics in ps_metrics:
                    ps_cpu_usage_list.append(metrics['cpu/usage_rate'] / 1000.0)
                ps_cpu_diff = max(ps_cpu_usage_list) - min(ps_cpu_usage_list)
                avg_ps_cpu = sum(ps_cpu_usage_list)/len(ps_cpu_usage_list)

                worker_cpu_usage_list = []
                for metrics in worker_metrics:
                    worker_cpu_usage_list.append(metrics['cpu/usage_rate'] / 1000.0)
                worker_cpu_diff = max(worker_cpu_usage_list) - min(worker_cpu_usage_list)
                avg_worker_cpu = sum(worker_cpu_usage_list) / len(worker_cpu_usage_list)

                # cpu usage
                cpu_usage_dict[job] = (avg_ps_cpu, avg_worker_cpu)

                '''
                if job in cpu_usage_dict:
                    old_ps_cpu, old_worker_cpu = cpu_usage_dict[job]
                    new_ps_cpu = max(old_ps_cpu, avg_ps_cpu)
                    new_worker_cpu = max(old_worker_cpu, avg_worker_cpu)
                    cpu_usage_dict[job] = (new_ps_cpu, new_worker_cpu)  # choose the max instead of average
                else:
                    cpu_usage_dict[job] = (avg_ps_cpu, avg_worker_cpu)
                '''

                # logger
                logger.info(f'job name: {job.name}, model name: {job.metadata.modelname} \
                    , kv_store: {job.envs.kv_store}\
                    , batch_size: {job.metadata.batch_size}\
                    , num_ps: {job.resources.ps.num_ps}, num_worker: {job.resources.worker.num_worker}\
                    , progress_list: {progress_list}\
                    , job total progress: {job.progress:.3f}\
                    , num_epochs: {job.num_epochs}\
                    , speed_list: {speed_list}, sum_speed (samples/second): {sum(speed_list)}\
                    , sum_speed(batches/second): {sum(speed_list) / int(job.metadata.batch_size)}\
                    , ps cpu usage diff: {ps_cpu_diff}\
                    , worker cpu usage diff: {worker_cpu_diff}')

                # whether job has been finished
                if job.progress >= job.num_epochs:
                    # progress start from epoch 0
                    job.status = 'completed'
                    job.end_slot = timer.get_clock()
                    job.end_time = time.time()
                    # job.delete(True)
                    logger.info(f'{job.name} has completed. \
                    # of time slots: {job.end_slot - job.arrival_slot} \
                    completion time: {job.end_time - job.arrival_time}')

                    Progressor.running_jobs.remove(job)
                    finished_jobs.append(job)
                else:
                    continue

            # get normalized cpu in this interval and append it to this timeslot
            slot_avg_ps_cpu_list = []
            slot_avg_worker_cpu_list = []
            for key, value in cpu_usage_dict.items():
                job = key
                ps_cpu, worker_cpu = value
                norm_ps_cpu = ps_cpu / job.resources.ps.ps_cpu
                norm_worker_cpu = worker_cpu / job.resources.worker.worker_cpu
                slot_avg_ps_cpu_list.append(norm_ps_cpu)
                slot_avg_worker_cpu_list.append(norm_worker_cpu)

            if len(slot_avg_ps_cpu_list) > 0:
                ps_cpu_occupation = sum(slot_avg_ps_cpu_list) / len(slot_avg_ps_cpu_list)
                Progressor.ps_cpu_occupations.append(ps_cpu_occupation)
                logger.debug(f'Normalized ps cpu: {ps_cpu_occupation}')

                worker_cpu_occupation = sum(slot_avg_worker_cpu_list) / len(slot_avg_worker_cpu_list)
                Progressor.worker_cpu_occupations.append(worker_cpu_occupation)
                logger.debug(f'Normalized worker cpu: {worker_cpu_occupation}')

            Progressor.num_running_tasks.append(num_tasks)

        # end of time slot
        return finished_jobs
