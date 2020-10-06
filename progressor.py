import time

import config
from communication import Handler, hub, Payload
from log import logger

class Progressor(Handler):
    def __init__(self, timer):
        super().__init__(connection=hub.connection, entity='progressor')
        self.module_name = 'progressor'
        self.timer = timer
        self.running_jobs = set()
        self.num_running_tasks = []

        self.start()

    def process(self, msg: Payload):
        logger.debug(f'[{self.module_name}] received message: {str(msg)}')

        job = msg.fetch_content('job')
        if msg.source == 'scheduler' and msg.type == 'running':
            if msg.content['job'] not in self.running_jobs:
                self.running_jobs.add(job)
        elif msg.source == 'scheduler' and msg.type == 'pending':
            if job in self.running_jobs:
                self.running_jobs.remove(job)
        elif msg.source == 'scheduler' and msg.type == 'done' and job is None:
            self._update_progress()

    def _update_progress(self):
        # update job progress and training speed periodically
        freq = 8
        counter = 0

        # save progress of each job at the beginning of time slot
        saved_progress_dict = dict()
        for job in self.running_jobs.copy():
            saved_progress_dict[job.uid] = job.progress

        # collect cpu occupations
        self.ps_cpu_occupations = []
        self.worker_cpu_occupations = []

        # collect # of running tasks
        self.num_running_tasks = []

        while counter < freq and len(self.running_jobs) > 0:
            # if no jobs in this timeslot, skip it
            for i in range(config.TS_INTERVAL/freq):
                time.sleep(1)
                if self.exit_flag:
                    return
            counter += 1
            cpu_usage_dict = dict()  # job, cpu_usage in this interval
            num_tasks = 0

            for job in self.running_jobs.copy():
                try:
                    (progress_list, val_loss_list) = job.get_training_progress_stats()
                    speed_list = job.get_training_speed()
                    (ps_metrics, worker_metrics) = job.get_metrics()
                except:
                    logger.info('[progressor] get training stats error!')
                    continue

                # progress, train_acc, train_loss, val_acc, val_loss
                # assume format [(epoch, batch), ...]
                # assume job epoch size is given
                if 'sync' in job.kv_store:
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
                        logger.debug(f'[progressor] epoch_size: {job.epoch_size}')
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
                logger.debug(f'[progressor] {(job.num_ps, job.num_worker)}: {sum(speed_list) / int(job.tot_batch_size)} batches/second')
                job.training_speeds[(job.num_ps, job.num_worker)] = sum(speed_list) / int(job.tot_batch_size)

                # get # of running tasks
                num_tasks += (job.num_ps + job.num_worker)

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
                logger.info(f'[progressor] job name: {job.name}, model name: {job.model_name} \
                    , kv_store: {job.kv_store}\
                    , batch_size: {job.tot_batch_size}\
                    , num_ps: {job.resources.ps.num_ps}, num_worker: {job.resources.worker.num_worker}\
                    , progress_list: {progress_list}\
                    , job total progress: {job.progress:.3f}\
                    , num_epochs: {job.num_epochs}\
                    , speed_list: {speed_list}, sum_speed (samples/second): {sum(speed_list)}\
                    , sum_speed(batches/second): {sum(speed_list) / int(job.tot_batch_size)}\
                    , ps cpu usage diff: {ps_cpu_diff}\
                    , worker cpu usage diff: {worker_cpu_diff}')

                # whether job has been finished
                if job.progress >= job.num_epochs:
                    # progress start from epoch 0
                    job.status = 'completed'
                    job.end_slot = self.timer.get_clock()
                    job.end_time = time.time()
                    job.delete(True)
                    logger.info(f'[progressor] {job.name} has completed. \
                                     # of time slots: {job.end_slot - job.arrival_slot} \
                                     completion time: {job.end_time - job.arrival_time}')
                    self.running_jobs.remove(job)
                    msg = Payload(self.timer.get_clock(), 'progressor', 'completion', {'job':job})
                    hub.push(msg, 'scheduler')
                else:
                    continue

            # get normalized cpu in this interval and append it to this timeslot
            slot_avg_ps_cpu_list = []
            slot_avg_worker_cpu_list = []
            for key, value in cpu_usage_dict.items():
                job = key
                ps_cpu, worker_cpu = value
                norm_ps_cpu = ps_cpu / job.ps_cpu
                norm_worker_cpu = worker_cpu / job.worker_cpu
                slot_avg_ps_cpu_list.append(norm_ps_cpu)
                slot_avg_worker_cpu_list.append(norm_worker_cpu)

            if len(slot_avg_ps_cpu_list) > 0:
                ps_cpu_occupation = sum(slot_avg_ps_cpu_list) / len(slot_avg_ps_cpu_list)
                self.ps_cpu_occupations.append(ps_cpu_occupation)
                logger.debug(f'[progressor] Normalized ps cpu: {ps_cpu_occupation}')

                worker_cpu_occupation = sum(slot_avg_worker_cpu_list) / len(slot_avg_worker_cpu_list)
                self.worker_cpu_occupations.append(worker_cpu_occupation)
                logger.debug(f'[progressor] Normalized worker cpu: {worker_cpu_occupation}')

            self.num_running_tasks.append(num_tasks)

        # end of time slot
        msg = Payload(self.timer.get_clock(), 'progressor', 'completion', None)
        hub.push(msg, 'scheduler')
