import abc
import time
import threading
from queue import PriorityQueue

from allocators.default_allocator import DefaultAllocator
from log import logger

from progressor import Progressor
from statsor import Statsor
from timer import Timer


class SchedulerBase(metaclass=abc.ABCMeta):
    def __init__(self, cluster):
        self.cluster = cluster
        self.allocator: DefaultAllocator

        self.queueing_jobs = PriorityQueue()
        self.uncompleted_jobs = set()
        self.completed_jobs = set()
        self.running_jobs = set()
        self.cur_ts_completed_jobs = set()
        self.not_ready_jobs = set()

        self.scaling_overhead = 0
        self.testing_overhead = 0

    def submit_job(self, job):
        job.status = 'queueing'
        # priority queue based on arrival time
        self.queueing_jobs.put((job.arrival_time, job))
        self.uncompleted_jobs.add(job)

    def stop(self):
        logger.debug(f'delete unfinished jobs...')
        thread_list = []
        for job in self.uncompleted_jobs:
            del_thread = threading.Thread(target=(lambda job=job: job.delete(True)), args=())
            del_thread.start()
            thread_list.append(del_thread)
        for del_thread in thread_list:
            del_thread.join()

    @abc.abstractmethod
    def _schedule(self):
        pass

    async def init_schedule(self):
        self._schedule()

        # placement
        ps_placements, worker_placements = self.allocator.allocate(self.uncompleted_jobs)

        scaling_tic = time.time()
        self.running_jobs = set()
        # send message to progress to update job progress
        thread_list = []
        for job in self.uncompleted_jobs:
            if job.uid not in ps_placements:
                continue
            ps_placement = ps_placements[job.uid]
            worker_placement = worker_placements[job.uid]
            if len(ps_placement) > 0 and len(worker_placement) > 0:
                # this may cause many ssh connections on a server and an error "ssh_exchange_identification: Connection closed by remote host"
                # to avoid this error, run 'echo "MaxStartups 100:10:200" | sudo tee -a /etc/ssh/sshd_config && sudo service ssh restart' on the server
                self.running_jobs.add(job)
                thread = threading.Thread(target=self.__run, args=(job, ps_placement, worker_placement,))
                thread.start()
                thread_list.append(thread)
                job.status = 'running'
            else:
                job.status = 'pending'
                Progressor.remove_from_running_jobs(job)

        for thread in thread_list:
            thread.join()
        scaling_toc = time.time()
        self.scaling_overhead += (scaling_toc - scaling_tic)
        logger.debug(f'job starting time: {scaling_toc - scaling_tic:.3f} seconds.')

        # signal scheduling completion to progressor
        finished_jobs = await Progressor.update_progress()
        (self.cur_ts_completed_jobs.add(finished_job) for finished_job in finished_jobs)
        await self._delete()



    def __run(self, job, ps_placement, worker_placement):
        """Run a given job with given ps and worker placements

        Args:
            job (DLJob): job instance
            ps_placement (list): list of ps nodes, i.e. ip addresses
            worker_placement (list): list of worker nodes, i.e. ip addresses
        """ 
        logger.debug(f'running {job.name}, num_ps: {job.resources.ps.num_ps}, \
            num_worker: {job.resources.worker.num_worker}, ps_placement: {ps_placement}, \
            worker_placement: {worker_placement}')
        # set placement and start job
        job.set_ps_placement(ps_placement)
        job.set_worker_placement(worker_placement)

        job.start()
        Progressor.add_to_running_jobs(job)

    async def _delete(self):
        """Delete all the jobs in the current timestamp of scheduler, including running and completed jobs.
        """
        delete_tic = time.time()
        # clear existing jobs for next time slot
        for job in self.running_jobs:
            job.delete(True)
            self.allocator.free_job_resources(job)

        for job in self.cur_ts_completed_jobs:
            self.uncompleted_jobs.discard(job)
            self.running_jobs.discard(job)
            self.completed_jobs.add(job)

        self.cur_ts_completed_jobs = []

        delete_toc = time.time()
        self.scaling_overhead += (delete_toc - delete_tic)
        logger.debug(f'job shutdown time: {(delete_toc - delete_tic):.3f} seconds.')

        # get statistics of this timeslot
        await Statsor.stats(Timer.get_clock())
        # signal timer to start next timeslot
        Timer.update_clock()