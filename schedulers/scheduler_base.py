from __future__ import annotations

import abc
import asyncio
from dl_job import DLJob
import time
from allocators.default_allocator import DefaultAllocator
from log import logger
from progressor import Progressor
from queue import PriorityQueue
from statsor import Statsor
from timer import Timer
from typing import List


class SchedulerBase(metaclass=abc.ABCMeta):
    def __init__(self, cluster: SchedulerBase) -> None:
        self.cluster = cluster
        self.allocator: DefaultAllocator

        self.queueing_jobs = PriorityQueue()
        self.uncompleted_jobs = []
        self.completed_jobs = []
        self.running_jobs = []
        self.cur_ts_completed_jobs = []
        self.not_ready_jobs = []

        self.scaling_overhead = 0
        self.testing_overhead = 0

    def submit_job(self, job: DLJob) -> None:
        job.status = "queueing"
        # priority queue based on arrival time
        self.queueing_jobs.put((job.arrival_time, job))
        self.uncompleted_jobs.append(job)

    async def stop(self) -> None:
        logger.debug(f"delete unfinished jobs...")
        thread_list = []
        for job in self.uncompleted_jobs:
            await job.delete(True)

    @abc.abstractmethod
    def _schedule(self):
        pass

    async def init_schedule(self) -> None:
        self._schedule()

        # placement
        ps_placements, worker_placements = self.allocator.allocate(self.uncompleted_jobs)

        scaling_tic = time.time()
        self.running_jobs = []
        # send message to progress to update job progress
        coroutine_list = []
        for job in self.uncompleted_jobs:
            if job.uid not in ps_placements and not job.metadata.dist_strategy == "allreduce":
                continue
            if not job.metadata.dist_strategy == "allreduce":
                ps_placement = ps_placements[job.uid]
            else:
                ps_placement = []
            worker_placement = worker_placements[job.uid]
            if (len(ps_placement) > 0 or job.metadata.dist_strategy == "allreduce") and len(worker_placement) > 0:
                # this may cause many ssh connections on a server and an error "ssh_exchange_identification: Connection closed by remote host"
                # to avoid this error, run 'echo "MaxStartups 100:10:200" | sudo tee -a /etc/ssh/sshd_config && sudo service ssh restart' on the server
                self.running_jobs.append(job)
                coroutine_list.append(asyncio.create_task(self.__run(job, ps_placement, worker_placement)))
                job.status = "running"
            else:
                job.status = "pending"
                Progressor.remove_from_running_jobs(job)

        await asyncio.gather(*coroutine_list)
        scaling_toc = time.time()
        self.scaling_overhead += scaling_toc - scaling_tic
        logger.debug(f"job starting time: {scaling_toc - scaling_tic:.3f} seconds.")

        # signal scheduling completion to progressor
        finished_jobs = await Progressor.update_progress()
        for finished_job in finished_jobs:
            self.cur_ts_completed_jobs.append(finished_job)
        await self._delete()

    async def __run(self, job: DLJob, ps_placement: List[str], worker_placement: List[str]) -> None:
        """
        Run a given job with given ps and worker placements

        Args:
            job: job instance
            ps_placement: list of ps nodes, i.e. ip addresses
            worker_placement: list of worker nodes, i.e. ip addresses
        """
        logger.debug(
            f"running {job.name}, dist_strategy: {job.metadata.dist_strategy}, num_ps: {job.resources.ps.num_ps}, \
            num_worker: {job.resources.worker.num_worker}, ps_placement: {ps_placement}, \
            worker_placement: {worker_placement}"
        )
        # set placement and start job
        job.set_ps_placement(ps_placement)
        job.set_worker_placement(worker_placement)

        await job.start()
        Progressor.add_to_running_jobs(job)

    async def remove_job(self, job: DLJob, reschedule: bool = False) -> None:
        """Removes a queued or running job from the scheduler.

        Args:
            job (DLJob): The job instance to remove.
            reschedule (boolean): Whether to reschedule the job in the next scheduling round. If this value is false,
                the job will be treated as failed.
        Raises:
            ValueError: If the job to be removed is not queued / running.
        """
        if job not in self.uncompleted_jobs:
            raise ValueError(f"Tried to remove a job that is no longer in queue: {job.name}.")

        if job in self.running_jobs:
            self.running_jobs.remove(job)
            self.allocator.free_job_resources(job)
            await job.delete()

        if reschedule:
            return

        job.status = "failed"
        self.uncompleted_jobs.remove(job)

    async def _delete(self) -> None:
        """Delete all the jobs in the current timestamp of scheduler, including running and completed jobs."""
        delete_tic = time.time()
        stopping_jobs = []
        # clear existing jobs for next time slot
        for job in self.running_jobs:
            stopping_jobs.append(asyncio.create_task(job.delete(True)))
            self.allocator.free_job_resources(job)

        for job in self.cur_ts_completed_jobs:
            self.uncompleted_jobs.remove(job)
            self.running_jobs.remove(job)
            self.completed_jobs.append(job)

        if len(stopping_jobs) > 0:
            await asyncio.wait(stopping_jobs)

        self.cur_ts_completed_jobs = []

        delete_toc = time.time()
        self.scaling_overhead += delete_toc - delete_tic
        logger.debug(f"job shutdown time: {(delete_toc - delete_tic):.3f} seconds.")

        # get statistics of this timeslot
        await Statsor.stats(Timer.get_clock())
        # signal timer to start next timeslot
        Timer.update_clock()
