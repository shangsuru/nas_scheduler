import abc
from queue import PriorityQueue

from communication import Handler, hub, Payload

class SchedulerBase(Handler, metaclass=abc.ABCMeta):
    def __init__(self, cluster, timer):
        super().__init__(connection=hub.connection, entity='scheduler')

        self.name = None
        self.timer = timer
        self.cluster = cluster

        self.queueing_jobs = PriorityQueue()
        self.uncompleted_jobs = []
        self.completed_jobs = []
        self.cur_ts_completed_jobs = []
        self.not_ready_jobs = set()

        self.scaling_overhead = 0
        self.testing_overhead = 0

    def process(self, msg):
        if msg.type == 'submission':
            job = msg.content['job']
            if job is None:
                # generator has finished the timeslot
                self.__init_schedule()
            else:
                job.status = 'queueing'
                # priority queue based on arrival time
                self.queueing_jobs.put((job.arrival_time, job))
                if job not in self.uncompleted_jobs:
                    self.uncompleted_jobs.append(job)
                else:
                    raise RuntimeError

        elif msg.type == 'completion' and msg.source == 'progressor':
            if job is None:
                # progressor has finished the timeslot
                self._delete()
            else:
                self.cur_ts_completed_jobs.append(job)
        elif msg.type == 'completion' and msg.source == 'statsor':
            # statsor finishes, start next timeslot
            self._start_next_ts()

    @abc.abstractmethod
    def _schedule(self):
        pass

    def __init_schedule(self):
        self._schedule()

        # placement
        ps_placements, worker_placements = self.allocator.allocate(self.uncompleted_jobs)

        scaling_tic = time.time()
        self.running_jobs = []
        # send message to progress to update job progress
        thread_list = []
        for job in self.uncompleted_jobs:
            if job.id not in ps_placements:
                continue
            ps_placement = ps_placements[job.id]
            worker_placement = worker_placements[job.id]
            if len(ps_placement) > 0 and len(worker_placement) > 0:
                # this may cause many ssh connections on a server and an error "ssh_exchange_identification: Connection closed by remote host"
                # to avoid this error, run 'echo "MaxStartups 100:10:200" | sudo tee -a /etc/ssh/sshd_config && sudo service ssh restart' on the server
                self.running_jobs.append(job)
                thread = threading.Thread(target=self.__run, args=(job, ps_placement, worker_placement,))
                thread.start()
                thread_list.append(thread)
                job.status = 'running'

                # send message to progressor
                msg = Payload(self.timer.get_clock(), 'scheduler', 'running', {'job':job})
                hub.push(msg, 'progressor')
            else:
                job.status = 'pending'

                # send message to progressor
                msg = Payload(self.timer.get_clock(), 'scheduler', 'pending', {'job':job})
                hub.push(msg, 'progressor')

        for thread in thread_list:
            thread.join()
        scaling_toc = time.time()
        self.scaling_overhead += (scaling_toc - scaling_tic)
        logger.debug(f'[scheduler] job starting time: {scaling_toc - scaling_tic:.3f} seconds.')

        # send message to progressor to signal scheduling completion
        msg = Payload(self.timer.get_clock(), 'scheduler', 'done')
        hub.push(msg, 'progressor')


    def __run(self, job, ps_placement, worker_placement):
        """Run a given job with given ps and worker placements

        Args:
            job (DLJob): job instance
            ps_placement (list): list of ps nodes, i.e. ip addresses
            worker_placement (list): list of worker nodes, i.e. ip addresses
        """ 
        logger.debug(f'[scheduler] running {job.name}, num_ps: {job.resources.ps.num_ps}, \
            num_worker: {job.resources.worker.num_worker}, ps_placement: {ps_placement}, \
            worker_placement: {worker_placement}')
        # set placement and start job
        # sys.exit()
        job.set_ps_placement(ps_placement)
        job.set_worker_placement(worker_placement)
        job.start()

    def _delete(self):
        """Delete all the jobs in the current timestamp of scheduler, including running and completed jobs.
        """
        for job in self.cur_ts_completed_jobs:
            self.uncompleted_jobs.remove(job)
            self.running_jobs.remove(job)
            self.completed_jobs.append(job)

        self.cur_ts_completed_jobs = []

        delete_tic = time.time()

        # clear existing jobs for next time slot
        for job in self.running_jobs:
            job.delete(True)

        delete_toc = time.time()
        self.scaling_overhead += (delete_toc - delete_tic)
        logger.debug(f'[scheduler] job shutdown time: {(delete_toc - delete_tic):.3f} seconds.')

        # send message to statsor to get statistics of this timeslot
        msg = Payload(self.timer.get_clock(), 'scheduler', 'control', None)
        hub.push(msg, 'statsor')

    def _start_next_ts(self):
        """Send message to timer to signal starting next timeslot.
        """
        msg = Payload(self.timer.get_clock(), 'scheduler', 'control', None)
        hub.push(Payload, 'timer')