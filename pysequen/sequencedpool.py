#!/usr/bin/python


from __future__ import print_function

import logging
from Queue import Queue
from threading import Thread, Event, Lock
from blockingdeque import BlockingDeque

logger = logging.getLogger(__name__)


class SequencedPool(object):
    """
    Class for a pool of threads that handle concurrent and sequenced workloads.

    Tasks are added to the pool and are executed according to both the sequence
    they are added to the pool along with their ability to execute at the same
    time as other tasks.  Tasks must implement two methods in order to be added
    to the pool.

    def can_run_with(self, other_task):
        return True  # or False

    def run(self):
        ... # code to be run on the thread in the pool

    can_run_with should return True if the tasks can run concurrently or False
    if the tasks cannot run concurrently.  When this method is called by the
    pool, self will be the task waiting to be executed and other_task will be
    the one already executing.  If the logic of this method depends on state
    within other_task that may change during execution, then proper care
    should be taken (and ideally avoided).  Sine this method may be called
    many times by the task dispatcher, it should execute very quickly and not
    depend on any external data.
    """
    def __init__(self, pool_size, max_pending_tasks, name=None, is_multi_process=False):
        assert pool_size > 0, 'pool_size must be greater than zero'
        assert max_pending_tasks > 0, 'max_pending_tasks must be greater than zero'

        self._pool_size = pool_size
        self._name = name
        self._is_multi_process = is_multi_process

        self._task_pool_lock = Lock()
        self._task_pool = BlockingDeque(maxlen=max_pending_tasks)
        self._ready_worker_queue = Queue(maxsize=self._pool_size)
        self._running_tasks = set()
        self._tasks_changed_event = Event()
        self._stopping_event = Event()

        self._workers = []
        for worker_id in xrange(self._pool_size):
            worker = Worker(name, worker_id, self._complete_task)
            self._workers.append(worker)
            self._ready_worker_queue.put(worker)
            worker.start_worker()

        self._dispatcher_thread = Thread(
            target=self._run_dispatcher,
            name=('%s dispatcher' % (self._name or '')).strip())
        self._dispatcher_thread.daemon = True
        self._dispatcher_thread.start()

    def stop(self):
        self._stopping_event.set()
        self._tasks_changed_event.set()
        self._dispatcher_thread.join()

    def add_task(self, sequenced_task):
        if self._stopping_event.is_set():
            raise Exception('Cannot add task [%s] after stop has been called' % sequenced_task)
        logger.debug('Adding task [%s]' % sequenced_task)
        self._task_pool_lock.acquire()
        self._task_pool.add(Task(sequenced_task))
        logger.debug('task pool: %s' % self._task_pool.items)
        self._tasks_changed_event.set()
        self._task_pool_lock.release()

    def _complete_task(self, worker, task):
        self._task_pool_lock.acquire()
        self._ready_worker_queue.put_nowait(worker)
        self._running_tasks.remove(task)
        self._tasks_changed_event.set()
        self._task_pool_lock.release()
        logger.debug('Completed task %s' % task)

    def _stop_all_workers(self):
        logger.debug('Stopping all workers')
        for _ in xrange(len(self._workers)):
            worker = self._ready_worker_queue.get()
            logger.debug('Stopping worker %d' % worker.worker_id)
            worker.stop_worker()
        logger.debug('All workers stopped')

    def _run_dispatcher(self):
        try:
            while True:
                logger.debug('Waiting for ready worker')
                worker = self._ready_worker_queue.get()
                logger.debug('Got worker %d' % worker.worker_id)
                while True:
                    if self._stopping_event.is_set():
                        self._ready_worker_queue.put_nowait(worker)
                        self._stop_all_workers()
                        return
                    task_to_run = None
                    logger.debug('Locking task pool')
                    self._task_pool_lock.acquire()
                    logger.debug('Checking task pool for tasks')
                    for task_in_pool in self._task_pool.items:
                        conflict_found = False
                        for running_task in self._running_tasks:
                            if not task_in_pool.can_run_with(running_task):
                                logger.debug('Found conflict - task to run [%s] cannot run with running task [%s]'
                                             % (task_in_pool, running_task))
                                conflict_found = True
                                break
                        if not conflict_found:
                            logger.debug('No conflict found for task to run [%s]' % task_in_pool)
                            task_to_run = task_in_pool
                            break
                    if task_to_run:
                        self._running_tasks.add(task_to_run)
                        self._task_pool.remove(task_to_run)
                        logger.debug('Starting task [%s]' % task_to_run)
                        logger.debug('task pool: %s' % self._task_pool.items)
                        worker.start_task(task_to_run)
                        self._task_pool_lock.release()
                        break
                    self._task_pool_lock.release()
                    logger.debug('No task ready to start, waiting for notification')
                    self._tasks_changed_event.wait()
                    self._tasks_changed_event.clear()
                    logger.debug('Got notification of change in tasks')
        except Exception:
            logger.exception('Error running dispatcher')
        else:
            logger.debug('Dispatcher stopped')


class Worker(object):
    def __init__(self, pool_name, worker_id, complete_task_handler):
        self._task = None
        self._task_ready_event = Event()
        self._complete_task_handler = complete_task_handler
        self.worker_id = worker_id
        self._is_running = Event()
        self._stopping_event = Event()
        self._worker_thread = Thread(
            name=('%s worker %s' % (pool_name or '', worker_id)).strip(),
            target=self._run_worker)
        self._worker_thread.daemon = True

    def is_busy(self):
        return self._is_running.is_set()

    def start_task(self, task):
        self._task = task
        self._task_ready_event.set()

    def start_worker(self):
        self._worker_thread.start()

    def _run_worker(self):
        try:
            while True:
                logger.debug('Waiting for task')
                self._task_ready_event.wait()
                if self._stopping_event.is_set():
                    break
                self._task_ready_event.clear()
                logger.debug('Running task %s' % self._task)
                try:
                    self._task.run()
                except Exception:
                    logger.exception('Error running task %s' % self._task)
                else:
                    logger.debug('Task %s finished' % self._task)
                self._complete_task_handler(self, self._task)
                self._task = None
        except Exception:
            logger.exception('Error running worker')
        logger.debug('Worker %d stopped' % self.worker_id)

    def stop_worker(self):
        self._stopping_event.set()
        self._task_ready_event.set()
        self._worker_thread.join()


class Task(object):
    _next_task_lock = Lock()
    _next_task_id = 1

    def __init__(self, sequenced_task):
        self._sequenced_task = sequenced_task
        Task._next_task_lock.acquire()
        self._task_id = Task._next_task_id
        Task._next_task_id += 1
        Task._next_task_lock.release()

    def can_run_with(self, other_task):
        return self._sequenced_task.can_run_with(other_task._sequenced_task)

    def run(self):
        self._sequenced_task.run()

    def __str__(self):
        return str(self._task_id)

    def __repr__(self):
        return str(self._task_id)

    def __eq__(self, other_task):
        return self._task_id == other_task._task_id

    def __hash__(self):
        return hash(self._task_id)
