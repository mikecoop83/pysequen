#!/usr/bin/python


from __future__ import print_function

import logging
import traceback
from Queue import Queue
from threading import Thread, Event, Lock
from slimta.util.deque import BlockingDeque


class SequencedPool(object):
    """Class for a pool of threads or processes
    that handle sequenced workloads"""
    def __init__(self, pool_size, max_pending_tasks, name=None, is_multi_process=False):
        assert pool_size > 0, 'pool_size must be greater than zero'
        assert max_pending_tasks > 0, 'max_pending_tasks must be greater than zero'

        self._pool_size = pool_size
        self._max_pending_tasks = max_pending_tasks
        self._name = name
        self._is_multi_process = is_multi_process

        self._task_pool_lock = Lock()
        self._task_pool = BlockingDeque(maxlen=max_pending_tasks)
        self._ready_worker_queue = Queue(maxsize=self._pool_size)
        self._running_tasks = set()
        self._tasks_changed_event = Event()
        self._stopping_event = Event()

        self._workers = []
        for idx in xrange(self._pool_size):
            worker = Worker(self._complete_task)
            self._workers.append(worker)
            self._ready_worker_queue.put(worker)
            worker.start_worker()

        self._dispatcher_thread = Thread(
            target=self._run_dispatcher,
            name='%s dispatcher thread')
        self._dispatcher_thread.start()

    def stop(self):
        self._stopping_event.set()
        self._tasks_changed_event.set()
        self._dispatcher_thread.join()

    def add_task(self, task):
        if self._stopping_event.is_set():
            raise Exception('Cannot add task [%s] after stop has been called' % task)
        print('Adding task [%s]' % task)
        self._task_pool_lock.acquire()
        self._task_pool.append(task)
        self._tasks_changed_event.set()
        self._task_pool_lock.release()

    def _complete_task(self, worker, task):
        self._ready_worker_queue.put_nowait(worker)
        self._running_tasks.remove(task)
        self._tasks_changed_event.set()
        print('Completed task %s' % task)

    def _stop_all_workers(self):
        print('Stopping all workers')
        for _ in xrange(len(self._workers)):
            worker = self._ready_worker_queue.get()
            print('Stopping worker %d' % worker.worker_id)
            worker.stop_worker()
        print('All workers stopped')

    def _run_dispatcher(self):
        while True:
            try:
                print('Waiting for ready worker')
                worker = self._ready_worker_queue.get()
                print('Got worker %d' % worker.worker_id)
                while True:
                    if self._stopping_event.is_set():
                        self._ready_worker_queue.put_nowait(worker)
                        self._stop_all_workers()
                        return
                    task_to_run = None
                    print('Locking task pool')
                    self._task_pool_lock.acquire()
                    print('Checking task pool for tasks')
                    for task_in_pool in self._task_pool:
                        conflict_found = False
                        for running_task in self._running_tasks:
                            if not task_in_pool.can_run_with(running_task):
                                print('Found conflict - task to run [%s] cannot run with running task [%s]'
                                      % (task_in_pool, running_task))
                                conflict_found = True
                                break
                        if not conflict_found:
                            print('No conflict found for task to run [%s]' % task_in_pool)
                            task_to_run = task_in_pool
                            break
                    if task_to_run:
                        self._running_tasks.add(task_to_run)
                        self._task_pool.remove(task_to_run)
                        print('Starting task %s' % task_to_run)
                        worker.start_task(task_to_run)
                        self._task_pool_lock.release()
                        break
                    self._task_pool_lock.release()
                    print('No task ready to start, waiting for notification')
                    self._tasks_changed_event.wait()
                    self._tasks_changed_event.clear()
                    print('Got notification of change in tasks')
            except:
                print('Error processing task for worker %d' % worker.worker_id)
                traceback.print_exc()
            finally:
                print('Dipatcher stopped')


class Worker(object):
    _next_worker_id = 1

    def __init__(self, complete_task_handler):
        self._task = None
        self._task_ready_event = Event()
        self._complete_task_handler = complete_task_handler
        self.worker_id = Worker._next_worker_id
        Worker._next_worker_id += 1
        self._is_running = Event()
        self._stopping_event = Event()
        self._worker_thread = Thread(name='worker ' + str(self.worker_id),
                                     target=self._run_worker)

    def is_busy(self):
        return self._is_running.is_set()

    def start_task(self, task):
        self._task = task
        self._task_ready_event.set()

    def start_worker(self):
        self._worker_thread.start()

    def _run_worker(self):
        while True:
            print('Waiting for task')
            self._task_ready_event.wait()
            if self._stopping_event.is_set():
                break
            self._task_ready_event.clear()
            print('Running task %s' % self._task)
            self._task.run()
            print('Task %s finished' % self._task)
            self._complete_task_handler(self, self._task)
            self._task = None
        print('Worker %d stopped' % self.worker_id)

    def stop_worker(self):
        self._stopping_event.set()
        self._task_ready_event.set()
        self._worker_thread.join()

if __name__ == '__main__':
    import time

    class Task(object):
        def __init__(self, task_id):
            self.task_id = task_id

        def can_run_with(self, other_task):
            return other_task.task_id % 2 != self.task_id % 2

        def __str__(self):
            return str(self.task_id)

        def __eq__(self, other_task):
            return self.task_id == other_task.task_id

        def __hash__(self):
            return hash(self.task_id)

        def run(self):
            print('Task %d sleeping' % self.task_id)
            time.sleep(0.5)
            print('Task %d sleep complete' % self.task_id)

    import sys
    logging.basicConfig(format='%(asctime) %(message)s', loglevel=print)
    logging.getLogger().addHandler(logging.StreamHandler(stream=sys.stdout))
    logging.info('test')

    sp = SequencedPool(3, 7, 'test pool', False)
    for task_id in xrange(2):
        sp.add_task(Task(task_id + 1))
    time.sleep(5)
    sp.stop()
