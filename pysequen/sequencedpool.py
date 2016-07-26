#!/usr/bin/python


from __future__ import print_function

import logging
from Queue import Queue
from threading import Thread, Event, Lock
from slimta.util.deque import BlockingDeque


class SequencedPool(object):
    """Class for a pool of threads or processes
    that handle sequenced workloads"""
    def __init__(self, pool_size, max_pending_tasks, name=None, is_multi_process=False):
        assert pool_size > 0, 'pool_size must be greater than zero'
        assert max_pending_tasks > 0, 'max_pending_tasks must be greater than zero'

        self.pool_size = pool_size
        self.max_pending_tasks = max_pending_tasks
        self.name = name
        self.is_multi_process = is_multi_process

        self.task_pool_lock = Lock()
        self.task_pool = BlockingDeque(maxlen=max_pending_tasks)
        self.ready_worker_queue = Queue(maxsize=self.pool_size)
        self.running_tasks = set()
        self.tasks_changed_event = Event()

        self.workers = []
        for idx in xrange(self.pool_size):
            worker = Worker(self.complete_task)
            self.workers.append(worker)
            self.ready_worker_queue.put(worker)
            worker.start_worker()

        self.dispatcher_thread = Thread(
            target=self.run_dispatcher,
            name='%s dispatcher thread')
        self.dispatcher_thread.start()

    def add_task(self, task):
        print('Adding task [%s]' % task)
        self.task_pool_lock.acquire()
        self.task_pool.append(task)
        self.tasks_changed_event.set()
        self.task_pool_lock.release()

    def complete_task(self, worker, task):
        self.ready_worker_queue.put_nowait(worker)
        self.running_tasks.remove(task)
        self.tasks_changed_event.set()
        print('Completed task %s' % task)

    def run_dispatcher(self):
        while True:
            print('Waiting for ready worker')
            worker = self.ready_worker_queue.get()
            print('Got worker %d' % worker.worker_id)
            try:
                while True:
                    task_to_run = None
                    self.task_pool_lock.acquire()
                    for task_in_pool in self.task_pool:
                        conflict_found = False
                        for running_task in self.running_tasks:
                            if not task_in_pool.can_run_with(running_task):
                                print('Found conflict - task to run [%s] cannot run with running task [%s]' % (task_in_pool, running_task))
                                conflict_found = True
                                break
                        if not conflict_found:
                            print('No conflict found for task to run [%s]' % task_in_pool)
                            task_to_run = task_in_pool
                            break
                    if task_to_run:
                        self.running_tasks.add(task_to_run)
                        self.task_pool.remove(task_to_run)
                        print('Starting task %s' % task_to_run)
                        worker.start_task(task_to_run)
                        self.task_pool_lock.release()
                        break
                    self.task_pool_lock.release()
                    print('No task ready to start, waiting for notification')
                    self.tasks_changed_event.wait()
                    self.tasks_changed_event.clear()
                    print('Got notification of change in tasks')
            except:
                logging.exception('Error processing task for worker %d' % worker.worker_id)


class Worker(object):
    next_worker_id = 1

    def __init__(self, complete_task_handler):
        self.task = None
        self.task_ready_event = Event()
        self.complete_task_handler = complete_task_handler
        self.worker_id = Worker.next_worker_id
        Worker.next_worker_id += 1
        self.is_running = Event()
        self.worker_thread = Thread(name='worker ' + str(self.worker_id),
                                    target=self.run_worker)

    def is_busy(self):
        return self.is_running.is_set()

    def start_task(self, task):
        self.task = task
        self.task_ready_event.set()

    def start_worker(self):
        self.worker_thread.start()

    def run_worker(self):
        while True:
            print('Waiting for task')
            self.task_ready_event.wait()
            self.task_ready_event.clear()
            print('Running task %s' % self.task)
            self.task.run()
            print('Task %s finished' % self.task)
            self.complete_task_handler(self, self.task)
            self.task = None

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
    for task_id in xrange(10):
        sp.add_task(Task(task_id + 1))
