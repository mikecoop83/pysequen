import logging
import sys
import random
from pysequen.sequencedpool import SequencedPool

if __name__ == '__main__':
    import time

    class Task(object):
        def __init__(self, task_id):
            self.task_id = task_id

        def can_run_with(self, other_task):
            return other_task.task_id % 2 != self.task_id % 2

        def __str__(self):
            return str(self.task_id)

        def __repr__(self):
            return str(self.task_id)

        def __eq__(self, other_task):
            return self.task_id == other_task.task_id

        def __hash__(self):
            return hash(self.task_id)

        def run(self):
            logging.info('Task %d begin' % self.task_id)
            delta = random.random() - 0.5
            time.sleep(0.5 + delta)
            logging.info('Task %d end' % self.task_id)

    logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s', loglevel=logging.DEBUG)
    logging.getLogger().setLevel(logging.DEBUG)

    sp = SequencedPool(3, 64, 'test pool', False)
    for task_id in xrange(100):
        sp.add_task(Task(task_id + 1))
    sys.stdin.read()
    sp.stop()
