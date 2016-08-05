import logging
import sys
import random
import time
from pysequen.sequencedpool import SequencedPool

if __name__ == '__main__':
    results = []

    class EvenOddTask(object):
        def __init__(self, task_num):
            self.task_num = task_num

        def can_run_with(self, other_task):
            return other_task.task_num % 2 != self.task_num % 2

        def run(self):
            logging.info('Task %d begin' % self.task_num)
            results.append('S%d' % self.task_num)
            delta = random.random() - 0.5
            time.sleep(0.5 + delta)
            results.append('E%d' % self.task_num)
            logging.info('Task %d end' % self.task_num)

    logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s', loglevel=logging.DEBUG)
    logging.getLogger().setLevel(logging.DEBUG)

    sp = SequencedPool(50, 10, 'test pool', False)
    for task_num in xrange(20):
        sp.add_task(EvenOddTask(task_num + 1))
    sys.stdin.readline()
    sp.stop()
    logging.info(results)
