# pysequen

Python library for concurrent processing of tasks with dependencies requiring sequencing

## Install

```bash
pip install pysequen
```

## Get Started

SequencedPool is a class which maintains a pool of threads that handle concurrent and sequenced workloads.

Tasks are added to the pool and are executed according to both the sequence
they are added to the pool along with their ability to execute at the same
time as other tasks.  Tasks must implement two methods in order to be added
to the pool.

```python
def can_run_with(self, other_task):
    return True  # or False

def run(self):
    ... # code to be run on the thread in the pool
```

can_run_with should return True if the tasks can run concurrently or False
if the tasks cannot run concurrently.  When this method is called by the
pool, self will be the task waiting to be executed and other_task will be
the one already executing.  If the logic of this method depends on state
within other_task that may change during execution, then proper care
should be taken (and ideally avoided).  Sine this method may be called
many times by the task dispatcher, it should execute very quickly and not
depend on any external data.
