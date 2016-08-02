from threading import Semaphore


class BlockingDeque():

    def __init__(self, maxlen):
        self.sema = Semaphore(maxlen)
        self.items = list()

    def add(self, item):
        self.sema.release()
        self.items.append(item)

    def remove(self, item):
        self.sema.acquire()
        self.items.remove(item)
