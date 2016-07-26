#!/usr/bin/python

from threading import Event, Lock


class AutoResetEvent(object):
    """Class mimicing the behavior of the .NET AutoResetEvent"""
    def __init__(self, initial_value):
        self.event = Event()
        self.event_lock = Lock()
        if initial_value:
            self.event.set()

    def set(self):
        self.event.set()

    def wait(self):
        while True:
            self.event.wait()
            self.event_lock.acquire()
            if self.event.is_set():
                self.event.clear()
                self.event_lock.release()
                return True
            else:
                self.event_lock.release()
