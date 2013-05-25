
from sapphire.core import *

import logging

import threading
import time



class _KVProcessRunner(threading.Thread):
    def __init__(self, process=None):
        super(_KVProcessRunner, self).__init__()

        self._process = process

        self.daemon = True

    def run(self):
        self._process._run()


class KVProcess(KVObject):
    def __init__(self, name=None, **kwargs):
        super(KVProcess, self).__init__(collection="processes", **kwargs)

        self.name = name

        self._stop_event = threading.Event()
        self._stop_event.set()

        self.running = False
        self._killed = False

        self._runner = _KVProcessRunner(self)
        self._runner.start()


    def start(self):
        if self.is_running():
            raise RuntimeError("KVProcess already running")

        self._stop_event.clear()

        self.running = True
        self.notify()

    def stop(self):
        if not self.is_running():
            raise RuntimeError("KVProcess not running")            

        self._stop_event.set()
        self.running = False
        self.notify()

    def is_running(self):
        return not self._stop_event.is_set()

    def wait(self, delay):
        self._stop_event.wait(delay)

    def setup(self):
        pass

    def shutdown(self):
        pass

    def _run(self):
        # NOT running loop
        # wait until start() or self.running == True
        while not self._killed:
            while not self.is_running() and not self._killed:
                time.sleep(1.0)

                if self.running:
                    try:
                        self.start()

                    except RuntimeError:
                        pass

            # process init
            self.setup()
            self.notify()

            # run loop
            # runs until stop() or self.running == False
            while self.is_running():
                self.loop()

                if not self.running:
                    try:
                        self.stop()

                    except RuntimeError:
                        pass

            # process shutdown
            self.shutdown()
            self.notify()

    def loop(self):
        self.wait(1.0)
        
    def join(self, timeout=60.0):
        self._runner.join(timeout)

    def kill(self):
        self._killed = True

        if self.is_running():
            self.stop()


if __name__ == "__main__":

    from sapphire.core import *
    import time

    KVObjectsManager.start()

    for i in xrange(10):
        a = KVProcess()
        a.start()
        #a.stop()


    #a = KVProcess(object_id="jeremy")
    #a.start()

    try:

        while True:
            time.sleep(1.0)

    except KeyboardInterrupt:
        pass


    KVObjectsManager.stop()

