#
# <license>
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# 
# 
# Copyright 2013 Sapphire Open Systems
#  
# </license>
#

from sapphire.core import *

from pydispatch import dispatcher
from Queue import Queue, Empty
import logging
import threading
import time
import collections



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

        self._event_q = None

        # connect to event dispatcher
        dispatcher.connect(self._receive_event, signal=SIGNAL_RECEIVED_KVEVENT)

        self._runner = _KVProcessRunner(self)
        self._runner.start()

    def _receive_event(self, event):
        if self._event_q:
            self._event_q.put(event)

    def receive_event(self, source=Query(all=True), keys=[], timeout=1.0):
        if not self._event_q:
            self._event_q = Queue(maxsize=256)

        if not isinstance(keys, collections.Iterable):
            keys = [keys]

        while True:
            event = self._event_q.get(True, timeout=timeout)

            # filter event
            if event.object_id not in [o.object_id for o in source()]:
                continue

            if event.key not in keys:
                continue

            return event

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
                try:
                    self.loop()

                # pass on queue empty exception
                except Empty:
                    pass

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

    #from sapphire.core import *

    app.init()

    for i in xrange(10):
        a = KVProcess()
        a.start()

    app.run()
    


    """
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
    """



