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

import sys
import os
import socket

import logging
import signal
import time


# Change stdout to automatically encode to utf8.
# Without this, running this in a subprocess and directing
# stdout to subprocess.PIPE will result in unicode errors
# when doing something as inoccuous as "print".
import codecs
sys.stdout = codecs.getwriter('utf-8')(sys.stdout) 


def sigterm_handler():
    logging.info("Received SIGTERM")
    sys.exit()

def init():
    settings.init()

    KVObjectsManager.start()

def run(script_name=None):
    if not script_name:
        script_name = sys.argv[0]
        
    logging.info("Starting: %s" % (script_name))
    logging.info("Process ID: %d" % (os.getpid()))
    logging.info("Origin ID: %s" % (origin.id))

    try:
        # main script control loop
        while True:
            time.sleep(1.0)

    except KeyboardInterrupt:
        logging.info("Shutdown requested")

    except SystemExit:
        logging.info("Shutdown requested by signal SIGTERM")

    KVObjectsManager.stop()
    KVObjectsManager.join()


if __name__ == "__main__":
    run()

