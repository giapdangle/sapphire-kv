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

import json
from datetime import datetime
import uuid
import logging
import collections

import origin
from kvevent import KVEvent, SIGNAL_RECEIVED_KVEVENT, SIGNAL_SENT_KVEVENT
import queryable
from pubsub import Publisher, Subscriber, ObjectSender
import json_codec
from pydispatch import dispatcher
import threading
from Queue import Queue, Empty
import time
import settings




class NotOriginatorException(Exception):
    pass

class KVObject(object):
    def __init__(self, 
                 object_id=None,
                 origin_id=None, 
                 updated_at=None, 
                 collection=None,
                 **kwargs):

        super(KVObject, self).__init__()

        #if not KVObjectsManager._initialized:
        #    raise RuntimeError("KVObjectsManager not initialized")


        self.__dict__["_lock"] = threading.RLock()
        
        self.__dict__["object_id"] = None
        self.__dict__["origin_id"] = None
        self.__dict__["updated_at"] = None
        self.__dict__["_attrs"] = None
        self.__dict__["_ttl"] = settings.OBJECT_TIME_TO_LIVE

        if object_id:
            self.object_id = object_id
        else:
            self.object_id = str(uuid.uuid4())

        if origin_id:
            self.origin_id = origin_id
        else:
            self.origin_id = origin.id

        if updated_at:
            self.updated_at = updated_at
        else:
            self.updated_at = datetime.utcnow()

        self._attrs = kwargs
        self._pending_events = dict()
        self._event_q = Queue()

        self.set("collection", collection)

    def to_dict(self):
        with self._lock:
            d = {"object_id": self.object_id,
                 "origin_id": self.origin_id,
                 "updated_at": self.updated_at.isoformat()}

            for k, v in self._attrs.iteritems():
                d[k] = v

            return d

    def to_json(self):
        return json_codec.Encoder().encode(self.to_dict())

    def from_dict(self, d):
        with self._lock:
            if "object_id" in d:
                self.object_id = d["object_id"]
                del d["object_id"]

            if "origin_id" in d:
                self.origin_id = d["origin_id"]
                del d["origin_id"]

            if "updated_at" in d:
                try:
                    self.updated_at = datetime.strptime(d["updated_at"], "%Y-%m-%dT%H:%M:%S.%f")

                except ValueError:
                    self.updated_at = datetime.strptime(d["updated_at"], "%Y-%m-%dT%H:%M:%S")  

                del d["updated_at"]

            for k, v in d.iteritems():
                self._attrs[k] = v

            return self

    def from_json(self, j):
        return self.from_dict(json_codec.Decoder().decode(j))

    def _reset_ttl(self):
        self._ttl = settings.OBJECT_TIME_TO_LIVE

    def _post_event(self, event):
        self._event_q.put(event)

    def _apply_events(self):
        events = list()

        with self._lock:
            # process list of events into updates
            updates = dict()

            while not self._event_q.empty():
                ev = self._event_q.get(block=False)

                events.append(ev)
                updates[ev.key] = ev.value

            # run batch update on object
            self.batch_update(updates)
            
        for ev in events:
            ev.receive()

    def __str__(self):
        with self._lock:
            if self.collection:
                s = "KVObject:%s.%s" % \
                    (self.collection,
                     self.object_id)
            
            else:
                s = "KVObject:%s" % \
                    (self.object_id)    

            return s

    def query(self, **kwargs):
        d = self.to_dict()

        if queryable.query_dict(d, **kwargs):
            return self

        return None

    def __getattr__(self, key):
        if key == "_lock":
            return self.__dict__[key]

        else:
            with self.__dict__["_lock"]:
                if key in self.__dict__:
                    return self.__dict__[key]
                
                else:
                    return self._attrs[key]

    def __setattr__(self, key, value):
        with self._lock:
            if (key in self.__dict__) or \
               (key.startswith('_')):
                self.__dict__[key] = value

            else:
                self.set(key, value)

    def get(self, key):
        return self._attrs[key]

    def set(self, key, value, timestamp=None):    
        with self._lock:
            # check if key is already in the object dict
            if key in self.__dict__:
                raise KeyError

            # only add a new key if we are the originator of this object
            if (key in self._attrs) or \
               (key not in self._attrs and self.is_originator()):

                # update current value
                self._attrs[key] = value

                if timestamp == None:
                    self.updated_at = datetime.utcnow()
                else:
                    self.updated_at = timestamp 

                # check if object has been published
                if self.object_id in KVObjectsManager._objects:
                    # generate event
                    event = KVEvent(key=key, 
                                    value=value, 
                                    timestamp=datetime.utcnow(),
                                    object_id=self.object_id)

                    # post event to change list (hash, actually)
                    self._pending_events[key] = event

            else:
                raise KeyError
    
    def batch_set(self, updates, timestamp=None):
        for k, v in updates.iteritems():
            self.set(k, v, timestamp=timestamp)

    def update(self, key, value, timestamp=None):    
        with self._lock:
            # check if key is already in the object dict
            if key in self.__dict__:
                raise KeyError

            # check if changing
            if key not in self._attrs or self._attrs[key] != value:
                
                # set new value
                self._attrs[key] = value

                # set timestamp
                if timestamp == None:
                    self.updated_at = datetime.utcnow()
                else:
                    self.updated_at = timestamp


    def batch_update(self, updates, timestamp=None):
        for k, v in updates.iteritems():
            self.update(k, v, timestamp=timestamp)

    def put(self):
        with self._lock:
            if self.is_originator():
                #if self.object_id not in KVObjectsManager._objects:
                logging.debug("Publishing object: %s" % (str(self)))

                # publish to exchange
                try:
                    KVObjectsManager._publisher.publish_method("publish", self)

                except AttributeError:
                    # publisher not running
                    pass

                # add to objects registry
                KVObjectsManager._objects[self.object_id] = self

    def notify(self):
        # check if new object, and publish if not
        if self.object_id not in KVObjectsManager._objects:
            self.put()

        self.updated_at = datetime.utcnow()

        # push events to exchange
        try:
            # check if there are events to publish
            if len(self._pending_events) > 0:
                logging.debug("Pushing events: %s" % (str(self)))
                KVObjectsManager.send_events(self._pending_events.values())

        except AttributeError:
            # publisher not running
            pass

        # clear events
        self._pending_events = dict()

    def _unpublish(self):
        with self._lock:
            if self.is_originator():
                self.delete()

    def delete(self):
        with self._lock:
            if self.is_originator():
                logging.debug("Unpublishing object: %s" % (str(self)))

                try:
                    KVObjectsManager._publisher.publish_method("delete", self)

                except AttributeError:
                    # publisher not running
                    pass
                    
                del KVObjectsManager._objects[self.object_id]

            else:
                raise NotOriginatorException

    def is_originator(self):
        with self._lock:
            return self.origin_id == origin.id


class ObjectUpdateProcessor(threading.Thread):
    def __init__(self, object_q=None):
        super(ObjectUpdateProcessor, self).__init__()

        self._object_q = object_q

        self._stop_event = threading.Event()

        self.daemon = True

        self.start()

    def run(self):
        while not self._stop_event.is_set():  
            try:
                # get work from queue
                obj = self._object_q.get()

                # run batch update on object
                obj._apply_events()

            except KeyError:
                pass

            except TypeError:
                pass

            except:
                pass

    def stop(self):
        self._stop_event.set()

class EventProcessor(threading.Thread):
    def __init__(self):
        super(EventProcessor, self).__init__()

        self._event_q = Queue()
        self._object_q = Queue()

        self._update_processors = []

        for i in xrange(10):
            self._update_processors.append(ObjectUpdateProcessor(self._object_q))

        self._stop_event = threading.Event()

        self.start()

    def post_events(self, events):
        self._event_q.put(events)

    def run(self):
        while not self._stop_event.is_set():    
            try:
                # get events from the queue
                events = self._event_q.get()

                updates = dict()

                # process events
                for ev in events:
                    ev.kvobject._post_event(ev)

                    updates[ev.kvobject.object_id] = ev.kvobject

                for k, v in updates.iteritems():
                    self._object_q.put(v)

            except Empty:
                pass

            except TypeError:
                pass

    def stop(self):
        self._stop_event.set()
        self._event_q.put(0)


class TTLProcessor(threading.Thread):
    def __init__(self):
        super(TTLProcessor, self).__init__()

        self.daemon = True
    
        self.start()

    def run(self):
        while True:
            time.sleep(10.0)        

            # query for all objects
            all_objects = KVObjectsManager.query(all=True)

            # get list of remote objects
            remote_objects = [o for o in all_objects if o.origin_id != origin.id]

            for obj in remote_objects:
                # decrement time to live
                obj._ttl -= 10

                # check if expired
                if obj._ttl < 0:
                    logging.debug("Deleting expired object: %s" % (str(obj)))

                    # delete object
                    KVObjectsManager.delete(obj.object_id)


class KVObjectsManager(object):
    _objects = dict()
    _publisher = None
    _subscriber = None
    _requester = None
    _event_processor = None
    _ttl_processor = None
    __lock = threading.RLock()
    _initialized = False
    
    @staticmethod
    def query(**kwargs):
        with KVObjectsManager.__lock:
            return [o for o in KVObjectsManager._objects.itervalues() if o.query(**kwargs)]

    @staticmethod
    def get(object_id):
        with KVObjectsManager.__lock:
            return KVObjectsManager._objects[object_id]

    @staticmethod
    def start():
        with KVObjectsManager.__lock:
        # this lock is not really needed here, since the start() method
        # should only be called one time per process.


            if KVObjectsManager._initialized:
                raise RuntimeError("KVObjectsManager already running")


            KVObjectsManager._initialized = True

            settings.init()

            KVObjectsManager._publisher         = Publisher(KVObjectsManager)
            KVObjectsManager._subscriber        = Subscriber(KVObjectsManager)
            KVObjectsManager._sender            = ObjectSender(KVObjectsManager)
            KVObjectsManager._event_processor   = EventProcessor()
            KVObjectsManager._ttl_processor     = TTLProcessor()

            origin_obj = KVObject(collection="origin")

            import socket
            origin_obj.hostname = socket.gethostname()

            origin_obj.notify()
        

    @staticmethod
    def request_objects():
        logging.debug("Requesting objects...")
        KVObjectsManager._publisher.publish_method("request_objects")

    @staticmethod
    def publish_objects():
        with KVObjectsManager.__lock:
            # publish all objects
            for o in KVObjectsManager._objects.itervalues():
                o.put()

    @staticmethod
    def unpublish_objects():
        with KVObjectsManager.__lock:
            for o in KVObjectsManager.query(all=True):
                o._unpublish()

    @staticmethod
    def delete(object_id):
        with KVObjectsManager.__lock:
            if object_id in KVObjectsManager._objects:
                obj = KVObjectsManager._objects[object_id]

                logging.debug("Deleted object: %s" % (str(obj)))

                del KVObjectsManager._objects[object_id]
          
    @staticmethod
    def update(data):
        # reconstruct object
        obj = KVObject().from_dict(data)
    
        if obj.object_id in KVObjectsManager._objects:
            # update object
            KVObjectsManager._objects[obj.object_id].batch_update(obj._attrs)

            # reset time to live
            KVObjectsManager._objects[obj.object_id]._reset_ttl()

        else:
            with KVObjectsManager.__lock:
                # add new object
                KVObjectsManager._objects[obj.object_id] = obj
                logging.debug("Received new object: %s" % (str(obj)))

    @staticmethod
    def receive_events(events):
        # check if events is iterable
        if not isinstance(events, collections.Sequence):
            events = [events]

        events_temp = list()
        for event in events:
            if not isinstance(event, KVEvent):
                # build event object from dictionary
                event = KVEvent().from_dict(event)

                # attach object to event
                event.kvobject = KVObjectsManager._objects[event.object_id]

            events_temp.append(event)

        # post list of events to processor
        KVObjectsManager._event_processor.post_events(events_temp)

    @staticmethod
    def send_events(events):
        # check if events is iterable
        if not isinstance(events, collections.Sequence):
            events = [events]

        KVObjectsManager._publisher.publish_method("events", events)

        for event in events:
            event.send()
            
    @staticmethod
    def stop():
        # stop all KVProcesses
        procs = KVObjectsManager.query(collection="processes")

        for proc in procs:
            try:
                # remote objects will not have this method
                proc.kill()
            except KeyError:
                pass

        for proc in procs:
            try:
                proc.join()
            except KeyError:
                pass

        KVObjectsManager.unpublish_objects()

        KVObjectsManager._publisher.stop()
        KVObjectsManager._subscriber.stop()
        KVObjectsManager._sender.stop()
        KVObjectsManager._event_processor.stop()

    @staticmethod
    def join():        
        KVObjectsManager._publisher.join()
        KVObjectsManager._subscriber.join()
        KVObjectsManager._sender.join()
        KVObjectsManager._event_processor.join()


def start():
    KVObjectsManager.start()

def stop():
    KVObjectsManager.stop()

def query(**kwargs):
    return KVObjectsManager.query(**kwargs)

