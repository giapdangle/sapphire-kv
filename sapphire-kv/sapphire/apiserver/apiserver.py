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

from events import EventQueue
from sapphire.core import KVObjectsManager, KVObject, KVEvent, settings

import os
import json
import logging
import datetime
import time
import threading

import bottle
from beaker.middleware import SessionMiddleware

API_SERVER_PORT = 8000
API_SERVER_STATIC_ROOT = os.getcwd()

try:
    API_SERVER_PORT = settings.API_SERVER_PORT
    API_SERVER_STATIC_ROOT = settings.API_SERVER_STATIC_ROOT

except:
    pass

INTERFACE = ('0.0.0.0', API_SERVER_PORT)
VERSION = "1.0"

API_PATH = '/api/v0'


def get_collections():
    all_objs = KVObjectsManager.query(all=True)

    collections = [o.collection for o in all_objs if o.collection is not None]

    # return uniquified list
    return list(set(collections))

class ApiServerJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, KVObject):
            return obj.to_dict()
        elif isinstance(obj, KVEvent):
            return obj.to_dict()
        else:
            return super(ApiServerJsonEncoder, self).default(obj)

################
# Static Files
################

@bottle.get('/')
@bottle.get('/<filename>')
def index(filename=None):
    if not filename:
        return bottle.static_file("index.html", root=API_SERVER_STATIC_ROOT)

    return bottle.static_file(filename, root=API_SERVER_STATIC_ROOT)

#######
# GET
#######

@bottle.get(API_PATH)
def get_root_collection():
    return ApiServerJsonEncoder().encode(["collections", "objects", "events"])

@bottle.get(API_PATH + '/objects')
def get_objects():
    if len(bottle.request.params) == 0:
        items = KVObjectsManager.query(all=True)

    else:
        items = KVObjectsManager.query(**bottle.request.params)

    bottle.response.set_header('Content-Type', 'application/json')

    return ApiServerJsonEncoder().encode(items)

@bottle.get(API_PATH + '/objects/<key>')
def get_object_data(key=None):
    items = KVObjectsManager.query(object_id=key)

    if len(items) == 0:
        bottle.abort(404, "Object not found")

    bottle.response.set_header('Content-Type', 'application/json')

    return ApiServerJsonEncoder().encode(items[0])

@bottle.get(API_PATH + '/collections')
def get_collection_list():
    collections = get_collections()

    bottle.response.set_header('Content-Type', 'application/json')

    return ApiServerJsonEncoder().encode(collections)

@bottle.get(API_PATH + '/collections/<collection>')
def get_object_collection(collection=None):
    if len(bottle.request.params) == 0:
        items = KVObjectsManager.query(collection=collection)

    else:
        items = KVObjectsManager.query(collection=collection, **bottle.request.params)

    if len(items) == 0:
        bottle.abort(404, "Collection not found")

    bottle.response.set_header('Content-Type', 'application/json')

    return ApiServerJsonEncoder().encode(items)


@bottle.get(API_PATH + '/collections/<collection>/<key>')
def get_collection_object_data(collection=None, key=None):
    items = KVObjectsManager.query(collection=collection, object_id=key)

    if len(items) == 0:
        bottle.abort(404, "Object not found")

    bottle.response.set_header('Content-Type', 'application/json')

    return ApiServerJsonEncoder().encode(items[0])

#######
# PUT
#######
@bottle.put(API_PATH + '/objects')
@bottle.put(API_PATH + '/objects/<key>')
def put_object_data(key=None):
    if key:
        obj = KVObject(object_id=key, **bottle.request.json)

    else:
        obj = KVObject(**bottle.request.json)

    # publish to exchange
    obj.publish()

#########
# PATCH
#########
@bottle.route(API_PATH + '/objects/<key>', method='patch')
# NOTE: bottle does not have a shortcut path method, so route is used
def patch_object_data(key=None):
    items = KVObjectsManager.query(object_id=key)

    # if new object
    if len(items) == 0:
        bottle.abort(404, "Object not found")

    else:
        obj = items[0]

        # update attributes
        for k, v in bottle.request.json.iteritems():
            obj.set(k, v)

    # publish to exchange
    obj.publish()

##########
# DELETE
##########
@bottle.delete(API_PATH + '/objects/<key>')
def delete_object(key=None):
    items = KVObjectsManager.query(object_id=key)

    # check if object exists
    if len(items) == 0:
        bottle.abort(404, "Object not found")

    obj = items[0]

    obj.delete()


#################
# Event Channel
#################
class SessionReaper(threading.Thread):
    def __init__(self, session):
        super(SessionReaper, self).__init__()

        self.session = session

        self.start()
    
    def run(self):
        while (time.time() - self.session['_accessed_time']) < 300:
            time.sleep(30.0)

        logging.debug("Reaping session: %s" % self.session.id)
        self.session.delete()


@bottle.get(API_PATH + '/events')
def events_collection():
    # get session, this will automatically create the session
    # if it did not exist
    session = bottle.request.environ.get('beaker.session')

    # check if there is a query for this session
    if "events" not in session:
        # create reaper for session
        SessionReaper(session)

        # create an event queue
        session["events"] = EventQueue()
    
        logging.debug("Starting new events session: %s" % (session.id))

        # return immediately to send session cookie to client
        #return

    # wait for stuff in queue
    events = [session["events"].get(block=True, timeout=60.0)]

    while not session["events"].empty():
        events.append(session["events"].get())

    # set content type
    bottle.response.set_header('Content-Type', 'application/json')
    
    logging.debug("Pushing events to session: %s" % (session.id))

    return ApiServerJsonEncoder().encode(events)


# these options control the Beaker sessions
session_opts = {
    'session.type': 'memory',
    'session.timeout': 300,
    'session.auto': True
}

class APIServer(object):
    def __init__(self):
        super(APIServer, self).__init__()
    
    def run(self):
        logging.info("APIServer serving on interface: %s port: %d" % (INTERFACE[0], INTERFACE[1]))
        logging.info("Static root: %s" % (API_SERVER_STATIC_ROOT))

        server_app = SessionMiddleware(bottle.app(), session_opts)
        #bottle.run(app=server_app, host=INTERFACE[0], port=INTERFACE[1], server='paste', quiet=settings.API_SERVER_QUIET)

        # NOTE: if daemon_threads is False, the server will tend to not terminate when requested
        bottle.run(app=server_app, host=INTERFACE[0], port=INTERFACE[1], server='paste', daemon_threads=True)
        
    

