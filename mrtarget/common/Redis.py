#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64
import simplejson as json
from collections import Counter

import jsonpickle
from mrtarget.common import require_all
from mrtarget.common.connection import new_redis_client
jsonpickle.set_preferred_backend('simplejson')
import logging
import uuid
import datetime

import numpy as np
import cProfile
np.seterr(divide='warn', invalid='warn')

from mrtarget.Settings import Config

try:
    import cPickle as pickle
except ImportError:
    import pickle
import time
from multiprocessing import Process, current_process

logger = logging.getLogger(__name__)

import signal

class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException

signal.signal(signal.SIGALRM, timeout_handler)

def millify(n):
    try:
        n = float(n)
        millnames=['','K','M','G','P']
        millidx=max(0, min(len(millnames) - 1,
                           int(np.math.floor(np.math.log10(abs(n)) / 3))))
        return '%.1f%s'%(n/10**(3*millidx),millnames[millidx])
    except Exception:
        return str(n)


class RedisLookupTable(object):
    '''
    Simple Redis-based key value store for string-based objects. Faster than
    its subclasses since it does not serialise and unseriliase strings. By
    default keys will expire in 2 days.

    Allows to store a lookup table (key/value store) in memory/redis so that it
    can be accessed quickly from multiple processes, reducing memory usage by
    sharing.
    '''

    LOOK_UPTABLE_NAMESPACE = 'lookuptable:%(namespace)s'
    KEY_NAMESPACE = '%(namespace)s:%(key)s'

    def __init__(self,
                 namespace = None,
                 r_server = None,
                 ttl = 60*60*24+2):
        if namespace is None:
            namespace = uuid.uuid4()

        self.namespace = self.LOOK_UPTABLE_NAMESPACE % {'namespace': namespace}
        self.r_server = new_redis_client() if not r_server else r_server
        self.default_ttl = ttl

        if self.r_server is None:
            raise RuntimeException("r_server must not be None")


    def set(self, key, obj, r_server = None, ttl = None):
        self._get_r_server(r_server).setex(self._get_key_namespace(key),
                                self._encode(obj),
                                ttl or self.default_ttl)

    def get(self, key, r_server = None):
        server = self._get_r_server(r_server)
        value = server.get(self._get_key_namespace(key))
        if value is not None:
            return self._decode(value)
        raise KeyError(key)


    def keys(self, r_server = None):
        return [key.replace(self.namespace+':','') \
                for key in self._get_r_server(r_server).keys(self.namespace+'*')]

    def set_r_server(self, r_server):
        self.r_server = r_server

    def _get_r_server(self, r_server = None):
        return r_server if r_server else self.r_server

    def _get_key_namespace(self, key, r_server=None):
        return self.KEY_NAMESPACE % {'namespace': self.namespace, 'key': key}

    def _encode(self, obj):
        return obj

    def _decode(self, obj):
        return obj

    def __contains__(self, key, r_server=None):
        server = self._get_r_server(r_server)
        return server.exists(self._get_key_namespace(key))

    def __getitem__(self, key, r_server=None):
        self.get(self._get_key_namespace(key),
                 r_server=self._get_r_server(r_server))

    def __setitem__(self, key, value,  r_server=None):
        if not self.lt_reuse:
            self.set(self._get_key_namespace(key), value,
                     r_server=self._get_r_server(r_server))


class RedisLookupTableJson(RedisLookupTable):
    '''
    Simple Redis-based key value store for Json serialised objects
    By default keys will expire in 2 days
    '''

    def _encode(self, obj):
        return json.dumps(obj)

    def _decode(self, obj):
        return json.loads(obj)



class RedisLookupTablePickle(RedisLookupTable):
    '''
    Simple Redis-based key value store for pickled objects
    By default keys will expire in 2 days
    '''

    def _encode(self, obj):
        return base64.encodestring(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL))

    def _decode(self, obj):
        return pickle.loads(base64.decodestring(obj))
