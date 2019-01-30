#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64
import ujson as json
from collections import Counter
import pylru as lru

import jsonpickle
from mrtarget.common import require_all
from mrtarget.common.connection import new_redis_client
jsonpickle.set_preferred_backend('ujson')
import logging
import uuid
import datetime
from threading import Thread

import numpy as np
import psutil
import cProfile
np.seterr(divide='warn', invalid='warn')

from mrtarget.Settings import Config

try:
    import cPickle as pickle
except ImportError:
    import pickle
import time
from multiprocessing import Process, current_process
from colorama import Fore, Back, Style

logger = logging.getLogger(__name__)

import signal

class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException

signal.signal(signal.SIGALRM, timeout_handler)

_redis_queue_worker_base = {'profiling': False}

def enable_profiling(enable=True):
    _redis_queue_worker_base['profiling'] = enable

def millify(n):
    try:
        n = float(n)
        millnames=['','K','M','G','P']
        millidx=max(0, min(len(millnames) - 1,
                           int(np.math.floor(np.math.log10(abs(n)) / 3))))
        return '%.1f%s'%(n/10**(3*millidx),millnames[millidx])
    except Exception:
        return str(n)

class RedisQueue(object):
    '''A simple pickable FIFO queue based on a Redis backend.

    Once the queue is initialised, add messages with the :func:`self.put`
    method. If the maximum size of the queue is reached the queue will block
    until some elements are picked up from the queue. Once the message
    submission is done, you can signal it to the queue with the
    :func:`self.submission_finished` method. Every pickable object is accepted
    and it will be stored as a pickled string in redis. When a message is put
    in the queue a key is generated and put in self.main_queue. The pickled
    string is stored in a different key with the pattern described by
    self.VALUE_STORE.

    Once a client request a message with the :func:`self.get` method a the
    message key is taken from self.main_queue and put in the
    self.processing_queue with a timestamp. If the client is able to process
    the message it should signal it with the :func:`self.done` method,
    eventually flagging if there was a processing error.

    It is possible to detect jobs that were picked up but not completed in time
    with the :func:`self.get_timedout_jobs` and resubmit them in the queue with
    the :func:`self.put_back_timedout_jobs` method.


    Once done (completely or partially) the :func:`self.close` method must be
    called to clean up data stored in redis.

    It is safe to pass the object to a worker if a redis server :param
    r_server: is not passed when the RedisQueue Object is initialised. In this
    case a :param r_server: (typically instantiated in the worker process)
    needs to be passed when calling the methods.

    Given the job-process oriented design by default keys stored in redis will
    expire in 2 days

    '''

    MAIN_QUEUE = "queue:main:%(queue)s"
    PROCESSING_QUEUE = "queue:processing:%(queue)s"
    VALUE_STORE = "queue:values:%(queue)s:%(key)s"
    SUBMITTED_STORE = "queue:submitted:%(queue)s"
    SUBMISSION_FINISH_STORE = "queue:submitionfinished:%(queue)s"
    PROCESSED_STORE = "queue:processed:%(queue)s"
    ERRORS_STORE = "queue:errors:%(queue)s"
    TOTAL_STORE = "queue:total:%(queue)s"
    BATCH_SIZE_STORE = "queue:total:%(queue)s"

    def __init__(self,
                 queue_id=None,
                 r_server = None,
                 max_size = 25000,
                 job_timeout = 30,
                 ttl = 60*60*24+2,
                 total=None,
                 batch_size=1,
                 serialiser = 'pickle'):
        '''
        :param queue_id: queue id to attach to preconfigured queues
        :param r_server: a redis.Redis instance to be used in methods. If
                             supplied the RedisQueue object will not be
                             pickable
        :param max_size: maximum size of the queue. queue will block if full,
                         and allow put only if smaller than the maximum size.
        :param serialiser: choose the serialiser backend: json (use ujson,
                           default) jsonpickle (use ujson) else will use pickle
        :return:
        '''
        self.logger = logging.getLogger(__name__)
        if not queue_id:
            queue_id = uuid.uuid4().hex
        self.queue_id = queue_id
        self.main_queue = self.MAIN_QUEUE% dict(queue = queue_id)
        self.processing_key = self.PROCESSING_QUEUE % dict(queue = queue_id)
        self.submitted_counter = self.SUBMITTED_STORE % dict(queue = queue_id)
        self.processed_counter = self.PROCESSED_STORE % dict(queue = queue_id)
        self.errors_counter = self.ERRORS_STORE % dict(queue = queue_id)
        self.submission_done = self.SUBMISSION_FINISH_STORE % dict(queue = queue_id)
        self.total_key = self.TOTAL_STORE % dict(queue=queue_id)
        self.batch_size_key = self.BATCH_SIZE_STORE % dict(queue=queue_id)
        self.r_server = r_server
        self.job_timeout = job_timeout#todo: store in redis
        self.max_queue_size = max_size#todo: store in redis
        self.default_ttl = ttl#todo: store in redis
        self.serialiser = serialiser
        self.batch_size = batch_size  # todo: store in redis
        self.started = False
        self.start_time = time.time()
        if total is not None:
            self.total = total
            if r_server is not None:
                self.set_total(total, r_server)

    def dumps(self, element):
        if self.serialiser == 'json':
            return json.dumps(element, double_precision=32)
        elif self.serialiser == 'jsonpickle':
            return jsonpickle.dumps(element)
        elif not self.serialiser:
            return element
        else:#default to pickle
            return base64.encodestring(pickle.dumps(element, protocol=pickle.HIGHEST_PROTOCOL))

    def loads(self, element):
        if self.serialiser == 'json':
            return json.loads(element)
        elif  self.serialiser == 'jsonpickle':
            return jsonpickle.loads(element)
        elif not self.serialiser:
            return element
        else:
            return pickle.loads(base64.decodestring(element))

    def put(self, element, r_server=None):
        if element is None:
            pass
        if not self.started:
            self.start_time = time.time()
            self.started = True
        r_server = self._get_r_server(r_server)
        queue_size = r_server.llen(self.main_queue)
        if queue_size:
            while queue_size >= self.max_queue_size:
                time.sleep(0.01)
                queue_size = r_server.llen(self.main_queue)
        key = uuid.uuid4().hex
        pipe = r_server.pipeline()
        pipe.lpush(self.main_queue, key)
        pipe.expire(self.main_queue, self.default_ttl)
        pipe.setex(self._get_value_key(key),
                   self.dumps(element),
                   self.default_ttl)
        pipe.incr(self.submitted_counter)
        pipe.expire(self.submitted_counter, self.default_ttl)
        pipe.execute()
        return key

    def get(self, r_server=None, timeout = 30):
        '''

        :param r_server:
        :param wait_on_empty:
        :return: message id, message content
        '''
        r_server = self._get_r_server(r_server)
        response = r_server.brpop(self.main_queue, timeout= timeout)
        if response is None:
            return
        key = response[1]
        pipe = r_server.pipeline()
        pipe.zadd(self.processing_key, key, time.time())
        pipe.expire(self.processing_key, self.default_ttl)
        pipe.execute()
        serialised = r_server.get(self._get_value_key(key))
        if serialised is not None:
            return key, self.loads(serialised)
        return None

    def done(self,key, r_server=None, error = False):
        r_server = self._get_r_server(r_server)
        pipe = r_server.pipeline()
        pipe.zrem(self.processing_key, key)
        pipe.delete(self._get_value_key(key))
        pipe.incr(self.processed_counter)
        pipe.expire(self.processed_counter, self.default_ttl)
        if error:
            pipe.incr(self.errors_counter)
            pipe.expire(self.errors_counter, self.default_ttl)
        pipe.execute()

    def _get_value_key(self, key):
        return self.VALUE_STORE % dict(queue = self.queue_id, key =key)

    def get_total(self, r_server = None):
        r_server = self._get_r_server(r_server)
        total = r_server.get(self.total_key)
        if total is None:
            return
        return int(total)

    def set_total(self, total, r_server=None):
        r_server = self._get_r_server(r_server)
        r_server.set(self.total_key, total)

    def incr_total(self, increment, r_server=None):
        r_server = self._get_r_server(r_server)
        return r_server.incr(self.total_key, increment)

    def get_processing_jobs(self, r_server = None):
        r_server = self._get_r_server(r_server)
        return r_server.zrange(self.processing_key, 0, -1, withscores=True)


    def get_timedout_jobs(self, r_server = None, timeout=None):
        r_server = self._get_r_server(r_server)
        if timeout is None:
            timeout = self.job_timeout
        timedout_jobs = [i[0] for i in r_server.zrange(self.processing_key, 0, -1, withscores=True) if time.time() - i[1] > timeout]
        if timedout_jobs:
            self.logger.debug('%i jobs timedout jobs in queue %s'%(len(timedout_jobs), self.queue_id))
        return timedout_jobs

    def put_back(self, key, r_server=None, ):
        r_server = self._get_r_server(r_server)
        if r_server.zrem(self.processing_key, key):
            r_server.lpush(self.queue_id, key)

    def put_back_timedout_jobs(self, r_server=None, timeout=None):
        r_server = self._get_r_server(r_server)
        if timeout is None:
            timeout = self.job_timeout
        for key in r_server.zrange(self.processing_key, 0, -1, withscores=True):
            if time.time()-key[1]>timeout:
                if r_server.zrem(self.processing_key, key[0]):
                    r_server.lpush(self.main_queue, key[0])
                    self.logger.debug('%s job is timedout and was put back in queue %s' %(key[0],self.queue_id))
                else:
                    self.logger.debug('%s job is timedout and was NOT put back in queue %s' % (key[0], self.queue_id))

    def __str__(self):
        return self.queue_id

    def get_status(self, r_server=None):
        r_server = self._get_r_server(r_server)
        data = dict(queue_id=self.queue_id,
                    submitted_counter=int(r_server.get(self.submitted_counter) or 0),
                    processed_counter=int(r_server.get(self.processed_counter) or 0),
                    errors_counter=int(r_server.get(self.errors_counter) or 0),
                    submission_done=bool(r_server.getbit(self.submission_done, 1)),
                    start_time=self.start_time,
                    queue_size=self.get_size(r_server),
                    max_queue_size=self.max_queue_size,
                    processing_jobs=len(self.get_processing_jobs(r_server)),
                    timedout_jobs=len(self.get_timedout_jobs(r_server)),
                    total=self.get_total(r_server),
                    client_data = r_server.info('clients'),
                    )
        # todo: add proper support for timedout jobs, possibly kill them: http://stackoverflow.com/questions/25027122/break-the-function-after-certain-time
        # if data['timedout_jobs']:
        #     self.put_back_timedout_jobs(r_server=r_server)
        return data

    def close(self, r_server=None):
        r_server = self._get_r_server(r_server)
        # values_left = r_server.keys(self.VALUE_STORE % dict(queue = self.queue_id, key ='*')) #slow
        # implementation. it will look over ALL the keys in redis. is slow if may other things are there.
        values_left = [self.VALUE_STORE % dict(queue=self.queue_id, key=key) \
                       for key in r_server.lrange(self.main_queue, 0, -1)]  # fast implementation
        pipe = r_server.pipeline()
        for key in values_left:
            pipe.delete(key)
        pipe.delete(self.main_queue)
        pipe.delete(self.processing_key)
        pipe.delete(self.processed_counter)
        pipe.delete(self.submitted_counter)
        pipe.delete(self.errors_counter)
        pipe.execute()

    def get_size(self, r_server=None):
        r_server = self._get_r_server(r_server)
        return r_server.llen(self.main_queue)

    def get_value_for_key(self, key, r_server=None):
        r_server = self._get_r_server(r_server)
        value = r_server.get(self._get_value_key(key))
        if value:
            return pickle.loads(value)
        return None

    def set_submission_finished(self, r_server=None):
        r_server = self._get_r_server(r_server)
        pipe = r_server.pipeline()
        pipe.setbit(self.submission_done, 1, 1)
        pipe.expire(self.submission_done, self.default_ttl)
        pipe.execute()

    def is_submission_finished(self, r_server=None):
        r_server = self._get_r_server(r_server)
        return r_server.getbit(self.submission_done, 1)

    def is_empty(self, r_server=None):
        return self.get_size(self._get_r_server(r_server)) == 0

    def is_done(self, r_server=None):
        r_server = self._get_r_server(r_server)
        if self.is_submission_finished(r_server) and self.is_empty(r_server):
            pipe = r_server.pipeline()
            pipe.get(self.submitted_counter)
            pipe.get(self.processed_counter)
            submitted, processed = pipe.execute()
            submitted = int(submitted or 0)
            processed = int(processed or 0)
            return submitted <= processed
        return False

    def _get_r_server(self, r_server=None):
        return r_server if r_server else self.r_server



class RedisQueueWorkerProcess(Process):
    '''
    Base class for workers attached to the RedisQueue class that runs in a separate process
    it requires a queue in object to get data from, and a redis connection path.
    if a queue out is specified it will push the output to that queue.
    the implemented classes needs to add an implementation of the 'process' method, that either store the output or
    returns it to be stored in a queue out
    '''
    def __init__(self,
                    queue_in,
                    redis_path,
                    queue_out=None,
                    auto_signal_submission_finished=True,
                    ignore_errors =[],
                    **kwargs
                    ):


        super(RedisQueueWorkerProcess, self).__init__(**kwargs)
        self.queue_in = queue_in #TODO: add support for multiple queues with different priorities
        self.queue_out = queue_out
        self.redis_path = redis_path
        self.auto_signal = auto_signal_submission_finished
        self.queue_in_as_batch = queue_in.batch_size >1
        if self.queue_out:
            self.queue_out_batch_size = queue_out.batch_size
        self.logger = logging.getLogger(__name__)
        self.logger.info('%s started' % self.name)
        self.job_result_cache = []
        self.kill_switch = False
        self.ignore_errors = ignore_errors

    def _inner_run(self):
        # here we are inside the new process
        self._init()

        while not self.queue_in.is_done(r_server=self.r_server) and not self.kill_switch:
            job = self.queue_in.get(r_server=self.r_server, timeout=1)

            if job is not None:
                key, data = job
                error = False
                signal.alarm(self.queue_in.job_timeout)
                try:
                    if self.queue_in_as_batch:
                        job_results = [self.process(d) for d in data]
                    else:
                        job_results = self.process(data)
                    if self.queue_out is not None and job_results is not None:
                        self.put_into_queue_out(job_results, aggregated_input = self.queue_in_as_batch)
                except TimeoutException as e:
                    error = True
                    self.logger.exception('Timed out processing job %s: %s' % (key, e.message))
                except Exception as e:
                    error = True
                    for ignored_error in self.ignore_errors:
                        if isinstance(e, ignored_error):
                            error = False
                    if error:
                        self.logger.exception('Error processing job %s: %s' % (key, e.message))
                    else:
                        self.logger.debug('Error processing job %s: %s' % (key, e.message))
                else:
                    signal.alarm(0)

                self.queue_in.done(key, error=error, r_server=self.r_server)
            else:
                # self.logger.info('nothing to do in '+self.name)
                time.sleep(.01)

        if self.job_result_cache:
            self._clear_job_results_cache()

        self.logger.info('%s done processing' % self.name)
        if (self.queue_out is not None) and self.auto_signal:
            self.queue_out.set_submission_finished(self.r_server)# todo: check for problems with concurrency. it might be signalled as finished even if other workers are still processing

        # closing everything properly before exiting the spawned process/thread
        self._close()

    def _outer_run(self):
        cur_file_token = current_process().name

        if _redis_queue_worker_base['profiling']:
            print str(_redis_queue_worker_base)
            cProfile.runctx('self._inner_run()',
                            globals(), locals(),
                            '/tmp/prof_%s.prof' % cur_file_token)
        else:
            self._inner_run()

    def run(self):
        self._outer_run()

    def _split_iterable(self, item_list, size):
            for i in range(0, len(item_list), size):
                yield item_list[i:i + size]

    def put_into_queue_out(self, result, aggregated_input = False ):
        if result is not None:
            if self.queue_out_batch_size > 1:
                if aggregated_input:
                    not_none_results = [r for r in result if r is not None]
                    for batch in self._split_iterable(not_none_results, self.queue_out_batch_size):
                        self.queue_out.put(batch, self.r_server)
                else:
                    self.job_result_cache.append(result)
                    if len(self.job_result_cache) >= self.queue_out_batch_size:
                        self._clear_job_results_cache()
            else:
                if aggregated_input:
                    for r in result:
                        self.queue_out.put(r, self.r_server)
                else:
                    self.queue_out.put(result, self.r_server)

    def _clear_job_results_cache(self):
        self.queue_out.put(self.job_result_cache, self.r_server)
        self.job_result_cache = []

    def init(self):
        pass

    def close(self):
        '''
        implement in subclass to clean up loaders and other trailing elements when the processer is done if needed
        :return:
        '''
        pass

    def process(self, data):
        raise NotImplementedError('please add an implementation to process the data')

    def _init(self):
        self.r_server = new_redis_client()
        # TODO move 1000 to a conf
        self.lru_cache = lru.lrucache(10000)
        self.init()

    def _close(self):
        self.close()
        self.lru_cache.clear()

    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass

    def get_r_server(self):
        return self.r_server

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

        require_all(self.r_server is not None)

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
