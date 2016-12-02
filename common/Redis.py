#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64
import json
import logging
import uuid
import datetime
from threading import Thread

import numpy as np
import psutil

np.seterr(divide='warn', invalid='warn')
from tqdm import tqdm
try:
    import cPickle as pickle
except ImportError:
    import pickle
import time
from multiprocessing import Process
from redislite import Redis
from settings import Config
from colorama import Fore, Back, Style

logger = logging.getLogger(__name__)

def millify(n):
    try:
        n = float(n)
        millnames=['','K','M','G','P']
        millidx=max(0, min(len(millnames) - 1,
                           int(np.math.floor(np.math.log10(abs(n)) / 3))))
        return '%.1f%s'%(n/10**(3*millidx),millnames[millidx])
    except Exception, e:
        return n

class RedisQueue(object):
    '''
    A simple pickable FIFO queue based on a Redis backend.

    Once the queue is initialised, add messages with the :func:`self.put` method.
    If the maximum size of the queue is reached the queue will block until some elements are picked up from the queue.
    Once the message submission is done, you can signal it to the queue with the :func:`self.submission_finished`
    method.
    Every pickable object is accepted and it will be stored as a pickled string in redis.
    When a message is put in the queue a key is generated and put in self.main_queue.
    The pickled string is stored in a different key with the pattern described by self.VALUE_STORE.

    Once a client request a message with the :func:`self.get` method a the message key is taken from self.main_queue and
    put in the self.processing_queue with a timestamp.
    If the client is able to process the message it should signal it with the :func:`self.done` method, eventually
    flagging if there was a processing error.

    It is possible to detect jobs that were picked up but not completed in time with the :func:`self.get_timedout_jobs`
    and resubmit them in the queue with the :func:`self.put_back_timedout_jobs` method.


    Once done (completely or partially) the :func:`self.close` method must be called to clean up data stored in redis.

    It is safe to pass the object to a worker if a redis server :param r_server: is not passed when the RedisQueue
    Object is initialised.
    In this case a :param r_server: (typically instantiated in the worker process) needs to be passed when calling
    the methods.

    Given the job-process oriented design by default keys stored in redis will expire in 2 days

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
                 batch_size=1):
        '''
        :param queue_id: queue id to attach to preconfigured queues
        :param r_server: a redis.Redis instance to be used in methods. If supplied the RedisQueue object
                             will not be pickable
        :param max_size: maximum size of the queue. queue will block if full, and allow put only if smaller than the
                         maximum size.
        :return:
        '''
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
        self.batch_size = batch_size  # todo: store in redis
        self.started = False
        self.start_time = time.time()
        if total is not None:
            self.total = total
            if r_server is not None:
                self.set_total(total, r_server)


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
                time.sleep(0.1)
                queue_size = r_server.llen(self.main_queue)
        key = uuid.uuid4().hex
        pipe = r_server.pipeline()
        pipe.lpush(self.main_queue, key)
        pipe.expire(self.main_queue, self.default_ttl)
        pipe.setex(self._get_value_key(key),
                   base64.encodestring(pickle.dumps(element, pickle.HIGHEST_PROTOCOL)),
                   self.default_ttl)
        pipe.incr(self.submitted_counter)
        pipe.expire(self.submitted_counter, self.default_ttl)
        pipe.execute()
        return key

    def get(self, r_server=None, wait_on_empty = 0, timeout = 0):
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
        pickled = r_server.get(self._get_value_key(key))
        if pickled is not None:
            return key, pickle.loads(base64.decodestring(pickled))
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
        if not r_server:
            r_server = self.r_server
        timedout_jobs = [i[0] for i in r_server.zrange(self.processing_key, 0, -1, withscores=True) if time.time() - i[1] > timeout]
        if timedout_jobs:
            logger.debug('%i jobs timedout jobs in queue %s'%(len(timedout_jobs), self.queue_id))
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
                    logger.debug('%s job is timedout and was put back in queue %s' %(key[0],self.queue_id))
                else:
                    logger.debug('%s job is timedout and was NOT put back in queue %s' % (key[0], self.queue_id))

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

        if data['timedout_jobs']:
            self.put_back_timedout_jobs(r_server=r_server)
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
        return self.get_size(r_server) == 0

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
        if not r_server:
            r_server = self.r_server
        if r_server is None:
            try:
               r_server = Redis(Config.REDISLITE_DB_PATH)
            except:
                raise AttributeError('A redis server is required either at class instantiation or at the method level')
        return r_server


class RedisQueueStatusReporter(Process):
    '''
    Cyclically logs the status of a list RedisQueue objects
    '''

    _history_plot = u" ▁▂▃▄▅▆▇█" # get more options from here: http://unicode.org/charts/#symbols
    _history_plot_max = u"▓"
    _history_plot_interval = len(_history_plot)-1

    def __init__(self,
                 queues,
                 interval=15,
                 history = True,
                 history_resolution = 30,
                 ):
        super(RedisQueueStatusReporter, self).__init__()
        self.queues = queues
        self.r_server = Redis(Config.REDISLITE_DB_PATH)
        self.interval = interval
        self.logger = logging.getLogger(__name__)
        self.history = history
        self.historical_data = dict()
        self.history_resolution = history_resolution
        if history:
            for queue in queues:
                self.historical_data[queue.queue_id] = dict(processing_jobs = [],
                                                            queue_size = [],
                                                            timedout_jobs=[],
                                                            processing_speed = [],
                                                            reception_speed = [],
                                                            processed_jobs = [],
                                                            received_jobs = [],
                                                            cpu_load=[],
                                                            memory_load=[],
                                                            memory_use=[],
                                                            )


    def run(self):
        self.logger.info("reporter worker started")

        self.bars = {}
        for i,q in enumerate(self.queues):
            queue_id = self._simplify_queue_id(q.queue_id)
            self.bars[q.queue_id] = dict(
                                         submitted_counter=tqdm(desc='%s received jobs [batch size: %i]'%(queue_id,q.batch_size),
                                                                unit=' jobs',
                                                                total=q.get_total(self.r_server),
                                                                dynamic_ncols=True,
                                                                # position=queue_position+0,
                                                                ),
                                         processed_counter=tqdm(desc='%s processed jobs [batch size: %i]'%(queue_id,q.batch_size),
                                                                unit=' jobs',
                                                                total=q.get_total(self.r_server),
                                                                dynamic_ncols=True,
                                                                # position=queue_position+1,
                                                                ),
                                         last_status = None
                                        )

        while not self.is_done():
            for i,q in enumerate(self.queues):
                queue_position = i * 3
                data = q.get_status(self.r_server)
                self.log(data)
                last_data = self.bars[q.queue_id]['last_status']
                submitted_counter = data['submitted_counter']
                processed_counter = data['processed_counter']
                if last_data:
                    submitted_counter -= last_data['submitted_counter']
                    processed_counter -= last_data['processed_counter']
                if submitted_counter:
                    if data['total'] and self.bars[q.queue_id]['submitted_counter'].total != data['total']:
                        self.bars[q.queue_id]['submitted_counter'].total = data['total']
                    self.bars[q.queue_id]['submitted_counter'].update(submitted_counter)


                if processed_counter:

                    if data['total'] and self.bars[q.queue_id]['processed_counter'].total != data['total']:
                        self.bars[q.queue_id]['processed_counter'].total = data['total']
                    self.bars[q.queue_id]['processed_counter'].update(processed_counter)
                self.bars[q.queue_id]['last_status'] = data


            time.sleep(self.interval)

    def is_done(self):
        for q in self.queues:
            if not q.is_done(self.r_server):
                return False
        return True

    def _simplify_queue_id(self, queue_id):
        if '|' in queue_id:
            return queue_id.split('|')[-1]
        return queue_id

    def log(self, data):
        self.logger.debug(self.format(data))

    def format(self, data):
        now = time.time()
        submitted = data['submitted_counter']
        status = Fore.CYAN+'initialised'+ Fore.RESET
        if submitted:
            processed = data['processed_counter']
            errors = data['errors_counter']
            error_percent = 0.
            if processed:
                error_percent = float(errors) / processed
            submission_finished = data['submission_done']
            processing_speed = 0.
            if processed:
                processing_speed = round(processed / (now - data['start_time']), 2)
            queue_size = data['queue_size']
            queue_size_status = 'empty'
            if queue_size:
                if queue_size >= data['max_queue_size']:
                    queue_size_status = "full"
                else:
                    queue_size_status = 'accepting jobs'

            status = Fore.BLUE +'idle'+ Fore.RESET
            if submitted:
                status = Fore.LIGHTYELLOW_EX + 'jobs sumbitted' + Fore.RESET
            if processed:
                status = Fore.GREEN + 'processing' + Fore.RESET
            if submission_finished and \
                    (submitted == processed):
                status = Fore.RED + 'done' + Fore.RESET
            lines = ['\n****** QUEUE: %s | STATUS %s ******' % (Back.RED+Style.DIM+data['queue_id']+Style.RESET_ALL, status)]
            if self.history:#log history
                if 'done' not in status:
                    historical_data = self.historical_data[data['queue_id']]
                    if not historical_data['processed_jobs']:
                        processing_speed = float(data['processed_counter'])/self.interval
                        reception_speed = float(data['submitted_counter']) / self.interval
                    else:
                        processing_speed = float(data['processed_counter']-historical_data['processed_jobs'][-1]) / self.interval
                        reception_speed = float(data['submitted_counter']-historical_data['received_jobs'][-1]) / self.interval
                    historical_data['queue_size'].append(data["queue_size"])
                    historical_data['processing_jobs'].append(data["processing_jobs"])
                    historical_data['timedout_jobs'].append(data["timedout_jobs"])
                    historical_data['processing_speed'].append(processing_speed)
                    historical_data['reception_speed'].append(reception_speed)
                    historical_data['processed_jobs'].append(data['processed_counter'])
                    historical_data['received_jobs'].append(data['submitted_counter'])
                    cpu_load = psutil.cpu_percent(interval=None)
                    if isinstance(cpu_load, float):
                        historical_data['cpu_load'].append(cpu_load)
                    else:
                        historical_data['cpu_load'].append(0.)
                    memory_load = psutil.virtual_memory()
                    historical_data['memory_load'].append(memory_load.percent)
                    historical_data['memory_use'].append(round(memory_load.used/(1024*1024*1024),2))#Gb of used memory

                lines.append(self._compose_history_line(data['queue_id'], 'received_jobs'))
                lines.append(self._compose_history_line(data['queue_id'], 'reception_speed'))
                lines.append(self._compose_history_line(data['queue_id'], 'processed_jobs'))
                lines.append(self._compose_history_line(data['queue_id'], 'processing_speed'))
                lines.append(self._compose_history_line(data['queue_id'], 'processing_jobs'))
                lines.append(self._compose_history_line(data['queue_id'], 'timedout_jobs'))
                lines.append(self._compose_history_line(data['queue_id'], 'cpu_load'))
                lines.append(self._compose_history_line(data['queue_id'], 'memory_load'))
                lines.append(self._compose_history_line(data['queue_id'], 'memory_use'))
                lines.append(self._compose_history_line(data['queue_id'], 'queue_size', queue_size_status))
            else:#log single datapoint

                    lines.append('Received jobs: %i' % submitted)
                    lines.append('Processed jobs: {} | {:.1%}'.format(processed, float(processed) / submitted))
                    lines.append('Errors: {} | {:.1%}'.format(errors, error_percent))
                    lines.append('Processing speed: {:.1f} jobs per second'.format(processing_speed))
                    lines.append('-' * 50)
                    lines.append('Queue size: %i | %s' % (queue_size, queue_size_status))
                    lines.append('Jobs being processed: %i' % data["processing_jobs"])
                    lines.append('Jobs timed out: %i' % data["timedout_jobs"])
                    lines.append('Sumbmission finished: %s' % submission_finished)
                    lines.append('-' * 50)
                    lines.append('STATUS: %s' % status)
                    lines.append('Elapsed time: %s' % datetime.timedelta(seconds=now - data['start_time']))
            lines.append(('=' * 50))
        else:
            lines = ['****** QUEUE: %s | STATUS: %s ******' % (Back.RED+Style.DIM+data['queue_id']+Style.RESET_ALL, status)]

        return '\n'.join(lines)

    def _average_long_interval(self,data,):
        np_data = np.array(data).astype(float)
        if data:
            try:
                if len(data) > self.history_resolution:
                    normalisation = int(round(len(data)/self.history_resolution))
                    np.mean(np_data[:-(len(data) % normalisation)].reshape(-1, normalisation), axis=1)
                np_data /= np.max(np.abs(np_data), axis=0) # normalize by max value
                if np_data.max() >0:
                    np_data *= (self._history_plot_interval / np_data.max()) #map to interval
            except RuntimeWarning:
                pass
        return np_data

    def _compose_history_line(self, queue_id, key, status = None):
        data = self.historical_data[queue_id][key]
        label = unicode(key.capitalize().replace('_',' '))
        if status is None:
            if data:
                status = u'Current: %s | Max: %s'%(unicode(millify(data[-1])),unicode(millify(max(data))))
        averaged_data = self._average_long_interval(data)
        output = u'%s |%s| %s'%(label.ljust(20), self.sparkplot(averaged_data), status)
        return output.encode('utf8')

    def sparkplot(self, data):
        output = []
        if data.size:
            max_data = data.max()
            rounded_data = np.round(data).astype(int)
            for i, value in enumerate(data):
                if value == max_data:
                    data_char = Fore.YELLOW+self._history_plot_max+Fore.RESET
                elif 0. < value < self._history_plot_interval:
                    data_char = self._history_plot[rounded_data[i]]
                else:
                    data_char = self._history_plot[0]
                output.append(data_char)
        return u''.join(output)


def get_redis_worker(base = Process):
    '''
    Factory for returning workers
    :param base: either a multiprocessing.Process or threading.Thread base class. might work with other base classes with duck typing
    :return: worker class subclassing the base class
    '''

    class RedisQueueWorkerBase(base):
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
                     **kwargs
                     ):


            super(RedisQueueWorkerBase, self).__init__()
            self.queue_in = queue_in #TODO: add support for multiple queues with different priorities
            self.queue_out = queue_out
            self.r_server = Redis(redis_path, serverconfig={'save': []})
            self.auto_signal = auto_signal_submission_finished
            self.queue_in_as_batch = queue_in.batch_size >1
            if self.queue_out:
                self.queue_out_batch_size = queue_out.batch_size
            self.logger = logging.getLogger(__name__)
            self.logger.info('%s started' % self.name)
            self.job_result_cache = []


        def run(self):
            while not self.queue_in.is_done(r_server=self.r_server):
                job = self.queue_in.get(r_server=self.r_server, timeout=1)

                if job is not None:
                    key, data = job
                    error = False
                    try:
                        if self.queue_in_as_batch:
                            job_results = [self.process(d) for d in data]
                        else:
                            job_results = self.process(data)
                        if self.queue_out is not None and job_results is not None:
                            self.put_into_queue_out(job_results, aggregated_input = self.queue_in_as_batch)
                    except Exception as e:
                        error = True
                        self.logger.exception('Error processing job %s: %s' % (key, e.message))

                    self.queue_in.done(key, error=error, r_server=self.r_server)
                else:
                    # self.logger.info('nothing to do in '+self.name)
                    time.sleep(.01)

            if self.job_result_cache:
                self._clear_job_results_cache()

            self.logger.info('%s done processing' % self.name)
            if (self.queue_out is not None) and self.auto_signal:
                self.queue_out.set_submission_finished(self.r_server)# todo: check for problems with concurrency. it might be signalled as finished even if other workers are still processing

            self.close()

        def _split_iterable(self, input, size):
                for i in range(0, len(input), size):
                    yield input[i:i + size]

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

        def close(self):
            '''
            implement in subclass to clean up loaders and other trailing elements when the processer is done
            :return:
            '''
            pass

        #TODO: enforce timeout
        def process(self, data):
            raise NotImplementedError('please add an implementation to process the data')

        def __enter__(self):
            pass

        def __exit__(self, *args):
            self.close()

    return RedisQueueWorkerBase

RedisQueueWorkerProcess = get_redis_worker()
RedisQueueWorkerThread = get_redis_worker(base=Thread)



class RedisLookupTable(object):
    '''
    Simple Redis-based key value store for string-based objects.
    Faster than its subclasses since it does not serialise and unseriliase strings.
    By default keys will expire in 2 days
    '''

    LOOK_UPTABLE_NAMESPACE = 'lookuptable:%(namespace)s'
    KEY_NAMESPACE = '%(namespace)s:%(key)s'

    def __init__(self,
                 namespace = None,
                 r_server = None,
                 ttl = 60*60*24+2):
        if namespace is None:
            namespace = uuid.uuid4()
        self.namespace = self.LOOK_UPTABLE_NAMESPACE % dict(namespace = namespace)
        self.r_server = r_server
        self.default_ttl = ttl


    def set(self, key, obj, r_server = None, ttl = None):
        # if not (isinstance(obj, str) or isinstance(obj, unicode)):
        #     raise AttributeError('Only str and unicode types are accepted as object value. Use the \
        #     RedisLookupTablePickle subclass for generic objects.')
        r_server = self._get_r_server(r_server)
        r_server.setex(self._get_key_namespace(key),
                              self._encode(obj),
                              ttl or self.default_ttl)

    def get(self, key, r_server = None):
        r_server = self._get_r_server(r_server)
        value = r_server.get(self._get_key_namespace(key))
        if value is not None:
            return self._decode(value)
        raise KeyError(key)


    def keys(self, r_server = None):
        r_server = self._get_r_server(r_server)
        return [key.replace(self.namespace+':','') for key in r_server.keys(self.namespace+'*')]


    def _get_r_server(self, r_server = None):
        if not r_server:
            r_server = self.r_server
        if r_server is None:
            raise AttributeError('A redis server is required either at class instantation or at the method level')
        return r_server

    def _get_key_namespace(self, key):
        return self.KEY_NAMESPACE % dict(namespace = self.namespace, key = key)

    def _encode(self, obj):
        return obj

    def _decode(self, obj):
        return obj

    def __contains__(self, key, r_server = None):
        if not r_server:
            r_server = self.r_server
        return r_server.exists(self._get_key_namespace(key))

    def __getitem__(self, key,r_server=None):
        self.get(self._get_key_namespace(key), r_server)

    def __setitem__(self, key, value, r_server=None):
        self.set(self._get_key_namespace(key), value, r_server)


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
