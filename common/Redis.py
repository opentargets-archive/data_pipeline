import uuid, pickle
import time


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

    def __init__(self,
                 queue_id=None,
                 r_server = None,
                 max_size = 25000,
                 ttl = 60*60*24+2):
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
        self.r_server = r_server
        self.job_timeout = 5
        self.max_queue_size = max_size
        self.default_ttl = ttl


    def put(self, element, r_server=None):
        r_server = self._get_r_server(r_server)
        queue_size = r_server.llen(self.main_queue)
        if queue_size:
            while queue_size >= self.max_queue_size:
                time.sleep(0.1)
                queue_size = r_server.llen(self.main_queue)
        key = uuid.uuid4().hex
        r_server.lpush(self.main_queue, key)
        r_server.expire(self.main_queue, self.default_ttl)
        r_server.setex(self._get_value_key(key), pickle.dumps(element), self.default_ttl)
        r_server.incr(self.submitted_counter)
        r_server.expire(self.submitted_counter, self.default_ttl)
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
        r_server.zadd(self.processing_key, key, time.time())
        r_server.expire(self.processing_key, self.default_ttl)
        return key, pickle.loads(r_server.get(self._get_value_key(key)))

    def done(self,key, r_server=None, error = False):
        r_server = self._get_r_server(r_server)
        self._remove_job(key, r_server)
        r_server.incr(self.processed_counter)
        r_server.expire(self.processed_counter, self.default_ttl)
        if error:
            r_server.incr(self.errors_counter)
            r_server.expire(self.errors_counter, self.default_ttl)

    def _get_value_key(self, key):
        return self.VALUE_STORE % dict(queue = self.queue_id, key =key)

    def get_processing_jobs(self, r_server = None):
        r_server = self._get_r_server(r_server)
        return r_server.zrange(self.processing_key, 0, -1, withscores=True)


    def get_timedout_jobs(self, r_server = None, timeout=None):
        r_server = self._get_r_server(r_server)
        if not r_server:
            r_server = self.r_server
        return [i[0] for i in r_server.zrange(self.processing_key, 0, -1, withscores=True) if time.time() - i[1] > timeout]

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

    def __str__(self):
        return self.queue_id

    def get_status(self, r_server=None):
        r_server = self._get_r_server(r_server)
        lines = ['==== QUEUE: %s ====='%self.queue_id ]
        submitted = int(r_server.get(self.submitted_counter) or 0)
        if submitted:
            processed = int(r_server.get(self.processed_counter) or 0)
            errors = int(r_server.get(self.errors_counter) or 0)
            error_percent = 0.
            if processed:
                error_percent = float(errors)/processed
            submission_finished = bool(r_server.getbit(self.submission_done, 1))
            lines.append('Submitted jobs: %i'%submitted)
            lines.append('Processed jobs: {} | {:.1%}'.format(processed, float(processed)/submitted))
            lines.append('Errors: {} | {:.1%}'.format(errors, error_percent))
            lines.append('-'*50)
            queue_size = self.get_size(r_server)
            queue_size_status = 'empty'
            if queue_size:
                if queue_size >= self.max_queue_size:
                    queue_size_status = "full"
                else:
                    queue_size_status = 'occepting jobs'
            lines.append('Queue size: %i | %s'%(queue_size, queue_size_status))
            lines.append('Jobs being processed: %i'%len(self.get_processing_jobs(r_server)))
            lines.append('Jobs timed out: %i'%len(self.get_timedout_jobs(r_server)))
            lines.append('Sumbission finished: %s'%submission_finished)
            status = 'idle'
            if submitted:
                status = 'jobs sumbitted'
            if processed:
                status = 'processing'
            if submission_finished and \
                    (submitted == processed):
                status = 'done'
            lines.append('-'*50)
            lines.append('STATUS: %s'%status)
        else:
            lines.append('Queue size: 0 | initialised')
        lines.append(('='*50))

        return '\n'.join(lines)

    def close(self, r_server=None):
        r_server = self._get_r_server(r_server)
        #values_left = r_server.keys(self.VALUE_STORE % dict(queue = self.queue_id, key ='*')) #slow implementation. it will look over ALL the keys in redis. is slow if may other things are there.
        values_left = [ self.VALUE_STORE % dict(queue = self.queue_id, key =key) \
                        for key in r_server.lrange(self.main_queue, 0, -1)] # fast implementation
        for key in values_left:
            r_server.delete(key)
        r_server.delete(self.main_queue)
        r_server.delete(self.processing_key)
        r_server.delete(self.processed_counter)
        r_server.delete(self.submitted_counter)
        r_server.delete(self.errors_counter)

    def get_size(self, r_server= None):
        r_server = self._get_r_server(r_server)
        return r_server.llen(self.main_queue)

    def get_value_for_key(self, key, r_server = None):
        r_server = self._get_r_server(r_server)
        value = r_server.get(self._get_value_key(key))
        if value:
            return pickle.loads(value)
        return None

    def _remove_job(self, key, r_server):
        r_server.zrem(self.processing_key, key)
        r_server.delete(self._get_value_key(key))

    def set_submission_finished(self, r_server = None):
        r_server = self._get_r_server(r_server)
        r_server.setbit(self.submission_done, 1, 1)
        r_server.expire(self.submission_done, self.default_ttl)


    def is_done(self, r_server = None):
        r_server = self._get_r_server(r_server)
        submission_finished = r_server.getbit(self.submission_done, 1)
        if submission_finished:
            return r_server.get(self.submitted_counter) == r_server.get(self.processed_counter)
        return False

    def _get_r_server(self, r_server = None):
        if not r_server:
            r_server = self.r_server
        return r_server