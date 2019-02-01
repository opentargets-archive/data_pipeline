import logging
import time
import os
import tempfile as tmp
from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
from redislite import Redis
import redis.exceptions as redis_ex

from mrtarget.Settings import Config

# just one redis instance per app
r_instance = {'instance': None}

#TODO hackily use globals for now, replace with proper passing
#once lookup and queues are handled better
default_host = "localhost"
default_port = 6379


def new_redis_client(host=default_host, port=default_port):
    return Redis(host=host, port=port)

def new_es_client(hosts):
    return Elasticsearch(hosts=hosts,
                         maxsize=50,
                         timeout=1800,
                         # sniff_on_connection_fail=True,
                         # sniff_on_start=True,
                         # sniffer_timeout=60,
                         retry_on_timeout=True,
                         max_retries=10,
                         connection_class=RequestsHttpConnection,
                         verify_certs=True)


"""
Simple context manager that handles creation and teardown
of embedded redis instance, if appropriate.

Use in a with statemnt, i.e.

  with RedisManager(remote, host, port) as redisManager:
    redis_client = new_redis_client(host, port)
    ...do stuff...

Don't try and use this anywhere but the main thread. It should work,
but just don't try it!

"""
class RedisManager():
    def __init__(self, remote, host, port):
        self.logger = logging.getLogger(__name__)
        self.r_instance = None
        self.remote = remote
        self.host = host
        self.port = port

    def redis_server_is_up(self):
        is_up = False
        try:
            c = new_redis_client(self.host, self.port)
            c.ping()
            is_up = True
            self.logger.debug('detected a redis-server instance running, so attaching to it')
        except redis_ex.ConnectionError:
            self.logger.warning('not detected a redis-server running')
        finally:
            return is_up

    def create_redislite_if_needed(self):
        # check if redis server is already running it will be checked if we dont want
        # remote enabled but the local redis server instance is still running and we want
        # things implicit and stop bothering other developers forced to kill local redis
        if not self.remote and self.redis_server_is_up():
            raise RuntimeError("Able to connect to redis when should be creating one!")
        elif self.remote and not self.redis_server_is_up():
            #we asked to use an external redis, but it doesn't exist
            raise RuntimeError("Unable to connect to redis")

        if not self.remote and not self.r_instance:
            self.redis_db_file = tmp.mktemp(suffix='.rdb', dir='/tmp')
            self.r_instance = Redis(dbfilename=self.redis_db_file,
                serverconfig={'save': [],
                    'maxclients': 10000,
                    'bind': str(self.host),
                    'port': str(self.port)})

    def __enter__(self):
        self.create_redislite_if_needed()
        return self
        
    def __exit__(self, type, value, traceback):
        if self.r_instance:
            self.r_instance.shutdown()
            self.r_instance = None
            os.remove(self.redis_db_file + '.settings')

        #don't return True to indicate any exceptions have been handled
        #this contex manager is only for cleanup
