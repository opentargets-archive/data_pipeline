import logging
import time
import certifi
import os
import tempfile as tmp
from elasticsearch import Elasticsearch, ConnectionTimeout
from elasticsearch import RequestsHttpConnection
from redislite import Redis
import redis.exceptions as redis_ex

from mrtarget.Settings import Config
from redis.exceptions import ConnectionError


# just one redis instance per app
r_instance = {'instance': None}


def new_redis_client():
    return Redis(host=Config.REDISLITE_DB_HOST,
                 port=Config.REDISLITE_DB_PORT)


def redis_server_is_up(log_h=None):
    is_up = False
    try:
        c = new_redis_client()
        c.ping()
        is_up = True

        log_h.warning('detected a redis-server instance running, so attaching to it')
    except redis_ex.ConnectionError:
        if log_h:
            log_h.info('not detected a redis-server running so starting a new one')
    finally:
        return is_up


def new_es_client(hosts=Config.ELASTICSEARCH_NODES):
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


class PipelineConnectors():

    def __init__(self):
        """Initialises the class

        Declares the connector parts
        """
        ''' elasticsearch data connection'''
        self.es = None

        ''' elasticsearch publication connection'''
        # self.es_pub = None

        ''' Redis '''
        self.r_server = None
        self.logger = logging.getLogger(__name__)
        self.r_instance = r_instance['instance']

    def init_services_connections(self,
                                  redispersist=False):
        success = False
        self.persist = redispersist
        r_host = Config.REDISLITE_DB_HOST
        r_port = Config.REDISLITE_DB_PORT
        r_remote = Config.REDISLITE_REMOTE

        '''init es client for data'''
        hosts = Config.ELASTICSEARCH_NODES

        if hosts:
            self.es = new_es_client(hosts)

            try:
                connection_attempt = 1
                while not self.es.ping():
                    wait_time = 3*connection_attempt
                    self.logger.warn('Cannot connect to elasticsearch data nodes retrying in %i', wait_time)
                    time.sleep(wait_time)
                    if connection_attempt >= 3:
                        raise ConnectionTimeout("Couldn't connect to %s after 3 tries" % str(Config.ELASTICSEARCH_NODES))
                    connection_attempt += 1
                self.logger.debug('Connected to elasticsearch data nodes: %s', str(Config.ELASTICSEARCH_NODES))
                success = True
            except ConnectionTimeout:
                self.logger.exception("Elasticsearch data nodes connection timeout")

        else:
            self.logger.warn('No valid configuration available for elasticsearch data nodes')
            self.es = None

        # '''init es client for publication'''
        # pub_hosts = Config.ELASTICSEARCH_NODES_PUB
        # if pub_hosts != hosts:
        #     if pub_hosts:
        #         self.es_pub = new_es_client(pub_hosts)
        #         try:
        #             connection_attempt = 1
        #             while not self.es.ping():
        #                 wait_time = 3 * connection_attempt
        #                 self.logger.warn('Cannot connect to elasticsearch publication nodes retrying in %i', wait_time)
        #                 time.sleep(wait_time)
        #                 if connection_attempt >= 3:
        #                     raise ConnectionTimeout(
        #                         "Couldn't connect to %s after 3 tries" % str(Config.ELASTICSEARCH_NODES_PUB))
        #                 connection_attempt += 1
        #             self.logger.debug('Connected to elasticsearch publication nodes: %s',
        #                              str(Config.ELASTICSEARCH_NODES_PUB))
        #             success = True
        #         except ConnectionTimeout:
        #             self.logger.exception("Elasticsearch publication nodes connection timeout")

        #     else:
        #         self.logger.warn('No valid configuration available for elasticsearch publication nodes')
        #         self.es_pub = None
        # else:
        #     self.es_pub = self.es


        # check if redis server is already running it will be checked if we dont want
        # remote enabled but the local redis server instance is still running and we want
        # things implicit and stop bothering other developers forced to kill local redis
        if redis_server_is_up(self.logger):
            r_remote = True

        if not r_remote and not r_instance['instance']:
            self.redis_db_file = tmp.mktemp(suffix='.rdb', dir='/tmp')
            self.logger.debug('new named temp file for redis %s with persist %s',
                              self.redis_db_file, str(redispersist))

            r_instance['instance'] = Redis(dbfilename=self.redis_db_file,
                                 serverconfig={'save': [],
                                               'maxclients': 10000,
                                               'bind': r_host,
                                               'port': str(r_port)})
            self.r_instance = r_instance['instance']

        self.r_server = new_redis_client()
        self.logger.debug('Established redislite at %s port %s',
                          r_host,
                          str(r_port))

        return success

    def close(self):
        try:
            if r_instance['instance']:
                r_instance['instance'].shutdown()
                r_instance['instance'] = None
                os.remove(self.redis_db_file + '.settings')
                self.r_instance = None
        except:
            self.logger.exception('Could not shutdown redislite server')
