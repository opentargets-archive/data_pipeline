import logging
import time
import certifi
import os
import tempfile as tmp
from elasticsearch import Elasticsearch, ConnectionTimeout
from elasticsearch import RequestsHttpConnection
from redislite import Redis

from mrtarget.Settings import Config

# just one redis instance per app
r_instance = {'instance': None}


def new_redis_client():
    return Redis(host=Config.REDISLITE_DB_HOST,
                 port=Config.REDISLITE_DB_PORT)


class PipelineConnectors():

    def __init__(self):
        """Initialises the class

        Declares the connector parts
        """
        self.es = None
        self.r_server = None
        self.logger = logging.getLogger(__name__)

    def init_services_connections(self, redispersist=False):
        '''init es client'''
        connection_attempt = 1
        success = False
        self.persist = redispersist
        r_host = Config.REDISLITE_DB_HOST
        r_port = Config.REDISLITE_DB_PORT
        r_remote = Config.REDISLITE_REMOTE

        hosts = Config.ELASTICSEARCH_NODES

        if hosts:
            self.es = Elasticsearch(hosts=hosts,
                                    maxsize=50,
                                    timeout=1800,
                                    # sniff_on_connection_fail=True,
                                    #sniff_on_start=True,
                                    #sniffer_timeout=60,
                                    retry_on_timeout=True,
                                    max_retries=10,
                                    connection_class=RequestsHttpConnection,
                                    verify_certs=True
                                   )
            try:
                connection_attempt = 1
                while not self.es.ping():
                    wait_time = 3*connection_attempt
                    self.logger.warn('Cannot connect to Elasticsearch retrying in %i', wait_time)
                    time.sleep(wait_time)
                    if connection_attempt >= 3:
                        raise ConnectionTimeout("Couldn't connect to %s after 3 tries" % str(Config.ELASTICSEARCH_NODES))
                    connection_attempt += 1
                self.logger.debug('Connected to elasticsearch nodes: %s', str(Config.ELASTICSEARCH_NODES))
                success = True
            except ConnectionTimeout:
                self.logger.exception("Elasticsearch connection timeout")

        else:
            self.logger.warn('No valid configuration available for elasticsearch')
            self.es = None

        if not r_remote and not r_instance['instance']:
            self.redis_db_file = tmp.mktemp(suffix='.rdb', dir='/tmp')
            self.logger.debug('new named temp file for redis %s with persist %s',
                              self.redis_db_file, str(redispersist))

            r_instance['instance'] = Redis(dbfilename=self.redis_db_file,
                                 serverconfig={'save': [],
                                               'maxclients': 10000,
                                               'bind': r_host,
                                               'port': str(r_port)})

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
        except:
            self.logger.exception('Could not shutdown redislite server')
