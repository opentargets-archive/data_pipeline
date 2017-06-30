import logging
import time
import certifi
import tempfile as tmp
from elasticsearch import Elasticsearch, ConnectionTimeout
from elasticsearch import RequestsHttpConnection
from redislite import Redis

from mrtarget.Settings import Config


def new_redis_client():
    return Redis(host=Config.REDISLITE_DB_HOST,
                 port=Config.REDISLITE_DB_PORT)

class PipelineConnectors():

    def __init__(self):
        """Initialises the class

        Declares the connector parts
        """
        ''' Elastic Search connection'''
        self.es = None

        ''' Redis '''
        self.r_server = None
        self.r_instance = None

        self.logger = logging.getLogger(__name__)

    def init_services_connections(self, redispersist=False):
        '''init es client'''
        connection_attempt = 1
        success = False
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

        redis_db_file = tmp.NamedTemporaryFile(mode='r+w+b',
                                               suffix='.rdb',
                                               delete=(not redispersist))
        self.logger.debug('new named temp file for redis %s with persist %s',
                          redis_db_file.name, str(redispersist))

        self.r_instance = Redis(dbfilename=redis_db_file,
                              serverconfig={'save': [],
                                            'maxclients': 10000,
                                            'port': str(Config.REDISLITE_DB_PORT)})
        self.r_server = new_redis_client()
        self.logger.debug('Established redislite at port %d', Config.REDISLITE_DB_PORT)

        return success

    def close(self):
        try:
            self.r_instance.shutdown()
        except:
            self.logger.exception('Could not shutdown redislite server')
