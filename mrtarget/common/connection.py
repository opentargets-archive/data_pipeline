import logging
import os
import time
import certifi
from elasticsearch import Elasticsearch, ConnectionTimeout
from elasticsearch import RequestsHttpConnection
from redislite import Redis

from mrtarget.Settings import Config


class PipelineConnectors():

    def __init__(self):
        """Initialises the class

        Declares the connector parts
        """
        ''' Elastic Search connection'''
        self.es = None

        ''' Redis '''
        self.r_server = None

        self.logger = logging.getLogger(__name__)

    def clear_redislite_db(self):
        if os.path.exists(Config.REDISLITE_DB_PATH):
            os.remove(Config.REDISLITE_DB_PATH)
        time.sleep(2)

    def init_services_connections(self, redispersist=True):
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
                self.logger.info('Connected to elasticsearch nodes: %s', str(Config.ELASTICSEARCH_NODES))
                success = True
            except ConnectionTimeout:
                self.logger.exception("Elasticsearch connection timeout")

        else:
            self.logger.warn('No valid configuration available for elasticsearch')
            self.es = None

        if not redispersist:
            self.clear_redislite_db()
            self.logger.debug('Clearing previous instances of redislite db...')
        self.r_server = Redis(dbfilename=str(Config.REDISLITE_DB_PATH),
                              serverconfig={'save': [],
                                            'maxclients': 10000,
                                            'port': '35000'
                                            })
        self.logger.info('Established redislite DB at %s', Config.REDISLITE_DB_PATH)

        return success

    def close(self):
        try:
            self.r_server.shutdown()
        except:
            self.logger.exception('Could not shutdown redislite server')
