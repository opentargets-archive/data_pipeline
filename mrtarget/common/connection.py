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

    def init_services_connections(self, redispersist=False):
        '''init es client'''
        connection_attempt = 1
        success = False
        hosts = Config.ELASTICSEARCH_NODES
        # if len(Config.ELASTICSEARCH_NODES) > 1 and Config.ELASTICSEARCH_PORT:
        #     hosts = [dict(host=node, port=int(Config.ELASTICSEARCH_PORT))
        #              for node in Config.ELASTICSEARCH_NODES]

        # elif Config.ELASTICSEARCH_HOST and Config.ELASTICSEARCH_PORT:
        #     while 1:
        #         import socket

        #         # is a valid ip
        #         try:
        #             socket.inet_aton(Config.ELASTICSEARCH_HOST)
        #             hosts = [dict(host=Config.ELASTICSEARCH_HOST,
        #                           port=int(Config.ELASTICSEARCH_PORT))]
        #             break

        #         # resolve nameserver to list of ips
        #         except socket.error:
        #             try:
        #                 socket.getaddrinfo(Config.ELASTICSEARCH_HOST, Config.ELASTICSEARCH_PORT)
        #                 nr_host = set([i[4][0] for i in socket.getaddrinfo(Config.ELASTICSEARCH_HOST, Config.ELASTICSEARCH_PORT)])
        #                 hosts = [dict(host=h, port=int(Config.ELASTICSEARCH_PORT)) for h in nr_host ]
        #                 self.logger.info('Elasticsearch resolved to %i hosts: %s' %(len(hosts), hosts))
        #                 break
        #             except socket.gaierror:
        #                 wait_time = 5 * connection_attempt
        #                 self.logger.warn('Cannot resolve Elasticsearch to ip list. retrying in %i' % wait_time)
        #                 self.logger.warn('/etc/resolv.conf file: content: \n%s'%file('/etc/resolv.conf').read())
        #                 time.sleep(wait_time)
        #                 if connection_attempt >= 3:
        #                     self.logger.error('Elasticsearch is not resolvable at %s' % Config.ELASTICSEARCH_URL)
        #                     break
        #                 connection_attempt+=1
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
                        raise ConnectionTimeout("Couldn't connect to %s after 3 tries" % Config.ELASTICSEARCH_NODES)
                    connection_attempt += 1
                self.logger.info('Connected to elasticsearch nodes: %s', Config.ELASTICSEARCH_NODES)
                success = True
            except ConnectionTimeout as e:
                self.logger.exception(e)

        else:
            self.logger.warn('No valid configuration available for elasticsearch')
            self.es = None

        if not redispersist:
            self.clear_redislite_db()
            self.logger.info('Clearing previous instances of redislite db...')
        self.r_server = Redis(dbfilename=str(Config.REDISLITE_DB_PATH),
                              serverconfig={'save': [], 'maxclients': 10000})
        self.logger.info('Established redislite DB at %s', Config.REDISLITE_DB_PATH)

        return success
