import logging
import os

import time

from SPARQLWrapper import SPARQLWrapper
from elasticsearch import Elasticsearch
from redislite import Redis

from settings import Config


class PipelineConnectors():

    def __init__(self):
        """Initialises the class

        Declares the connector parts
        """
        ''' Elastic Search connection'''
        self.es = None
        ''' sparql endpoint client'''
        self.sparql = None
        ''' redis '''
        self.r_server = None

        self.logger = logging.getLogger(__name__)


    def clear_redislite_db(self):
        if os.path.exists(Config.REDISLITE_DB_PATH):
            os.remove(Config.REDISLITE_DB_PATH)

    def init_services_connections(self, redispersist=False):
        '''init es client'''
        connection_attempt = 1
        hosts=[]
        #es = None
        if Config.ELASTICSEARCH_HOST and Config.ELASTICSEARCH_PORT:
            while 1:
                import socket
                try:#is a valid ip
                    socket.inet_aton(Config.ELASTICSEARCH_HOST)
                    hosts = [dict(host=Config.ELASTICSEARCH_HOST, port=Config.ELASTICSEARCH_PORT)]
                    break
                except socket.error:#resolve nameserver to list of ips
                    try:
                        socket.getaddrinfo(Config.ELASTICSEARCH_HOST, Config.ELASTICSEARCH_PORT)
                        nr_host = set([i[4][0] for i in socket.getaddrinfo(Config.ELASTICSEARCH_HOST, Config.ELASTICSEARCH_PORT)])
                        hosts = [dict(host=h, port=Config.ELASTICSEARCH_PORT) for h in nr_host ]
                        self.logger.info('Elasticsearch resolved to %i hosts: %s' %(len(hosts), hosts))
                        break
                    except socket.gaierror:
                        wait_time = 5 * connection_attempt
                        self.logger.warn('Cannot resolve Elasticsearch to ip list. retrying in %i' % wait_time)
                        self.logger.warn('/etc/resolv.conf file: content: \n%s'%file('/etc/resolv.conf').read())
                        time.sleep(wait_time)
                        if connection_attempt >= 3:
                            self.logger.error('Elasticsearch is not resolvable at %s' % Config.ELASTICSEARCH_URL)
                            break
                        connection_attempt+=1
            if hosts:
                self.es = Elasticsearch(hosts = hosts,
                                   maxsize=50,
                                   timeout=1800,
                                   sniff_on_connection_fail=True,
                                   retry_on_timeout=True,
                                   max_retries=10,
                                   )
                connection_attempt = 1
                while not self.es.ping():
                    wait_time = 3*connection_attempt
                    self.logger.warn('Cannot connect to Elasticsearch retrying in %i'%wait_time)
                    time.sleep(wait_time)
                    if connection_attempt >=3:
                        self.logger.error('Elasticsearch is not reachable at %s'%Config.ELASTICSEARCH_URL)
                        break
                    connection_attempt += 1
            else:
                self.es=None


        '''init sparql endpoint client'''
        self.sparql = SPARQLWrapper(Config.SPARQL_ENDPOINT_URL)


        if not redispersist:
            self.clear_redislite_db()
        self.r_server= Redis(Config.REDISLITE_DB_PATH, serverconfig={'save': [],
                                                                     'maxclients': 10000})
