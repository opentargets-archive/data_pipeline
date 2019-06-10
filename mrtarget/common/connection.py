import logging
import time
import os
import tempfile as tmp
from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection

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
