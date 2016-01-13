from collections import defaultdict
from datetime import datetime, time
import logging
from elasticsearch.exceptions import NotFoundError
from elasticsearch.helpers import streaming_bulk, parallel_bulk
from sqlalchemy import and_
from common import Actions
from common.PGAdapter import ElasticsearchLoad
from common.processify import processify
from settings import ElasticSearchConfiguration, Config



class ESQuery(object):

    def __init__(self, es):
        self.handler = es


    def get_all_targets(self, fields = None):
        if fields is None:
            fields = ['*']

        res = self.handler.search(index=Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                                  doc_type=Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                                  body={"query": {
                                          "match_all": {}
                                        },
                                       'fields': fields,
                                       'size': int(1e5),
                                       }
                                  )
        for hit in res['hits']['hits']:
            yield hit

    def get_all_diseases(self, fields = None):
        res = self.handler.search(index=Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME,
                                  doc_type=Config.ELASTICSEARCH_EFO_LABEL_DOC_NAME,
                                  body={"query": {
                                          "match_all": {}
                                        },
                                       'fields': fields,
                                       'size': int(1e5),
                                       }
                                  )
        for hit in res['hits']['hits']:
            yield hit