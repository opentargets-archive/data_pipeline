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

class AssociationSummary(object):

    def __init__(self, res):
        self.top_associations = []
        self.total_associations = 0
        if res['hits']['total']:
            self.total_associations = 0
            self.top_associations = [hit['_source'] for hit in res['hits']['hits']]



class ESQuery(object):

    def __init__(self, es):
        self.handler = es


    def get_all_targets(self, fields = None):#TODO: Reimplement with scan method and a small chunksize to lower memory fingerprint
        if fields is None:
            fields = ['*']
        source =  {"include": fields},
        res = self.handler.search(index=Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                                  doc_type=Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                                  body={"query": {
                                          "match_all": {}
                                        },
                                       '_source': source,
                                       'size': int(1e5),
                                       }
                                  )
        for hit in res['hits']['hits']:
            yield hit['_source']

    def get_all_diseases(self, fields = None):#TODO: Reimplement with scan method and a small chunksize to lower memory fingerprint
        if fields is None:
            fields = ['*']
        source =  {"include": fields},
        res = self.handler.search(index=Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME,
                                  doc_type=Config.ELASTICSEARCH_EFO_LABEL_DOC_NAME,
                                  body={"query": {
                                          "match_all": {}
                                        },
                                       '_source': source,
                                       'size': int(1e5),
                                       }
                                  )
        for hit in res['hits']['hits']:
            yield hit['_source']


    def get_associations_for_target(self, target):
        res = self.handler.search(index=Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME,
                                  doc_type=Config.ELASTICSEARCH_DATA_ASSOCIATION_DOC_NAME,
                                  body={"query": {
                                          "filtered": {
                                              "filter": {
                                                   "terms": {"target.id": [target]}
                                              }
                                          }
                                        },
                                       '_source': True,
                                       'size': 100,
                                       }
                                  )
        return AssociationSummary(res)

    def get_associations_for_disease(self, disease):
        res = self.handler.search(index=Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME,
                                  doc_type=Config.ELASTICSEARCH_DATA_ASSOCIATION_DOC_NAME,
                                  body={"query": {
                                          "filtered": {
                                              "filter": {
                                                   "terms": {"disease.id": [disease]}
                                              }
                                          }
                                        },
                                       '_source': True,
                                       'size': 100,
                                       }
                                  )
        return AssociationSummary(res)