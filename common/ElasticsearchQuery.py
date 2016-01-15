from collections import defaultdict
from datetime import datetime, time
import logging

from elasticsearch import helpers
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
        self.top_associations_ids = []
        self.total_associations = 0
        if res['hits']['total']:
            for hit in res['hits']['hits']:
                if 'cttv_root' not in hit['_id']:
                    if '_source' in hit:
                        self.top_associations.append(hit['_source'])
                    elif 'fields' in hit:
                        self.top_associations.append(hit['fields'])
                    self.top_associations_ids.append(hit['_id'])
            self.total_associations = len(self.top_associations_ids)




class ESQuery(object):

    def __init__(self, es):
        self.handler = es


    def get_all_targets(self, fields = None):
        if fields is None:
            fields = ['*']
        source =  {"include": fields},


        res = helpers.scan(client=self.handler,
                            query={"query": {
                                      "match_all": {}
                                    },
                                   '_source': source,
                                   'size': 100,
                                   },
                            scroll='1h',
                            doc_type=Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                            index=Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                            timeout="10m",
                            )
        for hit in res:
            yield hit['_source']

    def get_all_diseases(self, fields = None):
        # if fields is None:
        #     fields = ['*']
        # source =  {"include": fields},

        res = helpers.scan(client=self.handler,
                            query={"query": {
                                      "match_all": {}
                                    },
                                   'fields': fields,
                                   'size': 100,
                                   },
                            scroll='1h',
                            doc_type=Config.ELASTICSEARCH_EFO_LABEL_DOC_NAME,
                            index=Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME,
                            timeout="10m",
                            )
        for hit in res:
            yield hit['_source']


    def get_associations_for_target(self, target, fields = None):
        res = self.handler.search(index=Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME,
                                  doc_type=Config.ELASTICSEARCH_DATA_ASSOCIATION_DOC_NAME,
                                  body={"query": {
                                          "filtered": {
                                              "filter": {
                                                   "terms": {"target.id": [target]}
                                              }
                                          }
                                        },
                                       "sort" : [{ "haromic-sum.overall" : "desc" }],
                                       'fields': fields,
                                       'size': 100,
                                       }
                                  )
        return AssociationSummary(res)

    def get_associations_for_disease(self, disease, fields = None):
        res = self.handler.search(index=Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME,
                                  doc_type=Config.ELASTICSEARCH_DATA_ASSOCIATION_DOC_NAME,
                                  body={"query": {
                                          "filtered": {
                                              "filter": {
                                                   "terms": {"disease.id": [disease]}
                                              }
                                          }
                                        },
                                       "sort" : [{ "haromic-sum.overall" : "desc" }],
                                       'fields': fields,
                                       'size': 100,
                                       }
                                  )
        return AssociationSummary(res)