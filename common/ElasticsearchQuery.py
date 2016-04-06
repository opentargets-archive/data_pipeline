from collections import defaultdict
from datetime import datetime, time
import logging
from pprint import pprint

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
        self.top_associations = dict(total = [], direct =[])
        self.associations_count = dict(total = 0, direct =0)
        if res['hits']['total']:
            self.associations_count['total'] = res['hits']['total']
            for hit in res['hits']['hits']:
                if '_source' in hit:
                    self.top_associations['total'].append(hit['_source'])
                elif 'fields' in hit:
                    self.top_associations['total'].append(hit['fields'])
            self.associations_count['direct'] = res['aggregations']['direct_associations']['doc_count']
            for hit in res['aggregations']['direct_associations']['top_direct_ass']['hits']['hits']:
                if '_source' in hit:
                    self.top_associations['direct'].append(hit['_source'])
                elif 'fields' in hit:
                    self.top_associations['direct'].append(hit['fields'])





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


    def get_associations_for_target(self, target, fields = None, size = 100):
        source =  {"include": fields}
        res = self.handler.search(index=Config().get_versioned_index(Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME),
                                  doc_type=Config.ELASTICSEARCH_DATA_ASSOCIATION_DOC_NAME,
                                  body={"query": {
                                          "filtered": {
                                              "filter": {
                                                   "terms": {"target.id": [target]}
                                              }
                                          }
                                        },
                                       "sort" : { "harmonic-sum.overall" : {"order":"desc" }},
                                       '_source': source,
                                       "aggs" : {
                                            "direct_associations" : {
                                                "filter" : { "term": { "is_direct": True}},
                                                'aggs':{
                                                   "top_direct_ass": {
                                                        "top_hits": {
                                                            "sort" : { "harmonic-sum.overall" : {"order":"desc" }},
                                                            "_source": source,

                                                        "size" : size
                                                        },
                                                   }
                                                },
                                            }
                                       },
                                       'size': size,
                                       }
                                  )
        return AssociationSummary(res)

    def get_associations_for_disease(self, disease, fields = None, size = 100):
        source =  {"include": fields}

        res = self.handler.search(index=Config().get_versioned_index(Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME),
                                  doc_type=Config.ELASTICSEARCH_DATA_ASSOCIATION_DOC_NAME,
                                  body={"query": {
                                          "filtered": {
                                              "filter": {
                                                   "terms": {"disease.id": [disease]}
                                              }
                                          }
                                        },
                                       "sort" : { "harmonic-sum.overall" : {"order":"desc"}},
                                       '_source': source,
                                       "aggs" : {
                                            "direct_associations" : {
                                                "filter" : { "term": {"is_direct": True}},
                                                'aggs':{
                                                   "top_direct_ass": {
                                                        "top_hits": {
                                                            "sort" : { "harmonic-sum.overall" : {"order":"desc" }},
                                                            "_source": source,

                                                        "size" :size
                                                        },
                                                   }
                                                },
                                            }
                                       },
                                       'size': size,
                                       }
                                  )
        return AssociationSummary(res)


    def get_validated_evidence_strings(self):

        res = helpers.scan(client=self.handler,
                           query={"query": {
                               "match_all": {}
                           },
                               '_source': True,
                               'size': 1000,
                           },
                           scroll='1h',
                           # doc_type=Config.ELASTICSEARCH_VALIDATED_DATA_DOC_NAME,
                           index=Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME,
                           timeout="10m",
                           )

        # res = self.handler.search(index=Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME,
        #                           body={"query": { "match_all": {}},
        #                                  '_source': True,
        #                                  'size': 10,
        #                                 }
        #
        #                           )

        for hit in res:
            yield hit['_source']