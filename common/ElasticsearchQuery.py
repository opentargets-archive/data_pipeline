from collections import defaultdict
from datetime import datetime, time
import logging
from pprint import pprint

import jsonpickle
from elasticsearch import helpers
from elasticsearch.exceptions import NotFoundError
from elasticsearch.helpers import streaming_bulk, parallel_bulk
from sqlalchemy import and_
from common import Actions
from common.DataStructure import SparseFloatDict
from common.ElasticsearchLoader import Loader
from common.PGAdapter import ElasticsearchLoad
from common.processify import processify
from settings import Config
from elasticsearch_config import ElasticSearchConfiguration


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

    @staticmethod
    def _get_source_from_fields(fields = None):
        if fields is None:
            fields = ['*']
        source = {"include": fields}
        return source

    def get_all_targets(self, fields = None):
        source = self._get_source_from_fields(fields)
        res = helpers.scan(client=self.handler,
                            query={"query": {
                                      "match_all": {}
                                    },
                                   '_source': source,
                                   'size': 100,
                                   },
                            scroll='12h',
                            doc_type=Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                            index=Loader.get_versioned_index(Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME),
                            timeout="30m",
                            )
        for hit in res:
            yield hit['_source']

    def get_all_diseases(self, fields = None):
        source = self._get_source_from_fields(fields)

        res = helpers.scan(client=self.handler,
                            query={"query": {
                                      "match_all": {}
                                    },
                                   '_source': source,
                                   'size': 1000,
                                   },
                            scroll='12h',
                            doc_type=Config.ELASTICSEARCH_EFO_LABEL_DOC_NAME,
                            index=Loader.get_versioned_index(Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME),
                            timeout="10m",
                            )
        for hit in res:
            yield hit['_source']


    def get_all_eco(self, fields=None):
        source = self._get_source_from_fields(fields)

        res = helpers.scan(client=self.handler,
                           query={"query": {
                               "match_all": {}
                           },
                               '_source': source,
                               'size': 1000,
                           },
                           scroll='2h',
                           doc_type=Config.ELASTICSEARCH_ECO_DOC_NAME,
                           index=Loader.get_versioned_index(Config.ELASTICSEARCH_ECO_INDEX_NAME),
                           timeout="10m",
                           )
        for hit in res:
            yield hit['_source']

    def get_associations_for_target(self, target, fields = None, size = 100, get_top_hits = True):
        source = self._get_source_from_fields(fields)
        aggs ={}
        if get_top_hits:
            aggs = {
                       "direct_associations": {
                           "filter": {"term": {"is_direct": True}},
                           'aggs': {
                               "top_direct_ass": {
                                   "top_hits": {
                                       "sort": {"harmonic-sum.overall": {"order": "desc"}},
                                       "_source": source,

                                       "size": size
                                   },
                               }
                           },
                       }
                   }

        res = self.handler.search(index=Loader.get_versioned_index(Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME),
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
                                       "aggs" : aggs,
                                       'size': size,
                                       }
                                  )
        return AssociationSummary(res)

    def get_associations_for_disease(self, disease, fields = None, size = 100, get_top_hits = True):
        source = self._get_source_from_fields(fields)
        aggs = {}
        if get_top_hits:
            aggs = {
                "direct_associations": {
                    "filter": {"term": {"is_direct": True}},
                    'aggs': {
                        "top_direct_ass": {
                            "top_hits": {
                                "sort": {"harmonic-sum.overall": {"order": "desc"}},
                                "_source": source,

                                "size": size
                            },
                        }
                    },
                }
            }

        res = self.handler.search(index=Loader.get_versioned_index(Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME),
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
                                       "aggs" : aggs,
                                       'size': size,
                                       }
                                  )
        return AssociationSummary(res)


    def get_validated_evidence_strings(self, fields = None):
        source = self._get_source_from_fields(fields)

        res = helpers.scan(client=self.handler,
                           query={"query": {
                               "match_all": {}
                           },
                               '_source': source,
                               'size': 10000,
                           },
                           scroll='12h',
                           # doc_type=Config.ELASTICSEARCH_VALIDATED_DATA_DOC_NAME,
                           index=Loader.get_versioned_index(Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME+'*'),
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

    def get_validated_evidence_strings_count(self,):

        res = self.handler.search(index=Loader.get_versioned_index(Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME+'*'),
                                  body={"query": {
                                      "match_all": {}
                                        },
                                       '_source': False,
                                      'size':0,
                                       }
                                  )
        return res['hits']['total']


    def get_all_ensembl_genes(self):
        res = helpers.scan(client=self.handler,
                           query={"query": {
                               "match_all": {}
                           },
                               '_source': True,
                               'size': 1000,
                           },
                           scroll='1h',
                           # doc_type=Config.ELASTICSEARCH_VALIDATED_DATA_DOC_NAME,
                           index=Loader.get_versioned_index(Config.ELASTICSEARCH_ENSEMBL_INDEX_NAME),
                           timeout="10m",
                           )
        for hit in res:
            yield hit['_source']

    def get_all_uniprot_entries(self):
        res = helpers.scan(client=self.handler,
                           query={"query": {
                               "match_all": {}
                           },
                               '_source': True,
                               'size': 100,
                           },
                           scroll='12h',
                           # doc_type=Config.ELASTICSEARCH_VALIDATED_DATA_DOC_NAME,
                           index=Loader.get_versioned_index(Config.ELASTICSEARCH_UNIPROT_INDEX_NAME),
                           timeout="10m",
                           )
        for hit in res:
            yield jsonpickle.loads(hit['_source']['entry'])


    def get_reaction(self, reaction_id):
        res = self.handler.search(index=Loader.get_versioned_index(Config.ELASTICSEARCH_REACTOME_INDEX_NAME),
                                  doc_type=Config.ELASTICSEARCH_REACTOME_REACTION_DOC_NAME,
                                  body={"query": {
                                            "ids" : {
                                                "values" : [reaction_id]
                                              }
                                          },
                                          '_source': True,
                                          'size': 1,
                                      }
                                  )
        for hit in res['hits']['hits']:
            return hit['_source']

    @property
    def get_disease_to_targets_vectors(self):
        #TODO: look at the multiquery api

        res = helpers.scan(client=self.handler,
                           query={"query": {
                               "term": {
                                   "is_direct": True,
                               }
                           },
                               '_source': {'include':["target.id", 'disease.id', 'harmonic-sum.overall']},
                               'size': 1000,
                           },
                           scroll='12h',
                           index=Loader.get_versioned_index(Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME),
                           timeout="10m",
                           )

        target_results = dict()
        disease_results = dict()

        c=0
        for hit in res:
            c+=1
            hit = hit['_source']
            '''store target associations'''
            if hit['target']['id'] not in target_results:
                target_results[hit['target']['id']] = SparseFloatDict()
            target_results[hit['target']['id']][hit['disease']['id']]=hit['harmonic-sum']['overall']

            '''store disease associations'''
            if hit['disease']['id'] not in disease_results:
                disease_results[hit['disease']['id']] = SparseFloatDict()
            disease_results[hit['disease']['id']][hit['target']['id']] = hit['harmonic-sum']['overall']

            if c%10000 ==0:
                print c

        return target_results, disease_results

    def get_target_labels(self, ids):
        res = helpers.scan(client=self.handler,
                           query={"query": {
                               "ids": {
                                   "values": ids,
                               }
                           },
                               '_source': 'approved_symbol',
                               'size': 1,
                           },
                          scroll='12h',
                          index=Loader.get_versioned_index(Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME),
                          timeout="10m",
                          )



        return dict((hit['_id'],hit['_source']['approved_symbol']) for hit in res)

    def get_disease_labels(self, ids):
        res = helpers.scan(client=self.handler,
                           query={"query": {
                               "ids": {
                                   "values": ids,
                               }
                           },
                               '_source': 'label',
                               'size': 1,
                           },
                           scroll='12h',
                           index=Loader.get_versioned_index(Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME),
                           timeout="10m",
                           )

        return dict((hit['_id'],hit['_source']['label']) for hit in res)

