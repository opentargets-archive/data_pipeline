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
                                   'size': 20,
                                   },
                            scroll='12h',
                            doc_type=Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                            index=Loader.get_versioned_index(Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME),
                            timeout="30m",
                            )
        for hit in res:
            yield hit['_source']
    
    def count_all_targets(self):

        return self.count_elements_in_index(Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME)

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

    def count_all_diseases(self):

        return self.count_elements_in_index(Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME)


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

    def count_all_eco(self):

        return self.count_elements_in_index(Config.ELASTICSEARCH_ECO_INDEX_NAME)

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


    def get_validated_evidence_strings(self, fields = None, size=1000, datasources = []):


        source = self._get_source_from_fields(fields)

        # TODO: do a scroll to get all the ids without sorting, and use many async mget queries to fetch the sources
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-multi-get.html
        index_name = Loader.get_versioned_index(Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME+'*')
        doc_type = None

        if datasources:
            doc_type = datasources
        res = helpers.scan(client=self.handler,
                           query={"query":  {"match_all": {}},
                               '_source': source,
                               'size': 1000,
                           },
                           scroll='12h',
                           doc_type=doc_type,
                           index=index_name,
                           timeout="10m",
                           )


        for hit in res:
            yield hit['_source']

    def count_validated_evidence_strings(self, datasources = []):

        doc_type = None
        if datasources:
            doc_type = datasources

        return self.count_elements_in_index(Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME + '*',
                                            doc_type = doc_type)


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

    def count_elements_in_index(self, index_name, doc_type=None):
        res = self.handler.search(index=Loader.get_versioned_index(index_name),
                                  doc_type=doc_type,
                                  body={"query": {
                                      "match_all": {}
                                  },
                                      '_source': False,
                                      'size': 0,
                                  }
                                  )
        return res['hits']['total']

    def get_evidence_simple(self, targets = None):

        def get_ids(ids):
            return self.handler.mget(index=Loader.get_versioned_index(Config.ELASTICSEARCH_DATA_INDEX_NAME + '*'),
                                   body={'docs': ids},
                                   _source= {"include": ["target.id",
                                                        "private.efo_codes",
                                                        "disease.id",
                                                        "scores.association_score",
                                                        "sourceID",
                                                        "id",
                                                        ],
                                            })

        if targets is None:
            targets = self.get_all_target_ids_with_evidence_data()


        for target in targets:
            query_body = {
                "query": { "filtered": {
                                       "filter": {
                                           "terms": {"target.id": target}
                                       }
                                   }},
                '_source':  {"include": ["target.id",
                                        "disease.id",
                            ]},
                "sort": ["target.id", "disease.id"],
            }

            res = helpers.scan(client=self.handler,
                               query=query_body,
                               scroll='1h',
                               index=Loader.get_versioned_index(Config.ELASTICSEARCH_DATA_INDEX_NAME + '*'),
                               timeout="1h",
                               request_timeout=2 * 60 * 60,
                               size=5000,
                               preserve_order=True
                               )
            ids = []
            for hit in res:
                ids.append({"_index": hit["_index"],
                                "_id" : hit["_id"]
                                },)
            id_buffer = []
            for doc_id in ids:
                id_buffer.append(doc_id)
                if len(id_buffer) == 1000:
                    res_get = get_ids(id_buffer)
                    for doc in res_get['docs']:
                        if doc['found']:
                            yield doc['_source']
                        else:
                            raise ValueError('document with id %s not found'%(doc['_id']))
                    id_buffer = []
            if id_buffer:
                res_get = get_ids(id_buffer)
                for doc in res_get['docs']:
                    if doc['found']:
                        yield doc['_source']
                    else:
                        raise ValueError('document with id %s not found' % (doc['_id']))


    def get_all_target_ids_with_evidence_data(self):
        #TODO: use an aggregation to get those with just data
        res = helpers.scan(client=self.handler,
                           query={"query": {
                               "match_all": {}
                           },
                               '_source': False,
                               'size': 100,
                           },
                           scroll='12h',
                           doc_type=Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                           index=Loader.get_versioned_index(Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME),
                           timeout="30m",
                           )
        for target in res:
            yield  target['_id']


    def get_evidence_for_target_simple(self, target, expected = None):
        query_body = {"query": {
                                "bool": {
                                  "filter": {
                                    "term": {
                                      "target.id": target
                                    }
                                  }
                                }
                            },
            '_source': {"include": ["target.id",
                                    "private.efo_codes",
                                    "disease.id",
                                    "scores.association_score",
                                    "sourceID",
                                    "id",
                                    ]},
            }

        if expected is not None and expected <10000:
            query_body['size']=10000
            res = self.handler.search(index=Loader.get_versioned_index(Config.ELASTICSEARCH_DATA_INDEX_NAME + '*'),
                                      body=query_body,
                                      routing=target,
                                      )
            for hit in res['hits']['hits']:
                yield hit['_source']
        else:
            res = helpers.scan(client=self.handler,
                               query=query_body,
                               scroll='1h',
                               index=Loader.get_versioned_index(Config.ELASTICSEARCH_DATA_INDEX_NAME + '*'),
                               timeout="1h",
                               request_timeout=2 * 60 * 60,
                               size=1000,
                               routing=target,
                               )
            for hit in res:
                yield hit['_source']

    def count_evidence_for_target(self, target):
        res = self.handler.search(index=Loader.get_versioned_index(Config.ELASTICSEARCH_DATA_INDEX_NAME + '*'),
                                  body={
                                        "query": {
                                            "bool": {
                                              "filter": {
                                                "term": {
                                                  "target.id": target
                                                }
                                              }
                                            }
                                        },
                                        '_source': False,
                                        'size': 0,
                                    },
                                  routing=target,
                                  )
        return res['hits']['total']


    def get_publications_by_id(self, ids):
        query_body = {"query": {
                               "ids": {
                                   "values": ids,
                               }
                           },
                               '_source': True,
                               'size': 100,
                           }
        if len(ids) <10000:
            query_body['size']=10000
            res = self.handler.search(index=Loader.get_versioned_index(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME),
                                      doc_type = Config.ELASTICSEARCH_PUBLICATION_DOC_NAME,
                                      body=query_body,
                                      )
            for hit in res['hits']['hits']:
                yield hit['_source']
        else:
            res = helpers.scan(client=self.handler,
                               query=query_body,
                               scroll='12h',
                               index=Loader.get_versioned_index(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME),
                               doc_type=Config.ELASTICSEARCH_PUBLICATION_DOC_NAME,
                               timeout="10m",
                               )
            for hit in res:
                yield hit['_source']


    def get_all_pub_ids_from_evidence(self,):
        #TODO: get all the validated evidencestrings and fetch medline abstracts there
        #USE THIS INSTEAD: self.es_query.get_validated_evidence_strings(datasources=datasources, fields='literature.references.lit_id'

        return ['http://europepmc.org/abstract/MED/24523595',
               'http://europepmc.org/abstract/MED/26784250',
               'http://europepmc.org/abstract/MED/27409410',
               'http://europepmc.org/abstract/MED/26290144',
               'http://europepmc.org/abstract/MED/25787843',
               'http://europepmc.org/abstract/MED/26836588',
               'http://europepmc.org/abstract/MED/26781615',
               'http://europepmc.org/abstract/MED/26646452',
               'http://europepmc.org/abstract/MED/26774881',
               'http://europepmc.org/abstract/MED/26629442',
               'http://europepmc.org/abstract/MED/26371324',
               'http://europepmc.org/abstract/MED/24817865',
               ]
