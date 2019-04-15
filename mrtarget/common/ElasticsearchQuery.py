import base64
import collections
import json
import logging
import time
from collections import Counter

import addict
import jsonpickle
from elasticsearch import helpers, TransportError
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MatchAll

from mrtarget.Settings import Config
from mrtarget.constants import Const
from mrtarget.common.DataStructure import SparseFloatDict
from mrtarget.common.ElasticsearchLoader import Loader


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

    def __init__(self, es, dry_run = False):
        self.handler = es
        self.dry_run = dry_run
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def _get_source_from_fields(fields = None):
        if not fields:
            return True
        return fields

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
                            doc_type=Const.ELASTICSEARCH_EFO_LABEL_DOC_NAME,
                            index=Loader.get_versioned_index(Const.ELASTICSEARCH_EFO_LABEL_INDEX_NAME,True),
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
                           doc_type=Const.ELASTICSEARCH_ECO_DOC_NAME,
                           index=Loader.get_versioned_index(Const.ELASTICSEARCH_ECO_INDEX_NAME,True),
                           timeout="10m",
                           )
        for hit in res:
            yield hit['_source']

    def get_all_hpa(self, fields=None):
        source = self._get_source_from_fields(fields)

        res = helpers.scan(client=self.handler,
                           query={"query": {
                               "match_all": {}
                            },
                            '_source': source,
                            'size': 100,
                           },
                           scroll='12h',
                           doc_type=Const.ELASTICSEARCH_EXPRESSION_DOC_NAME,
                           index=Loader.get_versioned_index(
                               Const.ELASTICSEARCH_EXPRESSION_INDEX_NAME, True),
                           timeout="10m")
        for hit in res:
            yield hit['_source']

    def get_associations_for_target(self, target, fields = None, size = 100):
        source = self._get_source_from_fields(fields)

        aggs = addict.Dict()
        aggs.direct_associations.filter.term.is_direct = True
        aggs.direct_associations.aggs.top_direct_ass.top_hits.sort['harmonic-sum.overall'].order = 'desc'
        aggs.direct_associations.aggs.top_direct_ass.top_hits._source = source
        aggs.direct_associations.aggs.top_direct_ass.top_hits.size = size

        q = addict.Dict()
        q.query.constant_score.filter.terms['target.id'] = [target]
        q.sort['harmonic-sum.overall'].order = 'desc'
        q._source = source
        q.aggs = aggs
        q.size = size

        res = self.handler.search(index=Loader.get_versioned_index(Const.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME,True),
                                  doc_type=Const.ELASTICSEARCH_DATA_ASSOCIATION_DOC_NAME,
                                  body=q.to_dict()
                                  )
        return AssociationSummary(res)

    def get_associations_for_disease(self, disease, fields = None, size = 100):
        source = self._get_source_from_fields(fields)

        aggs = addict.Dict()
        aggs.direct_associations.filter.term.is_direct = True
        aggs.direct_associations.aggs.top_direct_ass.top_hits.sort['harmonic-sum.overall'].order = 'desc'
        aggs.direct_associations.aggs.top_direct_ass.top_hits._source = source
        aggs.direct_associations.aggs.top_direct_ass.top_hits.size = size

        q = addict.Dict()
        q.query.constant_score.filter.terms['disease.id'] = [disease]
        q.sort['harmonic-sum.overall'].order = 'desc'
        q._source = source
        q.aggs = aggs
        q.size = size

        res = self.handler.search(index=Loader.get_versioned_index(Const.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME,True),
                                  doc_type=Const.ELASTICSEARCH_DATA_ASSOCIATION_DOC_NAME,
                                  body=q.to_dict()
                                  )
        return AssociationSummary(res)

    def get_all_ensembl_genes(self):
        res = helpers.scan(client=self.handler,
                           query={"query": {
                               "match_all": {}
                           },
                               '_source': True,
                               'size': 1000,
                           },
                           scroll='1h',
                           index=Loader.get_versioned_index(Const.ELASTICSEARCH_ENSEMBL_INDEX_NAME,True),
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
                           index=Loader.get_versioned_index(Const.ELASTICSEARCH_UNIPROT_INDEX_NAME,True),
                           timeout="10m",
                           )
        for hit in res:
            yield jsonpickle.decode(base64.b64decode(hit['_source']['entry']))


    def get_reaction(self, reaction_id):
        res = self.handler.search(index=Loader.get_versioned_index(Const.ELASTICSEARCH_REACTOME_INDEX_NAME,True),
                                  doc_type=Const.ELASTICSEARCH_REACTOME_REACTION_DOC_NAME,
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

    def get_disease_to_targets_vectors(self,
                                       treshold=0.1,
                                       evidence_count = 3):
        '''
        Get all the association objects that are:
        - direct -> to avoid ontology inflation
        - > 3 evidence count -> remove noise
        - overall score > threshold -> remove very lo quality noise
        :param treshold: minimum overall score threshold to consider for fetching association data
        :param evidence_count: minimum number of evidence consider for fetching association data
        :return: two dictionaries mapping target to disease  and the reverse
        '''
        self.logger.debug('scan es to get all diseases and targets')
        res = helpers.scan(client=self.handler,
                           query={"query": {
                               "term": {
                                   "is_direct": True,
                               }
                           },
                               '_source': {'includes':["target.id", 'disease.id', 'harmonic-sum', 'evidence_count']},
                               'size': 1000,
                           },
                           scroll='12h',
                           index=Loader.get_versioned_index(Const.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME,True),
                           timeout="10m",
                           )

        target_results = dict()
        disease_results = dict()

        self.logger.debug('start getting all targets and diseases from es')
        c=0
        for hit in res:
            c+=1
            pair_id = str(hit['_id'])
            hit = hit['_source']
            if hit['evidence_count']['total']>=evidence_count and \
                hit['harmonic-sum']['overall'] >=treshold:
                '''store target associations'''
                if hit['target']['id'] not in target_results:
                    target_results[hit['target']['id']] = SparseFloatDict()
                #TODO: return all counts and scores up to datasource level
                target_results[hit['target']['id']][hit['disease']['id']]=hit['harmonic-sum']['overall']

                '''store disease associations'''
                if hit['disease']['id'] not in disease_results:
                    disease_results[hit['disease']['id']] = SparseFloatDict()
                # TODO: return all counts and scores up to datasource level
                disease_results[hit['disease']['id']][hit['target']['id']] = hit['harmonic-sum']['overall']

                if c%10000 == 0:
                    self.logger.debug('%d elements retrieved', c)
            else:
                self.logger.debug('Not enough evidences or too low harmonic-sum score for pair: %s ' % pair_id)


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
                          index=Loader.get_versioned_index(Const.ELASTICSEARCH_GENE_NAME_INDEX_NAME,True),
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
                           index=Loader.get_versioned_index(Const.ELASTICSEARCH_EFO_LABEL_INDEX_NAME,True),
                           timeout="10m",
                           )

        return dict((hit['_id'],hit['_source']['label']) for hit in res)

    def count_elements_in_index(self, index_name, doc_type=None, query = None):
        if query is None:
            query =  {"match_all": {}}
        res = self.handler.search(index=Loader.get_versioned_index(index_name,True),
                                  doc_type=doc_type,
                                  body={"query": query,
                                      '_source': False,
                                      'size': 0,
                                  }
                                  )
        return res['hits']['total']

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
                           doc_type=Const.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                           index=Loader.get_versioned_index(Const.ELASTICSEARCH_GENE_NAME_INDEX_NAME,True),
                           timeout="30m",
                           )
        for target in res:
            yield  target['_id']


    def get_evidence_for_target_simple(self, target, expected = None):
        query_body = {
            "query": {
                "constant_score": {
                    "filter": {
                        "term": {
                            "target.id": target
                        }
                    }
                }
            },
            '_source': {
                "includes": ["target.id",
                             "private.efo_codes",
                             "disease.id",
                             "scores.association_score",
                             "sourceID",
                             "id",
                             ]},
        }

        if expected is not None and expected <10000:
            query_body['size']=10000
            res = self.handler.search(index=Loader.get_versioned_index(Const.ELASTICSEARCH_DATA_INDEX_NAME,True),
                                      body=query_body
                                      )
            for hit in res['hits']['hits']:
                yield hit['_source']
        else:
            res = helpers.scan(client=self.handler,
                               query=query_body,
                               scroll='1h',
                               index=Loader.get_versioned_index(Const.ELASTICSEARCH_DATA_INDEX_NAME,True),
                               timeout="1h",
                               request_timeout=2 * 60 * 60,
                               size=1000
                               )
            for hit in res:
                yield hit['_source']

    def count_evidence_for_target(self, target):
        res = self.handler.search(index=Loader.get_versioned_index(Const.ELASTICSEARCH_DATA_INDEX_NAME, True),
                                  body={
                                        "query": {
                                            "constant_score": {
                                              "filter": {
                                                "term": {
                                                  "target.id": target
                                                }
                                              }
                                            }
                                        },
                                        '_source': [],
                                        'size': 0
                                    }
                                  )
        return res['hits']['total']

    def get_objects_by_id(self,
                          ids,
                          index,
                          doc_type,
                          source = True,
                          source_exclude=[],
                          realtime = False):
        '''

        :param ids: list of idientifiers for documents
        :param index: index for all the documents
        :param doc_type: doc type for all the documents
        :return: generator of documents
        '''
        if isinstance(ids, (list, tuple)):
            res = self.handler.mget(index=Loader.get_versioned_index(index,True),
                                    doc_type=doc_type,
                                    body=dict(ids=ids),
                                    _source=source,
                                    _source_exclude=source_exclude,
                                    realtime=True,
                                    )
            if not res:
                time.sleep(0.1)
                res = self.handler.mget(index=Loader.get_versioned_index(index,True),
                                        doc_type=doc_type,
                                        body=dict(ids=ids),
                                        _source=source,
                                        _source_exclude=source_exclude,
                                        realtime=True,
                                        )
            for doc in res['docs']:
                if doc['found']:
                    yield doc['_source']
                else:
                    raise KeyError('object with id %s not found' % (doc['_id']))

        else:

            try:
                res = self.handler.get(index=Loader.get_versioned_index(index, True),
                                       doc_type=doc_type,
                                       id=ids,
                                       _source=source,
                                       _source_exclude=source_exclude,
                                       realtime=True,
                                       )
                try:
                    yield res['_source']
                except Exception as e:
                    self.logger.exception('cannot retrieve single object by id %s ' % ids)
                    raise KeyError('object with id %s not found' % ids)

            except TransportError as te:
                if te.status_code == 404:
                    raise KeyError('object with id %s not found' % ids)


    def get_all_associations(self,):
        res = helpers.scan(client=self.handler,
                           query={"query": {
                               "match_all": {}
                           },
                               '_source': True,
                               'size': 1000,
                           },
                           scroll='1h',
                           index=Loader.get_versioned_index(Const.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME,True),
                           timeout="10m",
                           )
        for hit in res:
            yield hit['_source']

    def get_all_evidence(self, fields = None):
        index_name = Loader.get_versioned_index(Const.ELASTICSEARCH_DATA_INDEX_NAME, True)
        doc_type = None
        res = helpers.scan(client=self.handler,
                           query={"query":  {"match_all": {}},
                               '_source': self._get_source_from_fields(fields),
                               'size': 1000,
                           },
                           scroll='12h',
                           index=index_name,
                           timeout="10m",
                           )

        # res = list(res)
        for hit in res:
            yield hit['_source']



    def get_all_evidence_for_datatype(self, datatype, fields = None, ):
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-multi-get.html
        index_name = Loader.get_versioned_index(Const.ELASTICSEARCH_DATA_INDEX_NAME, True)
        res = helpers.scan(client=self.handler,
            query={
                "query": {
                    "match": {
                        "type": datatype
                    }
                },
                    '_source': self._get_source_from_fields(fields),
                    'size': 1000,
                },
            scroll='12h',
            index=index_name,
            timeout="10m",
            )

        # res = list(res)
        for hit in res:
            yield hit['_source']

    def exists(self, index, doc_type, id,realtime = False):
        return self.handler.exists(index = Loader.get_versioned_index(index,True),
                                   doc_type =doc_type,
                                   id = id,
                                   realtime = realtime)
