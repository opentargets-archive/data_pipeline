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

