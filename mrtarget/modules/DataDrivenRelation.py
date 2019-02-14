import logging
from collections import Counter
import sys, os
import numpy as np
import scipy.sparse as sp
from sklearn.feature_extraction import DictVectorizer
from sklearn.feature_extraction.text import TfidfTransformer, _document_frequency
from mrtarget.common.DataStructure import JSONSerializable
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.ElasticsearchQuery import ESQuery
from scipy.spatial.distance import pdist
import time
from copy import copy
import math
import functools
import itertools
from mrtarget.constants import Const
from mrtarget.Settings import Config
import pypeln.process as pr

class RelationType(object):
    SHARED_DISEASE = 'shared-disease'
    SHARED_TARGET = 'shared-target'


class Relation(JSONSerializable):

    def __init__(self,
                 subject,
                 object,
                 scores,
                 type,
                 **kwargs):
        self.subject = subject
        self.object = object
        self.scores = scores
        self.type = type

        #add any other arugments as appropriate
        self.__dict__.update(**kwargs)
        self.set_id()

    #create an identifier for this relation
    #used when the parts change
    def set_id(self):
        self.id = '-'.join([self.subject['id'], self.object['id']])

class T2TRelation(JSONSerializable):
    type = RelationType.SHARED_DISEASE

class D2DRelation(JSONSerializable):
    type = RelationType.SHARED_TARGET

class LocalTfidfTransformer(TfidfTransformer):

    def fit(self, X, y=None):
        """Learn the idf vector (global term weights)

        Parameters
        ----------
        X : sparse matrix, [n_samples, n_features]
            a matrix of term/token counts
        """
        if not sp.issparse(X):
            X = sp.csc_matrix(X)
        if self.use_idf:
            n_samples, n_features = X.shape
            n_samples=float(n_samples)
            df = _document_frequency(X)

            # log+1 instead of log makes sure terms with zero idf don't get
            # suppressed entirely.
            # idf = np.log(df / n_samples)
            idf = df / n_samples
            self._idf_diag = sp.spdiags(idf,
                diags=0, m=n_features, n=n_features)
        return self


class OverlapDistance(object):
    """  use overlap between nonzero elements
    can work with nearpy if subclassing Distance"""

    def __init__(self):
        '''remove this if subclassing nearpy Distance'''
        pass

    def distance(self, x, y):
        return self.compute_distance(x, y)[0]

    @staticmethod
    def compute_distance(x, y):
        """
        Computes a similarity measure between vectors x and y. Returns float.
        0 if no match, 1 if perfect match
        """

        if sp.issparse(x):
            x = x.toarray().ravel()
            y = y.toarray().ravel()

        x_nz = set(np.flatnonzero(x).flat)
        y_nz = set(np.flatnonzero(y).flat)
        xy_intersection = x_nz & y_nz
        if not xy_intersection:
            distance = 0
            xy_union=set()
        else:
            xy_union = x_nz | y_nz
            distance = math.sqrt(float(len(xy_intersection)) / len(xy_union))
        return distance, x_nz, y_nz, xy_intersection, xy_union

    @staticmethod
    def compute_weighted_distance(x, y, idf_):
        """
        Computes a similarity measure between vectors x and y using idf stats as weight. Returns float.
        0 if no match, 1 if perfect match

        idf_: inverted idf frequency (1 infrequent, 0 in all positions)
        """

        if sp.issparse(x):
            x = x.toarray().ravel()
            y = y.toarray().ravel()

        x_nz = set(np.flatnonzero(x).flat)
        y_nz = set(np.flatnonzero(y).flat)
        xy_intersection = x_nz & y_nz
        if not xy_intersection:
            distance = 0
            xy_union = set()
        else:
            xy_union = x_nz | y_nz
            distance = math.sqrt(sum((idf_[i] for i in xy_intersection)) / sum((idf_[i] for i in xy_union)))
        return distance, x_nz, y_nz, xy_intersection, xy_union

    @staticmethod
    def estimate_above_threshold(x_sum, y_sum, threshold):
        shared_wc=float(min(x_sum, y_sum))
        union_wc = max(x_sum, y_sum)
        # union_wc = (x_sum+y_sum)/2.
        ratio_above_threshold = (1./threshold)**2
        ratio_wc = union_wc/shared_wc
        return ratio_wc<ratio_above_threshold


"""
Consumes the iterable passed in and loads into into provided loader
whilst also respecting the dry run flag given
"""
def store_in_elasticsearch(r, loader, dry_run):
    #ensure its not null
    if r:
        if not dry_run:
            loader.put(Const.ELASTICSEARCH_RELATION_INDEX_NAME,
                Const.ELASTICSEARCH_RELATION_DOC_NAME + '-' + r.type,
                r.id, r.to_json())
        subj = copy(r.subject)
        obj = copy(r.object)
        if subj['id'] != obj['id']:
            r.subject = obj
            r.object = subj
            r.set_id()
        if not dry_run:
            loader.put(Const.ELASTICSEARCH_RELATION_INDEX_NAME,
                Const.ELASTICSEARCH_RELATION_DOC_NAME + '-' + r.type,
                r.id, r.to_json())

def digest_in_buckets(v, buckets_number):
    digested =set()
    for i in np.flatnonzero(v).flat:
        digested.add(i%buckets_number)
    return tuple(digested)


"""
Dummy function to bake arguments into 
"""
def calculate_pairs_local_init(type, row_labels, rows_ids, column_ids, threshold, idf, idf_):
    return (type, row_labels, rows_ids, column_ids, threshold, idf, idf_)

"""
Dummy function to bake arguments into 
"""
def produce_pairs_local_init(vector_hashes, buckets, threshold, sums_vector, data_vector):
    return ( vector_hashes, buckets, threshold, sums_vector, data_vector)

"""
handles producing pairs for a particular set of inputs
spawns multiple processess as needed
used to standardize d2d and t2t code path
"""
def handle_pairs(type, subject_labels, subject_data, subject_ids, other_ids, 
        threshold, buckets_number, loader, dry_run, 
        workers_production, workers_score,
        queue_production_score, queue_score_result):

    #do some initial setup
    vectorizer = DictVectorizer(sparse=True)
    tdidf_transformer = LocalTfidfTransformer(smooth_idf=False, )
    data_vector = vectorizer.fit_transform([subject_data[i] for i in subject_ids])
    data_vector = data_vector > 0
    data_vector = data_vector.astype(int)
    transformed_data = tdidf_transformer.fit_transform(data_vector)
    sums_vector = np.squeeze(np.asarray(transformed_data.sum(1)).ravel())#sum by row
    '''put vectors in buckets'''
    buckets = {}
    for i in range(buckets_number):
        buckets[i]=[]
    vector_hashes = {}
    for i in range(len(subject_ids)):
        vector = transformed_data[i].toarray()[0]
        digested = digest_in_buckets(vector, buckets_number)
        for bucket in digested:
            buckets[bucket].append(i)
        vector_hashes[i]=digested

    idf = dict(zip(vectorizer.feature_names_, list(tdidf_transformer.idf_)))
    idf_ = 1-tdidf_transformer.idf_

    #now everything is computed that can be baked into the function arguments

    produce_pairs_local_init_baked = functools.partial(produce_pairs_local_init, 
        vector_hashes, buckets, threshold, sums_vector, data_vector)

    calculate_pairs_local_init_baked = functools.partial(calculate_pairs_local_init, 
        type, subject_labels, subject_ids, other_ids, threshold, idf, idf_)

    #create stage for producing disease-to-disease
    pipeline_stage = pr.flat_map(produce_pairs, range(len(subject_ids)), 
        workers=workers_production,
        maxsize=queue_production_score,
        on_start=produce_pairs_local_init_baked)

    #create stage to calculate disease-to-disease
    pipeline_stage = pr.map(calculate_pair, pipeline_stage, 
        workers=workers_score,
        maxsize=queue_score_result,
        on_start=calculate_pairs_local_init_baked)

    #store in elasticsearch
    #this could be multi process, but just use a single for now
    for r in pipeline_stage:
        store_in_elasticsearch(r, loader, dry_run)

"""
Function to run in child processess
"""
def produce_pairs(i, vector_hashes, buckets, threshold, sums_vector, data_vector):
    compared = set()
    result = []
    for bucket in vector_hashes[i]:
        for j in buckets[bucket]:
            if j not in compared:
                if i > j:
                    if OverlapDistance.estimate_above_threshold(sums_vector[i], sums_vector[j], threshold):
                        result.append((i, data_vector[i], j, data_vector[j]))
            compared.add(j)
    return result

def calculate_pair(data, type, row_labels, rows_ids, column_ids, threshold, idf, idf_):

    subject_index, subject_data, object_index, object_data = data
    
    distance, subject_nz, subject_nz, intersection, union = OverlapDistance.compute_weighted_distance(subject_data, object_data, idf_)

    #sanity checks, shouldn't really happen often
    if (distance <= threshold) or (not intersection) :
        return None

    subject = dict(id=rows_ids[subject_index],
                    label=row_labels[subject_index],
                    links={})
    object = dict(id=rows_ids[object_index],
                    label=row_labels[object_index],
                    links={})
    dist = {
        'overlap': distance,
    }
    body = dict()
    body['counts'] = {'shared_count': len(intersection),
                        'union_count': len(union),
                        }
    '''sort shared items by idf score'''
    weighted_shared_labels = sorted([(idf[column_ids[i]],column_ids[i])  for i in intersection])
    '''sort shared entities by significance'''
    shared_labels = [i[1] for i in weighted_shared_labels]
    if type == RelationType.SHARED_TARGET:
        subject['links']['targets_count'] = subject_data.getnnz()
        object['links']['targets_count'] = object_data.getnnz()
        body['shared_targets'] = shared_labels
    elif type == RelationType.SHARED_DISEASE:
        subject['links']['diseases_count'] = subject_data.getnnz()
        object['links']['diseases_count'] = object_data.getnnz()
        body['shared_diseases'] = shared_labels
    #create the relation object
    r = Relation(subject, object, dist, type, **body)
    return r


class DataDrivenRelationProcess(object):

    def __init__(self, es):
        self.es = es
        self.es_query=ESQuery(self.es)
        self.logger = logging.getLogger(__name__)

    def process_all(self, dry_run, 
            ddr_workers_production,
            ddr_workers_score,
            ddr_queue_production_score,
            ddr_queue_score_result):
        start_time = time.time()

        target_data, disease_data = self.es_query.get_disease_to_targets_vectors()

        self.logger.info('Retrieved all the associations data in %i s'%(time.time()-start_time))
        self.logger.info('target data length: %s size in memory: %f Kb'%(len(target_data),sys.getsizeof(target_data)/1024.))
        self.logger.info('disease data length: %s size in memory: %f Kb' % (len(disease_data),sys.getsizeof(disease_data)/1024.))

        '''sort the lists and keep using always the same order in all the steps'''
        disease_keys = sorted(disease_data.keys())
        target_keys = sorted(target_data.keys())

        self.logger.info('getting disese labels')
        disease_id_to_label = self.es_query.get_disease_labels(disease_keys)
        disease_labels = [disease_id_to_label[hit_id] for hit_id in disease_keys]
        self.logger.info('getting target labels')
        target_id_to_label = self.es_query.get_target_labels(target_keys)
        target_labels = [target_id_to_label[hit_id] for hit_id in target_keys]

        #setup elasticsearch
        self.loader = Loader(self.es, dry_run=dry_run)
        if not dry_run:
            #need to directly get the versioned index name for this function
            self.loader.create_new_index(Const.ELASTICSEARCH_RELATION_INDEX_NAME)
            self.loader.prepare_for_bulk_indexing(self.loader.get_versioned_index(Const.ELASTICSEARCH_RELATION_INDEX_NAME))


        #calculate and store disease-to-disease in multiple processess
        self.logger.info('handling disease-to-disease')
        handle_pairs(RelationType.SHARED_TARGET, disease_labels, disease_data, disease_keys, 
            target_keys, 0.19, 1024, self.loader, dry_run, 
            ddr_workers_production, ddr_workers_score, 
            ddr_queue_production_score, ddr_queue_score_result)
        self.logger.info('handled disease-to-disease')

        #calculate and store target-to-target in multiple processess
        self.logger.info('handling target-to-target')
        handle_pairs(RelationType.SHARED_DISEASE, target_labels, target_data, target_keys, 
            disease_keys, 0.19, 1024, self.loader, dry_run, 
            ddr_workers_production, ddr_workers_score, 
            ddr_queue_production_score, ddr_queue_score_result)
        self.logger.info('handled target-to-target')

        #cleanup elasticsearch
        if not dry_run:
            self.loader.flush_all_and_wait(Const.ELASTICSEARCH_RELATION_INDEX_NAME)
            #restore old pre-load settings
            #note this automatically does all prepared indexes
            self.loader.restore_after_bulk_indexing()
    
