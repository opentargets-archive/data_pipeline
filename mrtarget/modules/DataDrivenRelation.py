import logging
from collections import Counter
import sys, os
import numpy as np

import pickle

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

from mrtarget.common.Redis import RedisQueue, RedisQueueWorkerProcess
from mrtarget.constants import Const
from mrtarget.Settings import Config
from mrtarget.common.connection import new_redis_client



class RelationType(object):
    SHARED_DISEASE = 'shared-disease'
    SHARED_TARGET = 'shared-target'


class Relation(JSONSerializable):
    type = ''

    def __init__(self,
                 subject,
                 object,
                 scores,
                 type = '',
                 **kwargs):
        self.subject = subject
        self.object = object
        self.scores = scores
        if type:
            self.type = type
        self.__dict__.update(**kwargs)
        self.set_id()

    def set_id(self):
        self.id = '-'.join([self.subject['id'], self.object['id']])

class T2TRelation(JSONSerializable):
    type = RelationType.SHARED_DISEASE

class D2DRelation(JSONSerializable):
    type = RelationType.SHARED_TARGET

class DistanceComputationWorker(RedisQueueWorkerProcess):
    def __init__(self,
                 queue_in,
                 r_server,
                 queue_out,
                 type,
                 row_labels,
                 rows_ids,
                 column_ids,
                 threshold = 0.,
                 auto_signal_submission_finished = True
                 ):
        super(DistanceComputationWorker, self).__init__(queue_in,
                                                        r_server,
                                                        queue_out,
                                                        auto_signal_submission_finished = auto_signal_submission_finished)
        self.type = type
        self.row_labels = row_labels
        self.rows_ids = rows_ids
        self.column_ids = column_ids
        self.threshold = threshold

    def process(self, data):
        subject_index, subject_data, object_index, object_data, idf, idf_ = data
        # distance, subject_nz, subject_nz, intersection, union = OverlapDistance.compute_distance(subject_data, object_data)
        distance, subject_nz, subject_nz, intersection, union = OverlapDistance.compute_weighted_distance(subject_data, object_data, idf_)

        if (distance <= self.threshold) or (not intersection) :
            return
        subject = dict(id=self.rows_ids[subject_index],
                       label=self.row_labels[subject_index],
                       links={})
        object = dict(id=self.rows_ids[object_index],
                      label=self.row_labels[object_index],
                      links={})
        dist = {
            'overlap': distance,
        }
        body = dict()
        body['counts'] = {'shared_count': len(intersection),
                          'union_count': len(union),
                          }
        '''sort shared items by idf score'''
        weighted_shared_labels = sorted([(idf[self.column_ids[i]],self.column_ids[i])  for i in intersection])
        '''sort shared entities by significance'''
        shared_labels = [i[1] for i in weighted_shared_labels]
        if self.type == RelationType.SHARED_TARGET:
            subject['links']['targets_count'] = subject_data.getnnz()
            object['links']['targets_count'] = object_data.getnnz()
            body['shared_targets'] = shared_labels
        elif self.type == RelationType.SHARED_DISEASE:
            subject['links']['diseases_count'] = subject_data.getnnz()
            object['links']['diseases_count'] = object_data.getnnz()
            body['shared_diseases'] = shared_labels
        r = Relation(subject, object, dist, self.type, **body)
        return r

    def _get_ordered_keys(self, subject_data, object_data, keys):
        ordered_keys = sorted([(max(subject_data[key], object_data[key]), key) for key in keys], reverse=True)
        return list((i[1] for i in ordered_keys))

    def _compute_vector_based_distances(self, subject_data, object_data, keys):
        '''calculate a normlized inverted euclidean distance.
        return 1 for perfect match
        returns 0 if nothing is in common'''
        subj_vector_capped = [DataDrivenRelationProcess.cap_to_one(i) for i in
                       [subject_data[k] for k in keys]]
        obj_vector_capped = [DataDrivenRelationProcess.cap_to_one(i) for i in
                      [object_data[k] for k in keys]]
        subj_vector = [DataDrivenRelationProcess.transform_for_euclidean_distance(i) for i in subj_vector_capped]
        obj_vector = [DataDrivenRelationProcess.transform_for_euclidean_distance(i) for i in obj_vector_capped]
        # subj_vector_b = [i==0. for i in subj_vector]
        # obj_vector_b = [i==0. for i in obj_vector]
        vectors = np.array([subj_vector, obj_vector])
        # vectors_b = np.array([subj_vector_b, obj_vector_b])
        correlation = pdist(vectors, 'correlation')[0]
        if math.isnan(correlation):
            correlation = 0.0
        return dict(euclidean = 1.-(pdist(vectors, 'euclidean')[0]/(math.sqrt(len(keys))*2)),
                     # jaccard_formal= pdist(vectors, 'jaccard')[0],
                     # matching=pdist(vectors, 'matching')[0],
                     # matching_b=pdist(vectors_b, 'matching')[0],
                     # cosine=pdist([subj_vector_capped, obj_vector_capped], 'cosine')[0],
                     # correlation=correlation,
                     cityblock=1-(pdist(vectors, 'cityblock')[0]/(len(keys)*2)),
                     # hamming=pdist(vectors, 'hamming')[0],
                     # hamming_b=pdist(vectors_b, 'hamming')[0],
                     )


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
    def estimate_above_threshold(x_sum, y_sum, threshold = 0.19):
        shared_wc=float(min(x_sum, y_sum))
        union_wc = max(x_sum, y_sum)
        # union_wc = (x_sum+y_sum)/2.
        ratio_above_threshold = (1./threshold)**2
        ratio_wc = union_wc/shared_wc
        return ratio_wc<ratio_above_threshold



class RelationHandlerEuristicOverlapEstimation(object):


    def __init__(self,
                 target_data,
                 disease_data,
                 ordered_target_keys,
                 ordered_disease_keys,
                 r_server=None,
                 use_quantitiative_scores = False
                 ):
        '''
        :param queue_id: queue id to attach to preconfigured queues
        :param r_server: a redis.Redis instance to be used in methods. If supplied the RedisQueue object
                             will not be pickable
        :param max_size: maximum size of the queue. queue will block if full, and allow put only if smaller than the
                         maximum size.
        :return:
        '''

        self.r_server = r_server
        if self.r_server is None:
            self.r_server = new_redis_client()

        self.target_data = target_data
        self.disease_data = disease_data
        self.available_targets = ordered_target_keys
        self.available_diseases = ordered_disease_keys
        self.use_quantitiative_scores = use_quantitiative_scores

    def produce_d2d_pairs(self, subject_analysis_queue, produced_pairs_queue, redis_path):
        '''trigger production of disease to disease distances using carefully selected euristics threshold'''

        # produce disease pairs
        self._produce_pairs(self.disease_data,
                                     self.available_diseases,
                                     self.target_data,
                                     sample_size= 1024,
                                     threshold=0.19,
                                     subject_analysis_queue=subject_analysis_queue,
                                     produced_pairs_queue=produced_pairs_queue,
                                     redis_path = redis_path)

    def produce_t2t_pairs(self,  subject_analysis_queue, produced_pairs_queue, redis_path):
        '''trigger production of target to target distances using carefully selected euristics threshold'''

        # #produce target pairs
        self._produce_pairs(self.target_data,
                                     self.available_targets,
                                     self.disease_data,
                                     sample_size= 1024,
                                     threshold=0.19,
                                     subject_analysis_queue=subject_analysis_queue,
                                     produced_pairs_queue=produced_pairs_queue,
                                     redis_path = redis_path)

    def _produce_pairs(self, subject_data, subject_ids, shared_ids, threshold, sample_size, subject_analysis_queue, produced_pairs_queue, redis_path):
        self.r_server = redis_path if redis_path else new_redis_client()

        vectorizer = DictVectorizer(sparse=True)
        tdidf_transformer = LocalTfidfTransformer(smooth_idf=False, )
        # tdidf_transformer = TfidfTransformer(smooth_idf=False,)
        data_vector = vectorizer.fit_transform([subject_data[i] for i in subject_ids])
        if not self.use_quantitiative_scores:
            data_vector = data_vector > 0
            data_vector = data_vector.astype(int)
        transformed_data = tdidf_transformer.fit_transform(data_vector)
        sums_vector = np.squeeze(np.asarray(transformed_data.sum(1)).ravel())#sum by row
        limit = -1  # use for faster debug
        buckets_number = sample_size
        '''put vectors in buckets'''
        buckets = {}
        for i in range(buckets_number):
            buckets[i]=[]
        vector_hashes = {}
        for i in range(len(subject_ids[:limit])):
            vector = transformed_data[i].toarray()[0]
            digested = self.digest_in_buckets(vector, buckets_number)
            for bucket in digested:
                buckets[bucket].append(i)
            vector_hashes[i]=digested



        pair_producers = [RelationHandlerEuristicOverlapEstimationPairProducer(subject_analysis_queue,
                                                                               None,
                                                                               produced_pairs_queue,
                                                                               vector_hashes,
                                                                               buckets,
                                                                               threshold,
                                                                               sums_vector,
                                                                               data_vector,
                                                                               idf= dict(zip(vectorizer.feature_names_, list(tdidf_transformer.idf_))),
                                                                               idf_ = 1-tdidf_transformer.idf_,
                                                                               )
                          for i in range(Config.WORKERS_NUMBER)]
        for w in pair_producers:
            w.start()
        for i in range(len(subject_ids[:limit])):
            subject_analysis_queue.put(i, r_server=self.r_server)
        subject_analysis_queue.set_submission_finished(r_server=self.r_server)

        for w in pair_producers:
            w.join()

    @staticmethod
    def digest_in_buckets(v, buckets_number):
        digested =set()
        for i in np.flatnonzero(v).flat:
            digested.add(i%buckets_number)
        return tuple(digested)

class RelationHandlerEuristicOverlapEstimationPairProducer(RedisQueueWorkerProcess):


    def __init__(self,
                 queue_in,
                 redis_path,
                 queue_out,
                 vector_hashes,
                 buckets,
                 threshold,
                 sums_vector,
                 data_vector,
                 idf=None,
                 idf_=None,
                 ):
        super(RelationHandlerEuristicOverlapEstimationPairProducer, self).__init__(queue_in, redis_path, queue_out)
        self.vector_hashes = vector_hashes
        self.buckets = buckets
        self.threshold = threshold
        self.sums_vector = sums_vector
        self.data_vector = data_vector
        self.idf = idf #dictionary
        self.idf_ = idf_ #inverted idf np array

    def process(self, data):
        i=data
        compared = set()
        for bucket in self.vector_hashes[i]:
            for j in self.buckets[bucket]:
                if j not in compared:
                    if i > j:
                        if OverlapDistance.estimate_above_threshold(self.sums_vector[i], self.sums_vector[j],  #only works with binary data, not floats
                                                                    threshold=self.threshold):
                            self.put_into_queue_out((i, self.data_vector[i], j, self.data_vector[j], self.idf, self.idf_))
                compared.add(j)

"""
Consumes the iterable passed in and loads into into provided loader
whilst also respecting the dry run flag given
"""
def store_in_elasticsearch(iterable, loader, dry_run):
    for r in iterable
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

def produce_pairs(self, subject_data, subject_ids, threshold, buckets_number):

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
    for i in range(len(subject_ids[:limit])):
        vector = transformed_data[i].toarray()[0]
        digested = self.digest_in_buckets(vector, buckets_number)
        for bucket in digested:
            buckets[bucket].append(i)
        vector_hashes[i]=digested



    pair_producers = [RelationHandlerEuristicOverlapEstimationPairProducer(subject_analysis_queue,
            None,
            produced_pairs_queue,
            vector_hashes,
            buckets,
            threshold,
            sums_vector,
            data_vector,
            idf= dict(zip(vectorizer.feature_names_, list(tdidf_transformer.idf_))),
            idf_ = 1-tdidf_transformer.idf_,
            )
        for i in range(Config.WORKERS_NUMBER)]

    for i in range(len(subject_ids)):
        subject_analysis_queue.put(i, r_server=self.r_server)


class DataDrivenRelationProcess(object):


    def __init__(self, es, r_server):
        self.es = es
        self.es_query=ESQuery(self.es)
        self.r_server = r_server
        self.logger = logging.getLogger(__name__)


    @staticmethod
    def cap_to_one(i):
        if i>1.:
            return 1.
        return i

    @staticmethod
    def transform_for_euclidean_distance(i):
        if i == 0:
            i=-1.
        return DataDrivenRelationProcess.cap_to_one(i)


    def process_all(self, dry_run= False):
        start_time = time.time()

        target_data, disease_data = self.es_query.get_disease_to_targets_vectors()

        self.logger.info('Retrieved all the associations data in %i s'%(time.time()-start_time))
        self.logger.info('target data length: %s size in memory: %f Kb'%(len(target_data),sys.getsizeof(target_data)/1024.))
        self.logger.info('disease data length: %s size in memory: %f Kb' % (len(disease_data),sys.getsizeof(disease_data)/1024.))

        '''sort the lists and keep using always the same order in all the steps'''
        disease_keys = sorted(disease_data.keys())
        target_keys = sorted(target_data.keys())

        number_of_workers = Config.WORKERS_NUMBER
        number_of_storers = number_of_workers / 2
        queue_per_worker =150


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


        #TODO create stage for producing disease-to-disease
        #TODO calculate disease-to-disease
        #TODO store in elasticsearch
        store_in_elasticsearch(iterable, self.loader, dry_run)

        #TODO create stage for producing target-to-target
        #TODO calculate target-to-target
        #TODO store in elasticsearch
        store_in_elasticsearch(iterable, self.loader, dry_run)

        #cleanup elasticsearch
        if not dry_run:
            self.loader.flush_all_and_wait(Const.ELASTICSEARCH_RELATION_INDEX_NAME)
            #restore old pre-load settings
            #note this automatically does all prepared indexes
            self.loader.restore_after_bulk_indexing()





        '''create the queues'''
        d2d_pair_producing = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|ddr_d2d_pair_producting',
                                        max_size=number_of_workers* 5,
                                        job_timeout=60 * 60 * 24,
                                        batch_size=1,
                                        ttl=60 * 60 * 24 * 14,
                                        serialiser='json')
        t2t_pair_producing = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|ddr_t2t_pair_producting',
                                        max_size=number_of_workers * 5,
                                        job_timeout=60 * 60 * 24,
                                        batch_size=1,
                                        ttl=60 * 60 * 24 * 14,
                                        serialiser='json')

        d2d_queue_processing = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|ddr_d2d_processing',
                                          max_size=number_of_workers * queue_per_worker,
                                          job_timeout=300,
                                          batch_size=10,
                                          ttl=60 * 60 * 24 * 14,
                                          serialiser='pickle')
        t2t_queue_processing = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|ddr_t2t_processing',
                                          max_size=number_of_workers * queue_per_worker,
                                          job_timeout=300,
                                          batch_size=10,
                                          ttl=60 * 60 * 24 * 14,
                                          serialiser='pickle')

        queue_storage = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|ddr_storage',
                                   max_size=int(queue_per_worker * number_of_storers*10),
                                   batch_size=10,
                                   job_timeout=300,
                                   serialiser='pickle')
        '''start shared workers'''
        
        storage_workers = [DistanceStorageWorker(queue_storage,
                                                 None,
                                                 dry_run=dry_run,
                                                 chunk_size=queue_per_worker,
                                                 # ) for i in range(multiprocessing.cpu_count())]
                                                 ) for _ in range(number_of_storers)]

        for w in storage_workers:
            w.start()

        '''start workers for d2d'''

        d2d_workers = [DistanceComputationWorker(d2d_queue_processing,
                                                 None,
                                                 queue_storage,
                                                 RelationType.SHARED_TARGET,
                                                 disease_labels,
                                                 disease_keys,
                                                 target_keys,
                                                 0.2,
                                                 auto_signal_submission_finished = False,# don't signal submission is done until t2t workers are done
                                                 ) for _ in range(number_of_workers*2)]
        for w in d2d_workers:
            w.start()

        self.logger.debug('call relationhandlereuristicoverlapestimation')
        rel_handler = RelationHandlerEuristicOverlapEstimation(target_data=target_data,
                                                               disease_data=disease_data,
                                                               ordered_target_keys=target_keys,
                                                               ordered_disease_keys=disease_keys,
                                                               r_server=self.r_server,
                                                               use_quantitiative_scores=False)
        ''' compute disease to disease distances'''
        self.logger.info('Starting to push pairs for disease to disease distances computation')
        '''use cartesian product'''
        rel_handler.produce_d2d_pairs(d2d_pair_producing, d2d_queue_processing, self.r_server)

        self.logger.info('disease to disease distances pair push done')

        '''start workers for t2t'''

        t2t_workers = [DistanceComputationWorker(t2t_queue_processing,
                                                 None,
                                                 queue_storage,
                                                 RelationType.SHARED_DISEASE,
                                                 target_labels,
                                                 target_keys,
                                                 disease_keys,
                                                 0.2,
                                                 ) for _ in range(number_of_workers*2)]
        for w in t2t_workers:
            w.start()

        ''' compute target to target distances'''
        self.logger.info('Starting to push pairs for target to target distances computation')
        rel_handler.produce_t2t_pairs(t2t_pair_producing, t2t_queue_processing, self.r_server)
        self.logger.info('target to target distances pair push done')

        '''stop d2d specifc workers'''
        for w in d2d_workers:
            w.join()

        '''stop t2t specifc workers'''
        for w in t2t_workers:
            w.join()

        '''stop storage workers'''
        queue_storage.set_submission_finished(self.r_server)
        for w in storage_workers:
            w.join()

        self.logger.info('flushing data to index')
        if not dry_run:
            self.loader.flush_all_and_wait(Const.ELASTICSEARCH_RELATION_INDEX_NAME)
            #restore old pre-load settings
            #note this automatically does all prepared indexes
            self.loader.restore_after_bulk_indexing()
