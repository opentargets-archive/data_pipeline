import logging
from collections import Counter
import sys, os
import multiprocessing
import numpy as np

import pickle

import scipy
import scipy.sparse as sp

from redislite import Redis
from sklearn.feature_extraction import DictVectorizer
from sklearn.feature_extraction.text import TfidfTransformer, _document_frequency
from tqdm import tqdm
from mrtarget.common import TqdmToLogger

from mrtarget.common import Actions
from mrtarget.common.DataStructure import JSONSerializable, RelationType
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.ElasticsearchQuery import ESQuery
from scipy.spatial.distance import pdist
import time
from copy import copy
import math

from mrtarget.common.Redis import RedisQueue, RedisQueueStatusReporter, RedisQueueWorkerProcess
from mrtarget.Settings import Config
from mrtarget.common.connection import new_redis_client



logger = logging.getLogger(__name__)
tqdm_out = TqdmToLogger(logger,level=logging.INFO)


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


class DataDrivenRelationActions(Actions):
    PROCESS='process'

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
        subject_index, subject_data, object_index, object_data = data
        distance, subject_nz, subject_nz, intersection, union = OverlapDistance.compute_distance(subject_data, object_data)
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
        shared_labels = [self.column_ids[i] for i in intersection]
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

    # def old_run(self):
    #     while not self.queue_in.is_done(r_server=self.r_server):
    #         job = self.queue_in.get(r_server=self.r_server, timeout=1)
    #         if job is not None:
    #             key, data = job
    #             error = False
    #             try:
    #                 subject_id, subject_data, subject_label, object_id, object_data, object_label = data
    #                 subject = dict(id=subject_id,
    #                                label = subject_label,
    #                                links={})
    #                 object = dict(id=object_id,
    #                               label=object_label,
    #                               links={})
    #                 union_keys = set(subject_data.keys()) | set(object_data.keys())
    #                 shared_keys = set(subject_data.keys()) & set(object_data.keys())
    #                 if self.filtered_keys:
    #                     union_keys = union_keys - self.filtered_keys # remove filtered keys if needed
    #                     shared_keys = shared_keys - self.filtered_keys
    #                 shared_keys = self._get_ordered_keys(subject_data, object_data, shared_keys)
    #                 if union_keys:
    #                     w_neg = sum([1./self.weights[i] for i in union_keys])
    #                     w_pos = sum([1./self.weights[i] for i in shared_keys])
    #                     pos = len(shared_keys)
    #                     neg = len(union_keys)
    #                     jackard, jackard_weighted = 0., 0.
    #                     if neg:
    #                         jackard = float(pos)/neg
    #                         jackard_weighted = float(w_pos)/w_neg
    #                     dist = {
    #                             'jaccard': jackard,
    #                             'jackard_weighted': jackard_weighted,
    #                             }
    #                     dist.update(self._compute_vector_based_distances(subject_data, object_data, union_keys))
    #                     body = dict()
    #                     body['counts'] = {'shared_count': pos,
    #                                       'union_count': neg,
    #                                       }
    #                     if self.type == RelationType.SHARED_TARGET:
    #                         subject['links']['targets_count'] =len(subject_data)
    #                         object['links']['targets_count'] = len(object_data)
    #                         body['shared_targets'] = list(shared_keys)
    #                     elif self.type == RelationType.SHARED_DISEASE:
    #                         subject['links']['diseases_count'] = len(subject_data)
    #                         object['links']['diseases_count'] = len(object_data)
    #                         body['shared_diseases'] = list(shared_keys)
    #                     r = Relation(subject, object, dist, self.type, **body)
    #                     self.queue_out.put(r, self.r_server)#TODO: create an object here
    #             except Exception, e:
    #                 error = True
    #                 logger.exception('Error processing key %s' % key)
    #
    #             self.queue_in.done(key, error=error, r_server=self.r_server)

        # logger.info('%s done processing'%self.name)

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


class DistanceStorageWorker(RedisQueueWorkerProcess):
        def __init__(self,
                     queue_in,
                     redis_path,
                     queue_out=None,
                     dry_run = False,
                     chunk_size=1000):
            super(DistanceStorageWorker, self).__init__(queue_in, redis_path, queue_out)
            self.loader = None
            self.chunk_size = chunk_size
            self.dry_run = dry_run

        def process(self, data):
            r = data
            self.loader.put(Config.ELASTICSEARCH_RELATION_INDEX_NAME,
                       Config.ELASTICSEARCH_RELATION_DOC_NAME + '-' + r.type,
                       r.id,
                       r.to_json(),
                       create_index=False,
                       routing=r.subject['id'])
            subj = copy(r.subject)
            obj = copy(r.object)
            if subj['id'] != obj['id']:
                r.subject = obj
                r.object = subj
                r.set_id()
                self.loader.put(Config.ELASTICSEARCH_RELATION_INDEX_NAME,
                           Config.ELASTICSEARCH_RELATION_DOC_NAME + '-' + r.type,
                           r.id,
                           r.to_json(),
                           create_index=False,
                           routing=r.subject['id'])

        def init(self):
            super(DistanceStorageWorker, self).init()
            self.loader = Loader(chunk_size=self.chunk_size,
                                 dry_run=self.dry_run)

        def close(self):
            super(DistanceStorageWorker, self).close()
            self.loader.close()


class MatrixIteratorWorker(RedisQueueWorkerProcess):

    def __init__(self,
                 queue_in,
                 redis_path,
                 queue_out=None,
                 matrix_data = None,
                 key_list = None,
                 labels = None
                 ):
        super(MatrixIteratorWorker, self).__init__(queue_in,redis_path, queue_out)
        self.matrix_data = matrix_data
        self.keys = key_list
        self.labels = labels

    def process(self, data):
        i = data
        for j in range(len(self.keys)):
            if j >= i:
                subj_id = self.keys[i]
                obj_id = self.keys[j]
                if set(self.matrix_data[subj_id].keys()) & set(self.matrix_data[obj_id].keys()):
                    self.put_into_queue_out((subj_id,
                                             self.matrix_data[subj_id],
                                             self.labels[subj_id], obj_id,
                                             self.matrix_data[obj_id],
                                             self.labels[obj_id]),
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

class RedisRelationHandler(object):
    '''
    A Redis backend to optimise storage and lookup ot target-disease relations

    '''

    SCORE = "score:%(key)s"#sorted set with target/disease as key, disease/target as value and association score as sorting score
    SUM = "sum:%(key)s"#store a float for each target or disease
    WEIGHT = "weight:%(key)s"#store a float for each target or disease
    RELATIONS= "weight:%(key)s"#sorted set with target/disease as key, target/disease as value and target/disease sum as sorting score

    def __init__(self,
                 target_data,
                 disease_data,
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
        self.available_targets = target_data.keys()
        self.available_diseases = disease_data.keys()
        vectorizer = DictVectorizer(sparse=True)
        target_tdidf_transformer = LocalTfidfTransformer(smooth_idf=False, norm=None)
        # target_tdidf_transformer = TfidfTransformer(smooth_idf=False, norm=None)
        target_data_vector = vectorizer.fit_transform([target_data[i] for i in self.available_targets])
        if not use_quantitiative_scores:
            target_data_vector = target_data_vector > 0
            target_data_vector = target_data_vector.astype(int)
        transformed_targets = target_tdidf_transformer.fit_transform(target_data_vector)
        for i in range(len(self.available_targets)):
            target=self.available_targets[i]
            vector= transformed_targets[i].toarray()[0]
            pipe = self.r_server.pipeline()
            for v in range(len(vector)):
                if vector[v]:
                    pipe.zadd(self.SCORE%dict(key=target), vectorizer.get_feature_names()[v], vector[v])
            pipe.execute()
            weighted_sum = vector.sum()
            # self.r_server.add(self.SUM%dict(key=target), weighted_sum)
            print i, target, weighted_sum

class RelationHandler(object):
    '''
    A Redis backend to optimise storage and lookup ot target-disease relations

    '''

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
        self.use_quantitiative_scores = False

    def produce_d2d_pairs(self, subject_analysis_queue=None, produced_pairs_queue=None, redis_path=None):

        # produce disease pairs
        self._produce_pairs(self.disease_data,
                                     self.available_diseases,
                                     self.target_data,
                                     sample_size= 1024,
                                     threshold=0.19,
                                     subject_analysis_queue=subject_analysis_queue,
                                     produced_pairs_queue=produced_pairs_queue,
                                     redis_path = redis_path)

    def produce_t2t_pairs(self,  subject_analysis_queue=None, produced_pairs_queue=None, redis_path=None):

        # #produce target pairs
        self._produce_pairs(self.target_data,
                                     self.available_targets,
                                     self.disease_data,
                                     sample_size= 1024,
                                     threshold=0.39,
                                     subject_analysis_queue=subject_analysis_queue,
                                     produced_pairs_queue=produced_pairs_queue,
                                     redis_path = redis_path)



    def _produce_pairs(self, subject_data, subject_ids, shared_ids, threshold=0.5, sample_size=128,  subject_analysis_queue = None, produced_pairs_queue = None, redis_path = None):
        raise NotImplementedError()



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
    def estimate_below_threshold( x_sum, y_sum, threshold = 0.2):
        shared_wc=float(min(x_sum, y_sum))
        union_wc = max(x_sum, y_sum)
        # union_wc = (x_sum+y_sum)/2.
        ratio_above_threshold = (1./threshold)**2
        ratio_wc = union_wc/shared_wc
        return ratio_wc<ratio_above_threshold



class RelationHandlerEuristicOverlapEstimation(RelationHandler):

    def _produce_pairs(self, subject_data, subject_ids, shared_ids, threshold=0.5, sample_size=512, subject_analysis_queue = None, produced_pairs_queue = None, redis_path = None):
        self.r_server = redis_path if redis_path else new_redis_client()

        vectorizer = DictVectorizer(sparse=True)
        # tdidf_transformer = LocalTfidfTransformer(smooth_idf=False, norm=None)
        tdidf_transformer = TfidfTransformer(smooth_idf=False, norm=None)
        data_vector = vectorizer.fit_transform([subject_data[i] for i in subject_ids])
        if not self.use_quantitiative_scores:
            data_vector = data_vector > 0
            data_vector = data_vector.astype(int)
        transformed_data = tdidf_transformer.fit_transform(data_vector)
        sums_vector = np.squeeze(np.asarray(transformed_data.sum(1)).ravel())
        limit = -1  # debugging
        buckets_number = sample_size
        tot = 0
        optimised_nn = 0
        really_above_threshold = 0
        '''put vectors in buckets'''
        buckets = {}
        for i in range(buckets_number):
            buckets[i]=[]
        vector_hashes = {}
        for i in tqdm(range(len(subject_ids[:limit])),
                      desc='hashing vectors',
                      file=tqdm_out):
            vector = transformed_data[i].toarray()[0]
            digested = self.digest_in_buckets(vector, buckets_number)
            for bucket in digested:
                buckets[bucket].append(i)
            vector_hashes[i]=digested
        # print 'Data distribution in buckets'
        # for k,v in sorted(buckets.items()):
        #     print k, len(v)


        pair_producers = [RelationHandlerEuristicOverlapEstimationPairProducer(subject_analysis_queue,
                                                                               None,
                                                                               produced_pairs_queue,
                                                                               vector_hashes,
                                                                               buckets,
                                                                               threshold,
                                                                               sums_vector,
                                                                               data_vector
                                                                               )
                          for i in range(Config.WORKERS_NUMBER)]
        for w in pair_producers:
            w.start()
        for i in tqdm(range(len(subject_ids[:limit])),
                      desc='getting neighbors',
                      file=tqdm_out):
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
                 ):
        super(RelationHandlerEuristicOverlapEstimationPairProducer, self).__init__(queue_in, redis_path, queue_out)
        self.vector_hashes = vector_hashes
        self.buckets = buckets
        self.threshold = threshold
        self.sums_vector = sums_vector
        self.data_vector = data_vector

    def process(self, data):
        i=data
        compared = set()
        for bucket in self.vector_hashes[i]:
            for j in self.buckets[bucket]:
                if j not in compared:
                    if i > j:
                        if OverlapDistance.estimate_below_threshold(self.sums_vector[i], self.sums_vector[j],
                                                                    threshold=self.threshold):
                            self.put_into_queue_out((i, self.data_vector[i], j, self.data_vector[j]))
                compared.add(j)

class RelationHandlerProduceAll(RelationHandler):

    def _produce_pairs(self, subject_data, subject_ids, shared_ids, threshold=0.5, sample_size=512):
        vectorizer = DictVectorizer(sparse=True)
        # tdidf_transformer = LocalTfidfTransformer(smooth_idf=False, norm=None)
        tdidf_transformer = TfidfTransformer(smooth_idf=False, norm=None)
        data_vector = vectorizer.fit_transform([subject_data[i] for i in subject_ids])
        limit = -1
        for i in tqdm(range(len(subject_ids[:limit])),
                      desc='producing all pairs',
                      file=tqdm_out):
            for j in range(len(subject_ids[:limit])):
                if i>j:
                    yield (i, data_vector[i],  j, data_vector[j])
        #             tot+= 1
        #     if i%1000 == 0:
        #         if optimised_nn:
        #             ratio =  (1-(optimised_nn/float(tot))) * 100
        #         else: ratio = 0.
        #         logger.info('total pairs %i | optimised pairs %i | compression ratio: %1.2f%% | above threshold %i '%(tot, optimised_nn, ratio, really_above_threshold))
        #
        #
        # logger.info("found %i NNs, optimised to %i by distance threshold over %i analysed vectors. pairs above threshold: %i" % (tot, optimised_nn, len(subject_ids), really_above_threshold))

    @staticmethod
    def digest_in_buckets(v, buckets_number):
        digested =set()
        for i in np.flatnonzero(v).flat:
            digested.add(i%buckets_number)
        return tuple(digested)


class DataDrivenRelationProcess(object):


    def __init__(self, es, r_server):
        self.es = es
        self.es_query=ESQuery(self.es)
        self.r_server = r_server

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
        tmp_data_dump = '/tmp/ddr_data_dump.pkl'
        if os.path.exists(tmp_data_dump):
            target_data, disease_data = pickle.load(open(tmp_data_dump))
        else:
            target_data, disease_data = self.es_query.get_disease_to_targets_vectors()
            pickle.dump((target_data, disease_data), open(tmp_data_dump, 'w'))
        logger.info('Retrieved all the associations data in %i s'%(time.time()-start_time))
        logger.info('target data length: %s size in memory: %f Kb'%(len(target_data),sys.getsizeof(target_data)/1024.))
        logger.info('disease data length: %s size in memory: %f Kb' % (len(disease_data),sys.getsizeof(disease_data)/1024.))

        disease_keys = disease_data.keys()
        target_keys = target_data.keys()

        number_of_workers = Config.WORKERS_NUMBER
        number_of_storers = number_of_workers / 2
        queue_per_worker =150

        logger.debug('call relationhandlereuristicoverlapestimation')
        # rel_handler = RelationHandlerProduceAll(target_data=target_data,
        rel_handler = RelationHandlerEuristicOverlapEstimation(target_data=target_data,
                                                               disease_data=disease_data,
                                                               ordered_target_keys=target_keys,
                                                               ordered_disease_keys=disease_keys,
                                                               r_server=self.r_server)
        logger.info('getting disese labels')
        disease_id_to_label = self.es_query.get_disease_labels(disease_keys)
        disease_labels = [disease_id_to_label[hit_id] for hit_id in disease_keys]
        logger.info('getting target labels')
        target_id_to_label = self.es_query.get_target_labels(target_keys)
        target_labels = [target_id_to_label[hit_id] for hit_id in target_keys]



        '''create the index'''
        self.loader = Loader(self.es, dry_run=dry_run)
        self.loader.create_new_index(Config.ELASTICSEARCH_RELATION_INDEX_NAME, recreate=True)
        self.loader.prepare_for_bulk_indexing(self.loader.get_versioned_index(Config.ELASTICSEARCH_RELATION_INDEX_NAME))


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
        q_reporter = RedisQueueStatusReporter([d2d_pair_producing,
                                               t2t_pair_producing,
                                               d2d_queue_processing,
                                               t2t_queue_processing,
                                               queue_storage],
                                              interval=30,
                                              history=True)
        q_reporter.start()

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


        ''' compute disease to disease distances'''
        logger.info('Starting to push pairs for disease to disease distances computation')
        rel_handler.produce_d2d_pairs(d2d_pair_producing, d2d_queue_processing, self.r_server)

        logger.info('disease to disease distances pair push done')

        '''start workers for t2t'''

        t2t_workers = [DistanceComputationWorker(t2t_queue_processing,
                                                 None,
                                                 queue_storage,
                                                 RelationType.SHARED_DISEASE,
                                                 target_labels,
                                                 target_keys,
                                                 disease_keys,
                                                 0.4,
                                                 ) for _ in range(number_of_workers*2)]
        for w in t2t_workers:
            w.start()

        ''' compute target to target distances'''
        logger.info('Starting to push pairs for target to target distances computation')
        rel_handler.produce_t2t_pairs(t2t_pair_producing, t2t_queue_processing, self.r_server)
        logger.info('target to target distances pair push done')

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

        logger.info('flushing data to index')

        self.loader.close()
        self.es.indices.flush(
            '%s*' % Loader.get_versioned_index(Config.ELASTICSEARCH_RELATION_INDEX_NAME),
            wait_if_ongoing=True)


    def get_hot_node_blacklist(self, data):
        c = Counter()
        for k,v in data.items():
            c[k]=len(v)

        logger.info('Most common diseases: %s'%c.most_common(10))
        return [i[0] for i in c.most_common(10)]

    def get_inverted_counts(self, data):
        c = Counter()
        for k, v in data.items():
            c[k] = len(v)

        # logger.info('Most common diseases: %s' % c.most_common(10))
        return c

