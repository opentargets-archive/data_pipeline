import logging
from UserDict import UserDict
from collections import Counter
from multiprocessing import Pool, Process
from operator import itemgetter

import sys, os

import gc

import itertools

import multiprocessing
import numpy as np

import pickle

from redislite import Redis

from common import Actions
from common.DataStructure import JSONSerializable, RelationType
from common.ElasticsearchLoader import Loader
from common.ElasticsearchQuery import ESQuery
from scipy.spatial.distance import pdist
import time
from copy import copy
import math

from common.Redis import RedisQueue, RedisQueueStatusReporter, RedisQueueWorkerProcess
from settings import Config

STORAGE_CHUNK_SIZE = 1000
STORAGE_WORKERS = multiprocessing.cpu_count()/2

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

class DistanceComputationWorker(Process):
    def __init__(self,
                 queue_in,
                 filtered_keys,
                 queue_out,
                 type,
                 ):
        super(DistanceComputationWorker, self).__init__()
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.r_server = Redis(Config.REDISLITE_DB_PATH)
        logging.info('%s started'%self.name)
        self.filtered_keys = set(filtered_keys)
        self.type = type

    def run(self):
        while not self.queue_in.is_done(r_server=self.r_server):
            job = self.queue_in.get(r_server=self.r_server, timeout=1)
            if job is not None:
                key, data = job
                error = False
                try:
                    subject_id, subject_data, subject_label, object_id, object_data, object_label = data
                    subject = dict(id=subject_id,
                                   label = subject_label,
                                   links={})
                    object = dict(id=object_id,
                                  label=object_label,
                                  links={})
                    union_keys = set(subject_data.keys()) | set(object_data.keys())
                    shared_keys = set(subject_data.keys()) & set(object_data.keys())
                    shared_keys = self._get_ordered_keys(subject_data, object_data, shared_keys)
                    if self.filtered_keys:
                        union_keys = union_keys - self.filtered_keys # remove filtered keys if needed
                        shared_keys = shared_keys - self.filtered_keys
                    if union_keys:
                        pos = len(shared_keys)
                        neg = len(union_keys)
                        jackard = 0.
                        if neg:
                            jackard = float(pos)/neg
                        dist = {'euclidean': self._compute_normalised_euclidean_distance(subject_data, object_data, union_keys),
                                'jaccard': jackard,
                                'stongest_link_euclidean': self._compute_normalised_euclidean_distance(subject_data, object_data, self._get_ordered_keys(subject_data, object_data, union_keys)[:100]),
                                }
                        body = dict()
                        body['counts'] = {'shared_count': pos,
                                          'union_count': neg,
                                          }
                        if self.type == RelationType.SHARED_TARGET:
                            subject['links']['targets_count'] =len(subject_data)
                            object['links']['targets_count'] = len(object_data)
                            body['shared_targets'] = list(shared_keys)
                        elif self.type == RelationType.SHARED_DISEASE:
                            subject['links']['diseases_count'] = len(subject_data)
                            object['links']['diseases_count'] = len(object_data)
                            body['shared_diseases'] = list(shared_keys)
                        r = Relation(subject, object, dist, self.type, **body)
                        self.queue_out.put(r, self.r_server)#TODO: create an object here
                except Exception, e:
                    error = True
                    logging.exception('Error processing key %s' % key)

                self.queue_in.done(key, error=error, r_server=self.r_server)

        logging.info('%s done processing'%self.name)

    def _get_ordered_keys(self, subject_data, object_data, keys):
        ordered_keys = sorted([(max(subject_data[key], object_data[key]), key) for key in keys], reverse=True)
        return list((i[1] for i in ordered_keys))

    def _compute_normalised_euclidean_distance(self, subject_data, object_data, keys):
        '''calculate a normlized inverted euclidean distance.
        return 1 for perfect match
        returns 0 if nothing is in common'''
        subj_vector = [DataDrivenRelationProcess.cap_to_one(i) for i in [subject_data[k] for k in keys]]
        obj_vector = [DataDrivenRelationProcess.cap_to_one(i) for i in [object_data[k] for k in keys]]
        return 1.-(pdist([subj_vector, obj_vector])[0]/math.sqrt(len(keys)))



class DistanceStorageWorker(Process):
            def __init__(self,
                         queue_in,
                         es):
                super(DistanceStorageWorker, self).__init__()
                self.queue_in = queue_in
                self.es = es
                self.r_server = Redis(Config.REDISLITE_DB_PATH)
                logging.info('%s started' % self.name)

            def run(self):
                c=0
                with Loader(self.es, chunk_size=STORAGE_CHUNK_SIZE) as loader:
                    while not self.queue_in.is_done(r_server=self.r_server):
                        job = self.queue_in.get(r_server=self.r_server, timeout=1)
                        if job is not None:
                            key, data = job
                            error = False
                            try:
                                r = data
                                loader.put(Config.ELASTICSEARCH_RELATION_INDEX_NAME,
                                           Config.ELASTICSEARCH_RELATION_DOC_NAME+'-'+r.type,
                                           r.id,
                                           r.to_json(),
                                           create_index=False,
                                           routing = r.subject['id'])
                                c += 1
                                subj= copy(r.subject)
                                obj = copy(r.object)
                                if subj['id'] != obj['id']:
                                    r.subject = obj
                                    r.object = subj
                                    r.set_id()
                                    loader.put(Config.ELASTICSEARCH_RELATION_INDEX_NAME,
                                               Config.ELASTICSEARCH_RELATION_DOC_NAME + '-' + r.type,
                                               r.id,
                                               r.to_json(),
                                               create_index=False,
                                               routing=r.subject['id'])
                                c += 1
                            except Exception, e:
                                error = True
                                logging.exception('Error processing key %s' % key)

                            self.queue_in.done(key, error=error, r_server=self.r_server)

                logging.info('%s done processing' % self.name)


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
                    self.queue_out.put((subj_id, self.matrix_data[subj_id], self.labels[subj_id], obj_id, self.matrix_data[obj_id], self.labels[obj_id]),
                                       r_server= self.r_server)



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


    def process_all(self):
        start_time = time.time()
        tmp_data_dump = '/tmp/ddr_data_dump.pkl'
        if os.path.exists(tmp_data_dump):
            target_data, disease_data = pickle.load(open(tmp_data_dump))
        else:
            target_data, disease_data = self.es_query.get_disease_to_targets_vectors
            pickle.dump((target_data, disease_data), open(tmp_data_dump, 'w'))
        logging.info('Retrieved all the associations data in %i s'%(time.time()-start_time))
        logging.info('target data length: %s size in memory: %f Kb'%(len(target_data),sys.getsizeof(target_data)/1024.))
        logging.info('disease data length: %s size in memory: %f Kb' % (len(disease_data),sys.getsizeof(disease_data)/1024.))
        # available_targets = target_data.keys()
        # available_diseases = disease_data.keys()

        filtered_diseases = self.get_hot_node_blacklist(disease_data)

        '''create the index'''
        Loader(self.es).create_new_index(Config.ELASTICSEARCH_RELATION_INDEX_NAME)

        '''create the queues'''
        d2d_queue_loading = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|ddr_d2d_loading',
                                       max_size=multiprocessing.cpu_count() * STORAGE_WORKERS*2,
                                       job_timeout=180)
        t2t_queue_loading = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|ddr_t2t_loading',
                                       max_size=multiprocessing.cpu_count() * STORAGE_CHUNK_SIZE * 2,
                                       job_timeout=180)

        d2d_queue_processing = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|ddr_d2d_processing',
                                      max_size=multiprocessing.cpu_count()*STORAGE_CHUNK_SIZE,
                                      job_timeout=20)
        t2t_queue_processing = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|ddr_t2t_processing',
                                      max_size=multiprocessing.cpu_count() * STORAGE_CHUNK_SIZE,
                                      job_timeout=20)

        queue_storage = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|ddr_storage',
                                   max_size=int(STORAGE_CHUNK_SIZE*STORAGE_WORKERS*1.2),
                                   job_timeout=20)
        '''start shared workers'''
        q_reporter = RedisQueueStatusReporter([d2d_queue_loading,
                                               d2d_queue_processing,
                                               t2t_queue_loading,
                                               t2t_queue_processing,
                                               queue_storage],
                                              interval=60)
        q_reporter.start()

        storage_workers = [DistanceStorageWorker(queue_storage,
                                                 self.es,
                                                 # ) for i in range(multiprocessing.cpu_count())]
                                                 ) for i in range(STORAGE_WORKERS)]

        for w in storage_workers:
            w.start()

        '''start workers for d2d'''
        disease_keys = disease_data.keys()
        disease_labels = self.es_query.get_disease_labels(disease_keys)
        d2d_workers = [DistanceComputationWorker(d2d_queue_processing,
                                                 [],
                                                 queue_storage,
                                                 RelationType.SHARED_TARGET,
                                                 ) for i in range(multiprocessing.cpu_count())]
        for w in d2d_workers:
            w.start()

        d2d_loader_workers = [MatrixIteratorWorker(d2d_queue_loading,
                                                   self.r_server.db,
                                                   d2d_queue_processing,
                                                   disease_data,
                                                   disease_keys,
                                                   disease_labels,
                                                   ) for i in range(multiprocessing.cpu_count())]

        for w in d2d_loader_workers:
            w.start()

        ''' compute disease to disease distances'''
        logging.info('Starting to push pairs for disease to disease distances computation')
        for i in range(len(disease_keys)):
            d2d_queue_loading.put(i, self.r_server)
        logging.info('disease to disease distances pair push done')

        '''stop d2d specifc workers'''
        d2d_queue_loading.set_submission_finished(self.r_server)
        for w in d2d_loader_workers:
            w.join()
        d2d_queue_processing.set_submission_finished(self.r_server)
        for w in d2d_workers:
            w.join()

        '''start workers for t2t'''
        target_keys = target_data.keys()
        target_labels = self.es_query.get_target_labels(target_keys)
        t2t_workers = [DistanceComputationWorker(t2t_queue_processing,
                                                 filtered_diseases,
                                                 queue_storage,
                                                 RelationType.SHARED_DISEASE,
                                                 ) for i in range(multiprocessing.cpu_count())]
        for w in t2t_workers:
            w.start()
        t2t_loader_workers = [MatrixIteratorWorker(t2t_queue_loading,
                                                   self.r_server.db,
                                                   t2t_queue_processing,
                                                   target_data,
                                                   target_keys,
                                                   target_labels,
                                                   ) for i in range(multiprocessing.cpu_count())]

        for w in t2t_loader_workers:
            w.start()

        ''' compute target to target distances'''
        logging.info('Starting to push pairs for target to target distances computation')
        for i in range(len(target_keys)):
            t2t_queue_loading.put(i, self.r_server)
        logging.info('target to target distances pair push done')

        '''stop t2t specifc workers'''
        t2t_queue_loading.set_submission_finished(self.r_server)
        for w in t2t_loader_workers:
            w.join()
        t2t_queue_processing.set_submission_finished(self.r_server)
        for w in t2t_workers:
            w.join()

        '''stop storage workers'''
        queue_storage.set_submission_finished(self.r_server)
        for w in storage_workers:
            w.start()


    def get_hot_node_blacklist(self, data):
        c = Counter()
        for k,v in data.items():
            c[k]=len(v)

        logging.info('Most common diseases: %s'%c.most_common(10))
        return [i[0] for i in c.most_common(10)]

