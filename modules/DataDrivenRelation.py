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

from common.Redis import RedisQueue, RedisQueueStatusReporter
from settings import Config



class Relation(JSONSerializable):
    type = ''

    def __init__(self,
                 subj_id,
                 obj_id,
                 scores,
                 type = ''):
        self.subject = dict(id = subj_id)
        self.object = dict(id=obj_id)
        self.scores = scores
        if type:
            self.type = type
        self._set_id(subj_id, obj_id)

    def _set_id(self, subj_id, obj_id):
        self.id = '-'.join([subj_id, obj_id])

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
                 type):
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
                    union_keys = set(data[1].keys()) | set(data[3].keys())
                    if self.filtered_keys:
                        union_keys = union_keys - self.filtered_keys # remove filtered keys if needed
                    if union_keys:
                        obj_id = data[2]
                        subj_id = data[0]
                        subj = [DataDrivenRelationProcess.cap_to_one(i) for i in [data[1][k] for k in union_keys]]
                        obj = [DataDrivenRelationProcess.cap_to_one(i) for i in [data[3][k] for k in union_keys]]
                        pos = len(set(data[1].keys()) & set(data[3].keys()))
                        neg = len(union_keys)
                        jackard = 0.
                        if neg:
                            jackard = float(pos)/neg
                        dist = {'euclidean': pdist([subj, obj])[0],
                                'jaccard': jackard,
                                'shared_count': pos,
                                'union_count': neg}
                        self.queue_out.put((subj_id, obj_id, dist, self.type), self.r_server)#TODO: create an object here
                except Exception, e:
                    error = True
                    logging.exception('Error processing key %s' % key)

                self.queue_in.done(key, error=error, r_server=self.r_server)

        logging.info('%s done processing'%self.name)

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
                with Loader(self.es) as loader:
                    while not self.queue_in.is_done(r_server=self.r_server):
                        job = self.queue_in.get(r_server=self.r_server, timeout=1)
                        if job is not None:
                            key, data = job
                            error = False
                            try:
                                subj_id, obj_id, dist, type = data
                                r1 = Relation(subj_id,obj_id, dist, type)
                                loader.put(Config.ELASTICSEARCH_RELATION_INDEX_NAME,
                                           Config.ELASTICSEARCH_RELATION_DOC_NAME+'-'+type,
                                           r1.id,
                                           r1.to_json(),
                                           create_index=False)
                                c += 1
                                if subj_id != obj_id:
                                    r2 = Relation(obj_id, subj_id, dist, type)
                                    loader.put(Config.ELASTICSEARCH_RELATION_INDEX_NAME,
                                               Config.ELASTICSEARCH_RELATION_DOC_NAME + '-' + type,
                                               r2.id,
                                               r2.to_json(),
                                               create_index=False)
                                    c += 1

                                #TODO: store in es here
                                Relation
                                if c%1000==0:
                                    logging.info('%i datapoint stored' % c)
                            except Exception, e:
                                error = True
                                logging.exception('Error processing key %s' % key)

                            self.queue_in.done(key, error=error, r_server=self.r_server)

                logging.info('%s done processing' % self.name)



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


        Loader(self.es).create_new_index(Config.ELASTICSEARCH_RELATION_INDEX_NAME)

        queue_processing = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|ddr_processing',
                           max_size=10000,
                           job_timeout=10)

        q_reporter = RedisQueueStatusReporter([queue_processing])
        q_reporter.start()

        queue_storage = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|ddr_storage',
                                   max_size=10000,
                                   job_timeout=10)

        q_reporter_storage = RedisQueueStatusReporter([queue_storage])
        q_reporter_storage.start()

        storage_workers = [DistanceStorageWorker(queue_storage,
                                                 self.es,
                                                 # ) for i in range(multiprocessing.cpu_count())]
                                                 ) for i in range(1)]

        for w in storage_workers:
            w.start()

        ''' compute disease to disease distances'''
        d2d_workers = [DistanceComputationWorker(queue_processing,
                                                 [],
                                                 queue_storage,
                                                 RelationType.SHARED_TARGET,
                                                 ) for i in range(multiprocessing.cpu_count())]
        for w in d2d_workers:
            w.start()

        logging.info('Starting to compute disease to disease distances')
        for pair in self.get_distance_pair(disease_data):
            queue_processing.put(pair, self.r_server)
        logging.info('disease to disease distances computation done')

        ''' compute target to target distances'''
        t2t_workers = [DistanceComputationWorker(queue_processing,
                                                 filtered_diseases,
                                                 queue_storage,
                                                 RelationType.SHARED_DISEASE,
                                                 ) for i in range(multiprocessing.cpu_count())]
        for w in t2t_workers:
            w.start()

        logging.info('Starting to compute disease to disease distances')
        for pair in self.get_distance_pair(target_data):
            queue_processing.put(pair, self.r_server)
        logging.info('disease to disease distances computation done')


        queue_processing.submission_done(self.r_server)

        while not queue_processing.is_done():
            time.sleep(0.1)
        queue_storage.submission_done(self.r_server)


        # ''' compute disease to disease distances'''
        # logging.info('Starting to compute disease to disease distances')
        # start_time = time.time()
        # disease_matrix = []
        # f = itemgetter(*available_targets)
        # c=0
        # for d in available_diseases:
        #     '''for every disease, get the default dict with data and transform it in a vector of float values keeping the same order of targets for all the diseases'''
        #     disease_matrix.append(map(self.cap_to_one,f(disease_data[d])))
        #     # print d, disease_data[d], disease_matrix[c-1][available_targets.index(disease_data[d].keys()[0])]
        #     del disease_data[d]
        #     c += 1
        #     if c%1000==0:
        #         print 'extracted', c
        #
        # disease_matrix = np.array(disease_matrix, copy=False)
        # logging.info('Computed disease to disease matrix %i s' % (time.time() - start_time))
        # logging.info('disease matrix size in memory: %f Kb' % (sys.getsizeof(disease_matrix) /1024.))
        # del disease_data #possibly free up some memory
        # logging.info('trying to remove disease data: %s' %(gc.collect()>1))
        # d2d_c = 0
        # vector_of_non_zero_target_counts = (disease_matrix != 0).sum(1) #array([0,1,10,0,10]) number of diseases sharing this target
        # # print list(vector_of_non_zero_target_counts)
        # # print (vector_of_non_zero_target_counts < 100)
        # print 'filter by 10', (vector_of_non_zero_target_counts < 10).sum()
        # print 'filter by 100', (vector_of_non_zero_target_counts < 100).sum()
        # print 'filter by 1000', (vector_of_non_zero_target_counts < 1000).sum()
        # print 'filter by 10000', (vector_of_non_zero_target_counts < 10000).sum()
        #
        # start_time = time.time()
        # for i in range(len(available_diseases))[:30]:
        #     for j in range(len(available_diseases)):
        #         if j>=i:
        #             subj = disease_matrix[i]
        #             obj = disease_matrix[j]
        #             if set(np.nonzero(subj)[0].tolist()) & set(np.nonzero(obj)[0].tolist()): # if they share a target
        #                 d2d_c+=1
        #                 dist = pdist([subj,obj])
        #                 if i < 10 and j < 10 and i!= j:
        #                     print dist
        #                     if dist == 0:
        #                         print subj == obj
        #                         print np.nonzero(subj)[0].tolist()
        #                         print np.nonzero(obj)[0].tolist()
        #             else:
        #                 # print 0.
        #                 pass
        #     print i+1, d2d_c, round(float(d2d_c)/(i+1)), round(float(d2d_c)/(time.time() - start_time))
        #     # sys.exit(0)
        #
        # # disease_dists = pdist(disease_matrix)
        # logging.info('Computed disease to disease distances in %i s' % (time.time() - start_time))
        # # print disease_dists
        # # logging.info('disease to disease distances matrix shape %s' % disease_dists.shape())
        #
        #
        # ''' compute target to target distances'''
        # logging.info('Starting to compute target to target distances')
        # start_time = time.time()
        # target_matrix = []
        # f = itemgetter(*available_diseases)
        # c = 0
        # for d in available_targets:
        #     c += 1
        #     '''for every disease, get the default dict with data and transform it in a vector of float values keeping the same order of targets for all the diseases'''
        #     target_matrix.append(np.array(map(self.cap_to_one, f(target_data[d]))))
        #     del target_data[d]
        #     if c % 1000 == 0:
        #         print 'extracted', c
        # logging.info('Computed target to disease matrix %i s' % (time.time() - start_time))
        # logging.info('target matrix size in memory: %f Kb' % (sys.getsizeof(target_matrix) / 1024.))
        # del target_data  # possibly free up some memory
        # logging.info('trying to remove target data: %s' % (gc.collect() > 1))
        # d2d_c = 0
        # start_time = time.time()
        # for i in range(len(available_targets))[:30]:
        #     for j in range(len(available_targets)):
        #         if j>=i:
        #             subj = target_matrix[i]
        #             obj = target_matrix[j]
        #             if set(np.nonzero(subj)[0].tolist()) & set(np.nonzero(obj)[0].tolist()):  # if they share a target
        #                 d2d_c += 1
        #                 dist = pdist([subj,obj])
        #                 if i<10 and j<10 and i != j:
        #                     print dist
        #                     if dist == 0:
        #                         print subj == obj
        #                         print np.nonzero(subj)[0].tolist()
        #                         print np.nonzero(obj)[0].tolist()
        #             else:
        #                 # print 0.
        #                 pass
        #     print i + 1, d2d_c, round(float(d2d_c) / (i + 1)), round(float(d2d_c)/(time.time() - start_time))
        #
        # logging.info('Computed disease to disease distances in %i s' % (time.time() - start_time))

        self.process_targets()
        self.process_diseases()

    def process_targets(self):
        pass

    def process_diseases(self):
        pass

    def get_distance_pair(self, data):
        keys = data.keys()
        space_size = (len(keys)**2)/2
        total = 0
        shared = 0
        logging.info('Space to explore: %i combinations'%space_size)
        for i in range(len(keys)):
            for j in range(len(keys)):
                if j>=i:
                    total += 1
                    subj_id = keys[i]
                    obj_id = keys[j]
                    if set(data[subj_id].keys()) & set(data[obj_id].keys()):
                        shared +=1
                        if shared % 10000 == 0:
                            logging.info('explored %i pairs (%.2f%%), loaded %i for computation, rate: %.2f%%'%(total, float(total)/space_size*100, shared, float(shared)/total*100))
                        yield subj_id, data[subj_id], obj_id, data[obj_id]

    # def get_distance_pair_batches(self,data, batch_size=10):
    #     batch = []
    #     for i in self.get_distance_pair(data):
    #         batch.append(i)
    #         if len(batch) == batch_size:
    #             yield batch
    #             batch = []



    def get_hot_node_blacklist(self, data):
        c = Counter()
        for k,v in data.items():
            c[k]=len(v)

        logging.info('Most common diseases: %s'%c.most_common(10))
        return [i[0] for i in c.most_common(10)]