import logging
import pprint
import multiprocessing
from sqlalchemy import and_, func
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import joinedload, subqueryload
import time
from common import Actions
from common.DataStructure import JSONSerializable
from common.ElasticsearchLoader import JSONObjectStorage
from common.PGAdapter import ElasticsearchLoad, TargetToDiseaseAssociationScoreMap, Adapter
from modules.EvidenceString import Evidence
from settings import Config
from multiprocessing import Pool, Process, Queue, Value

__author__ = 'andreap'
import math





class ScoringActions(Actions):
    EXTRACT='extract'
    PROCESS='process'
    UPLOAD='upload'


class ScoringMethods():
    HARMONIC_SUM ='harmonic-sum'
    SUM = 'sum'
    MAX = 'max'

def millify(n):
    try:
        n = float(n)
        millnames=['','K','M','G','P']
        millidx=max(0,min(len(millnames)-1,
                          int(math.floor(math.log10(abs(n))/3))))
        return '%.1f%s'%(n/10**(3*millidx),millnames[millidx])
    except:
        return n

class AssociationScore(JSONSerializable):

    def __init__(self):

        self.overall = 0.0
        self.evidence_count = 0
        self.init_scores()

    def init_scores(self):
        self.datatypes={}
        self.datatype_evidence_count={}
        self.datasources={}
        self.datasource_evidence_count={}

        for ds,dt in Config.DATASOURCE_TO_DATATYPE_MAPPING.items():
            self.datasources[ds]=0.0
            self.datatypes[dt]=0.0
            self.datasource_evidence_count[ds]=0
            self.datatype_evidence_count[dt]=0

class AssociationScoreSet(JSONSerializable):

    def __init__(self, target, disease):
        self.target = target
        self.disease = disease
        for method_key, method in ScoringMethods.__dict__.items():
            if not method_key.startswith('_'):
                self.set_method(method,AssociationScore())
    def get_method(self, method):
        if method not in ScoringMethods.__dict__.values():
            raise AttributeError("method need to be a valid ScoringMethods")
        return self.__dict__[method]

    def set_method(self, method, score):
        if method not in ScoringMethods.__dict__.values():
            raise AttributeError("method need to be a valid ScoringMethods")
        if not isinstance(score, AssociationScore):
            raise AttributeError("score need to be an instance of AssociationScore")
        self.__dict__[method] = score

class EvidenceScore():
    def __init__(self, evidence_string):
        e = Evidence(evidence_string).evidence
        self.score = e['scores']['association_score']
        self.datatype = e['type']
        self.datasource = e['sourceID']

class HarmonicSumScorer():

    def __init__(self, buffer = 1000):
        '''
        will get every score,
        keep in memory the top max number
        calculate the score on those
        :return:calculated score
        '''
        self.buffer = buffer
        self.data = [0.]*buffer
        self.refresh()
        self.total = 0

    def add(self, score):
        self.total+=1
        if score >self.min:
            for i, old_score in enumerate(self.data):
                if old_score==self.min:
                    self.data[i] = score
                    break
            self.refresh()

    def refresh(self):
        self.min = min(self.data)

    def score(self):
        return self.harmonic_sum(self.data)

    @staticmethod
    def harmonic_sum(data):
        data.sort(reverse=True)
        return sum(s/(i+1) for i,s in enumerate(data))




class Scorer():

    def __init__(self):
        pass

    def score(self,target, disease, evidence_scores, method  = None):

        score = AssociationScoreSet(target,disease)

        if (method == ScoringMethods.HARMONIC_SUM) or (method is None):
            self._harmonic_sum(evidence_scores, score)
        if (method == ScoringMethods.SUM) or (method is None):
            self._sum(evidence_scores, score)
        if (method == ScoringMethods.MAX) or (method is None):
            self._max(evidence_scores, score)

        return score

    def _harmonic_sum(self, evidence_scores, score, max_entries = 1000):
        har_sum_score = score.get_method(ScoringMethods.HARMONIC_SUM)
        datasource_scorers = {}
        for e in evidence_scores:
            har_sum_score.evidence_count+=1
            har_sum_score.datatype_evidence_count[e.datatype]+=1
            har_sum_score.datasource_evidence_count[e.datasource]+=1
            if e.datasource not in datasource_scorers:
                datasource_scorers[e.datasource]=HarmonicSumScorer(buffer=max_entries)
            datasource_scorers[e.datasource].add(e.score)
        '''compute datasource scores'''
        for datasource in datasource_scorers:
            har_sum_score.datasources[e.datasource]=datasource_scorers[e.datasource].score()
        '''compute datatype scores'''
        #TODO
        '''compute overall scores'''
        #TODO

        return score

    def _sum(self, evidence_scores, score):
        sum_score = score.get_method(ScoringMethods.SUM)
        for e in evidence_scores:
            sum_score.overall+=e.score
            sum_score.evidence_count+=1
            sum_score.datatypes[e.datatype]+=e.score
            sum_score.datatype_evidence_count[e.datatype]+=1
            sum_score.datasources[e.datasource]+=e.score
            sum_score.datasource_evidence_count[e.datasource]+=1

        return

    def _max(self, evidence_score, score):
        max_score = score.get_method(ScoringMethods.MAX)
        for e in evidence_score:
            if e.score > max_score.datasources[e.datasource]:
                max_score.datasources[e.datatype] = e.score
                if e.score > max_score.datatypes[e.datatype]:
                    max_score.datatypes[e.datatype]=e.score
                    if e.score > max_score.overall:
                        max_score.overall=e.score
            max_score.evidence_count+=1
            max_score.datatype_evidence_count[e.datatype]+=1
            max_score.datasource_evidence_count[e.datasource]+=1

        return




class ScoringExtract():

    def __init__(self,
                 adapter):
        self.adapter=adapter
        self.session=adapter.session

    def extract(self):
        '''
        iterate over all evidence strings available and extract the target to disease mappings used to calculate the scoring
        :return:
        '''

        rows_deleted = self.session.query(
                TargetToDiseaseAssociationScoreMap).delete(synchronize_session='fetch')

        if rows_deleted:
            logging.info('deleted %i rows from elasticsearch_load' % rows_deleted)
        c, i = 0, 0
        for row in self.session.query(ElasticsearchLoad.data).filter(and_(
                        ElasticsearchLoad.index.like(Config.ELASTICSEARCH_DATA_INDEX_NAME+'%'),
                        ElasticsearchLoad.active==True,
                        )
                    ).yield_per(100):
            c+=1
            evidence = Evidence(row.data).evidence
            for efo in evidence['_private']['efo_codes']:
                i+=1
                self.session.add(TargetToDiseaseAssociationScoreMap(
                                              target_id=evidence['target']['id'],
                                              disease_id=efo,
                                              evidence_id=evidence['id'],
                                              is_direct=efo==evidence['disease']['id'],
                                              association_score=evidence['scores']['association_score'],
                                              datasource=evidence['sourceID'],
                                              ))
            if c % 100 == 0:
                self.session.flush()
            if i % 10000 == 0:
                logging.info("%i rows inserted to score-data table, %i evidence strings analysed" %(i, c))

        self.session.commit()

class ScoreStorer():
    def __init__(self, adapter, chunk_size=100):

        self.adapter=adapter
        self.session=adapter.session
        self.chunk_size = chunk_size
        self.cache = []
        self.counter = 0

    def put(self, score):

        self.cache.append(score)
        self.counter +=1
        if (len(self.cache) % self.chunk_size) == 0:
            self.flush()


    def flush(self):

        #TODO: store in postgres
        for i,data in enumerate(self.cache):
            # pass
            data = data.to_json()
            # logging.debug()

        if (self.counter % self.chunk_size) == 0:
            logging.info("%i precalculated scores inserted in elasticsearch_load table" %(self.counter))

        self.session.flush()
        self.cache = []


    def close(self):

        self.flush()
        self.session.commit()


    def __enter__(self):
        return self


    def __exit__(self, type, value, traceback):
        self.close()

class EvidenceGetterQueueReporter(Process):
    def __init__(self, task_q, result_q, ):
        super(EvidenceGetterQueueReporter, self).__init__()
        self.task_q= task_q
        self.result_q= result_q



    def run(self):
        logging.info("reporter worker started")
        while 1:
            try:
                time.sleep(1)
                logging.info("data to process: %i"%self.task_q.qsize())
                logging.info("results processed: %i"%self.result_q.qsize())
                if self.tasks_q.empty():
                    break
            except:
                logging.error("reporter error")

        logging.info("reporter worker stopped")


class EvidenceGetter(Process):
    def __init__(self, task_q, result_q, id, global_count, start_time):
        super(EvidenceGetter, self).__init__()
        self.task_q= task_q
        self.result_q= result_q
        self.adapter=Adapter()
        self.session=self.adapter.session
        # self.name = str(name)
        self.global_count = global_count
        self.start_time = start_time


    def run(self):
        logging.info("worker %s started"%self.name)
        for data in iter(self.task_q.get, None):
            target, disease = data

            evidence  =[]

            query ="""SELECT COUNT(pipeline.target_to_disease_association_score_map.evidence_id)
                        FROM pipeline.target_to_disease_association_score_map
                          WHERE pipeline.target_to_disease_association_score_map.target_id = '%s'
                          AND pipeline.target_to_disease_association_score_map.disease_id = '%s'"""%(target, disease)
            is_associated = self.session.execute(query).fetchone().count


            if is_associated:

                evidence_id_subquery = self.session.query(
                                                    TargetToDiseaseAssociationScoreMap.evidence_id
                                                       )\
                                                .filter(
                                                    and_(
                                                        TargetToDiseaseAssociationScoreMap.target_id == target,
                                                        TargetToDiseaseAssociationScoreMap.disease_id == disease,
                                                        )
                                                    ).subquery()
                evidence = [EvidenceScore(row.data)
                                    for row in self.session.query(ElasticsearchLoad.data).filter(ElasticsearchLoad.id.in_(evidence_id_subquery))\
                                    .yield_per(10000)
                                ]
            self.task_q.task_done()
            self.result_q.put((target, disease, evidence))
            self.global_count.value +=1
            count = self.global_count.value
            if count %10000 == 0:
               logging.info('target-disease pair analysed: %s \n analysis rate: %1.2f pair per second'%(millify(count), millify(count/(time.time()-self.start_time))))

class ScoreStorerWorker(Process):
    def __init__(self, task_q, result_q, name):
        super(EvidenceGetter, self).__init__()
        self.task_q= task_q
        self.result_q= result_q
        self.adapter=Adapter()
        self.session=self.adapter.session
        # self.name = str(name)


    def run(self):
        logging.info("worker %s started"%self.name)
        for data in iter(self.task_q.get, None):
            target, disease = data

            evidence  =[]

            query ="""SELECT COUNT(pipeline.target_to_disease_association_score_map.evidence_id)
                        FROM pipeline.target_to_disease_association_score_map
                          WHERE pipeline.target_to_disease_association_score_map.target_id = '%s'
                          AND pipeline.target_to_disease_association_score_map.disease_id = '%s'"""%(target, disease)
            is_associated = self.session.execute(query).fetchone().count


            if is_associated:

                evidence_id_subquery = self.session.query(
                                                    TargetToDiseaseAssociationScoreMap.evidence_id
                                                       )\
                                                .filter(
                                                    and_(
                                                        TargetToDiseaseAssociationScoreMap.target_id == target,
                                                        TargetToDiseaseAssociationScoreMap.disease_id == disease,
                                                        )
                                                    ).subquery()
                evidence = [EvidenceScore(row.data)
                                    for row in self.session.query(ElasticsearchLoad.data).filter(ElasticsearchLoad.id.in_(evidence_id_subquery))\
                                    .yield_per(10000)
                                ]
            self.task_q.task_done()
            self.result_q.put((target, disease, evidence))
class ScoringProcess():

    def __init__(self,
                 adapter):
        self.adapter=adapter
        self.session=adapter.session
        self.scorer = Scorer()

    def process_all(self):
        self.score_target_disease_pairs()

    def score_target_disease_pairs(self):
        # with ScoreStorer(self.adapter) as storer:
        target_total = self.session.query(ElasticsearchLoad.id).filter(and_(
                        ElasticsearchLoad.index==Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                        ElasticsearchLoad.type==Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                        ElasticsearchLoad.active==True,
                        )
                    ).count()
        disease_total = self.session.query(ElasticsearchLoad.id).filter(and_(
                        ElasticsearchLoad.index==Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME,
                        ElasticsearchLoad.type==Config.ELASTICSEARCH_EFO_LABEL_DOC_NAME,
                        ElasticsearchLoad.active==True,
                        )
                    ).count()
        estimated_total = target_total*disease_total
        logging.info("%s targets available | %s diseases available | %s estimated combinations to precalculate"%(millify(target_total),
                                                                                                                millify(disease_total),
                                                                                                                millify(estimated_total)))

        c=0.
        combination_with_data = 0.
        self.start_time = time.time()
        tasks_q = multiprocessing.JoinableQueue()
        result_q = multiprocessing.Queue()
        # reporter = EvidenceGetterQueueReporter(tasks_q, result_q)
        # reporter.start()
        evidence_got_count =  Value('i', 0)
        consumers = [EvidenceGetter(tasks_q,
                                    result_q,
                                    i,
                                    evidence_got_count,
                                    self.start_time,
                                    ) for i in range(multiprocessing.cpu_count()*4)]
        for w in consumers:
            w.start()
        total_jobs = 0
        for target_row in self.session.query(ElasticsearchLoad.id).filter(and_(
                        ElasticsearchLoad.index==Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                        ElasticsearchLoad.type==Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                        ElasticsearchLoad.active==True,
                        # ElasticsearchLoad.id == 'ENSG00000113448',
                        )
                    )[:3000]:
            target = target_row.id
            for disease_row in self.session.query(ElasticsearchLoad.id).filter(and_(
                        ElasticsearchLoad.index==Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME,
                        ElasticsearchLoad.type==Config.ELASTICSEARCH_EFO_LABEL_DOC_NAME,
                        ElasticsearchLoad.active==True,
                        # ElasticsearchLoad.id =='EFO_0000270',
                        )
                    )[:2000]:
                disease = disease_row.id
                tasks_q.put((target, disease))
                total_jobs +=1
            if total_jobs % 10000 ==0:
                logging.info('%s tasks loaded'%(millify(total_jobs)))
                # try: #avoid mac os error
                #     logging.info('queue size: %s'%(millify(tasks_q.qsize())))
                # except NotImplementedError:
                #     pass
        for w in consumers:
            tasks_q.put(None)#kill consumers when done
        confirmed_total = total_jobs
        logging.info("task loading done: %s loaded in %is"%(millify(total_jobs), time.time()-self.start_time))
        ''' wait for all the jobs to complete'''
        # tasks_q.join()
        with ScoreStorer(self.adapter) as storer:
            while total_jobs:
                c+=1
                total_jobs -=1
                data = result_q.get()
                target, disease, evidence = data
                # evidence=evidence.get(timeout=180)
                if evidence:
                    score = self.scorer.score(target, disease, evidence)
                    combination_with_data +=1
                    storer.put(score)
                    # print c,round(c/estimated_total,2), target, disease, len(evidence)
                if c%10000 ==0:
                    logging.info('%1.1f%% combinations computed, %s with data, %s remaining'%(c/confirmed_total*100,
                                                                                              millify(combination_with_data),
                                                                                              millify(total_jobs)))

                    # print c,round(estimated_total/c,2) target, disease, len(evidence)

        logging.info('%1.1f%% combinations computed, %s with data, sparse ratio: %1.3f%%'%(c/confirmed_total*100,
                                                                                        millify(combination_with_data),
                                                                                        (confirmed_total-combination_with_data)/confirmed_total*100))


    def _get_evidence_for_pair(self, target, disease):
        '''
        SELECT rs.Field1,rs.Field2
            FROM (
                SELECT Field1,Field2, Rank()
                  over (Partition BY Section
                        ORDER BY RankCriteria DESC ) AS Rank
                FROM table
                ) rs WHERE Rank <= 10
        '''
        evidence  =[]

        query ="""SELECT COUNT(pipeline.target_to_disease_association_score_map.evidence_id)
                    FROM pipeline.target_to_disease_association_score_map
                      WHERE pipeline.target_to_disease_association_score_map.target_id = '%s'
                      AND pipeline.target_to_disease_association_score_map.disease_id = '%s'"""%(target, disease)
        is_associated = self.session.execute(query).fetchone().count

        # is_associated = self.session.query(
        #                                         TargetToDiseaseAssociationScoreMap.evidence_id
        #                                            )\
        #                                     .filter(
        #                                         and_(
        #                                             TargetToDiseaseAssociationScoreMap.target_id == target,
        #                                             TargetToDiseaseAssociationScoreMap.disease_id == disease,
        #                                             )
        #                                         ).count()

        if is_associated:
            # evidence_ids = [row.evidence_id for row in self.session.query(
            #                                     TargetToDiseaseAssociationScoreMap.evidence_id
            #                                        )\
            #                                 .filter(
            #                                     and_(
            #                                         TargetToDiseaseAssociationScoreMap.target_id == target,
            #                                         TargetToDiseaseAssociationScoreMap.disease_id == disease,
            #                                         )
            #                                     )
            #             ]
            # query ="""SELECT pipeline.target_to_disease_association_score_map.evidence_id
            #             FROM pipeline.target_to_disease_association_score_map
            #             WHERE pipeline.target_to_disease_association_score_map.target_id = '%s'
            #             AND pipeline.target_to_disease_association_score_map.disease_id = '%s'"""%(target, disease)
            # evidence_ids = self.session.execute(query).fetchall()

            # evidence = [EvidenceScore(row.data)
            #                     for row in self.session.query(ElasticsearchLoad.data).filter(ElasticsearchLoad.id.in_(evidence_ids))\
            #                     .yield_per(10000)
            #                 ]

            evidence_id_subquery = self.session.query(
                                                TargetToDiseaseAssociationScoreMap.evidence_id
                                                   )\
                                            .filter(
                                                and_(
                                                    TargetToDiseaseAssociationScoreMap.target_id == target,
                                                    TargetToDiseaseAssociationScoreMap.disease_id == disease,
                                                    )
                                                ).subquery()
            evidence = [EvidenceScore(row.data)
                                for row in self.session.query(ElasticsearchLoad.data).filter(ElasticsearchLoad.id.in_(evidence_id_subquery))\
                                .yield_per(10000)
                            ]
        return evidence




class ScoringUploader():

    def __init__(self,
                 adapter,
                 loader):
        self.adapter=adapter
        self.session=adapter.session
        self.loader=loader

    def upload_all(self):
        self.clear_old_data()
        JSONObjectStorage.refresh_index_data_in_es(self.loader,
                                         self.session,
                                         Config.ELASTICSEARCH_DATA_SCORE_INDEX_NAME
                                         )
        self.loader.optimize_index(Config.ELASTICSEARCH_DATA_SCORE_INDEX_NAME+'*')

    def clear_old_data(self):
        self.loader.clear_index(Config.ELASTICSEARCH_DATA_SCORE_INDEX_NAME+'*')
