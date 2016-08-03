import json
import logging
from Queue import Empty
from multiprocessing.queues import SimpleQueue
import pprint
import multiprocessing
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionTimeout
from sqlalchemy import and_, func
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import joinedload, subqueryload, defer
import time

from tqdm import tqdm

from common import Actions
from common.DataStructure import JSONSerializable
from common.ElasticsearchLoader import JSONObjectStorage, Loader
from common.PGAdapter import ElasticsearchLoad, TargetToDiseaseAssociationScoreMap, Adapter, \
    TargetToDiseaseAssociationScoreMapAnalysed
from modules.EFO import EfoRetriever
from modules.EvidenceString import Evidence, ExtendedInfoGene, ExtendedInfoEFO
from modules.GeneData import GeneRetriever
from settings import Config
from multiprocessing import Pool, Process, Queue, Value

__author__ = 'andreap'
import math


global_reporting_step = 5e5


class AssociationActions(Actions):
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
        self.init_scores()

    def init_scores(self):
        self.datatypes={}
        self.datasources={}

        for ds,dt in Config.DATASOURCE_TO_DATATYPE_MAPPING.items():
            self.datasources[ds]=0.0
            self.datatypes[dt]=0.0

class Association(JSONSerializable):

    def __init__(self, target, disease, is_direct):
        self.target = {'id':target}
        self.disease = {'id':disease}
        self.is_direct=is_direct
        self.set_id()
        for method_key, method in ScoringMethods.__dict__.items():
            if not method_key.startswith('_'):
                self.set_scoring_method(method,AssociationScore())
        self.evidence_count = dict(total = 0.0,
                                   datatypes = {},
                                   datasources = {})
        for ds,dt in Config.DATASOURCE_TO_DATATYPE_MAPPING.items():
            self.evidence_count['datasources'][ds]=0.0
            self.evidence_count['datatypes'][dt]= 0.0
        self.private = {}
        self.private['facets']=dict(datatype = [],
                                    datasource = [])

    def get_scoring_method(self, method):
        if method not in ScoringMethods.__dict__.values():
            raise AttributeError("method need to be a valid ScoringMethods")
        return self.__dict__[method]

    def set_scoring_method(self, method, score):
        if method not in ScoringMethods.__dict__.values():
            raise AttributeError("method need to be a valid ScoringMethods")
        if not isinstance(score, AssociationScore):
            raise AttributeError("score need to be an instance of AssociationScore")
        self.__dict__[method] = score

    def set_id(self):
        self.id = '%s-%s'%(self.target['id'], self.disease['id'])

    def set_target_data(self, gene):
        """get generic gene info"""
        pathway_data = dict(pathway_type_code=[],
                            pathway_code=[])
        GO_terms = dict(biological_process = [],
                        cellular_component=[],
                        molecular_function=[],
                        )
        uniprot_keywords = []
        #TODO: handle domains
        genes_info=ExtendedInfoGene(gene)

        if 'reactome' in gene._private['facets']:
            pathway_data['pathway_type_code'].extend(gene._private['facets']['reactome']['pathway_type_code'])
            pathway_data['pathway_code'].extend(gene._private['facets']['reactome']['pathway_code'])
            # except Exception:
            #     logging.warning("Cannot get generic info for gene: %s" % aboutid)
        if gene.go:
            for go_code,data in gene.go.items():
                try:
                    category,term = data['term'][0], data['term'][2:]
                    if category =='P':
                        GO_terms['biological_process'].append(dict(code=go_code,
                                                                   term=term))
                    elif category =='F':
                        GO_terms['molecular_function'].append(dict(code=go_code,
                                                                   term=term))
                    elif category =='C':
                        GO_terms['cellular_component'].append(dict(code=go_code,
                                                                   term=term))
                except:
                    pass
        if gene.uniprot_keywords:
            uniprot_keywords = gene.uniprot_keywords

        if genes_info:
            self.target[ExtendedInfoGene.root] = genes_info.data

        if pathway_data['pathway_code']:
            pathway_data['pathway_type_code']=list(set(pathway_data['pathway_type_code']))
            pathway_data['pathway_code']=list(set(pathway_data['pathway_code']))



        '''Add private objects used just for indexing'''

        if pathway_data['pathway_code']:
            self.private['facets']['reactome']= pathway_data
        if uniprot_keywords:
            self.private['facets']['uniprot_keywords'] = uniprot_keywords
        if GO_terms['biological_process'] or \
            GO_terms['molecular_function'] or \
            GO_terms['cellular_component'] :
            self.private['facets']['go'] = GO_terms

    def set_disease_data(self, efo):
        """get generic efo info"""
        efo_info=ExtendedInfoEFO(efo)

        if efo_info:
            self.disease[ExtendedInfoEFO.root] = efo_info.data

    def set_available_datasource(self, ds):
        if ds not in self.private['facets']['datasource']:
            self.private['facets']['datasource'].append(ds)
    def set_available_datatype(self, dt):
        if dt not in self.private['facets']['datatype']:
            self.private['facets']['datatype'].append(dt)


class EvidenceScore():
    def __init__(self,
                 evidence_string = None,
                 score= None,
                 datatype = None,
                 datasource = None,
                 is_direct = None):
        if evidence_string is not None:
            e = Evidence(evidence_string).evidence
            self.score = e['scores']['association_score']
            self.datatype = e['type']
            self.datasource = e['sourceID']
        if score is not None:
            self.score = score
        if datatype is not None:
            self.datatype = datatype
        if datasource is not None:
            self.datasource = datasource
        self.is_direct = is_direct

class HarmonicSumScorer():

    def __init__(self, buffer = 100):
        '''
        will get every score,
        keep in memory the top max number
        calculate the score on those
        :return:calculated score
        '''
        self.buffer = buffer
        self.data = []
        self.refresh()

    def add(self, score):
        if len(self.data)>= self.buffer:
            if score >self.min:
                self.data[self.data.index(self.min)] = float(score)
                self.refresh()
        else:
            self.data.append(float(score))
            self.refresh()


    def refresh(self):
        if self.data:
            self.min = min(self.data)
        else:
            self.min = 0.

    def score(self, *args,**kwargs):
        return self.harmonic_sum(self.data, *args, **kwargs)

    @staticmethod
    def harmonic_sum(data,
                     scale_factor = 1,
                     cap = None):
        data.sort(reverse=True)
        harmonic_sum = sum(s / ((i+1) ** scale_factor) for i, s in enumerate(data))
        if cap is not None and \
                        harmonic_sum > cap:
            return cap
        return harmonic_sum





class Scorer():

    def __init__(self):
        pass

    def score(self,target, disease, evidence_scores, is_direct, method  = None):

        ass = Association(target,disease, is_direct)

        for e in evidence_scores:
            "set evidence counts"
            ass.evidence_count['total']+=1
            ass.evidence_count['datatypes'][e.datatype]+=1
            ass.evidence_count['datasources'][e.datasource]+=1

            "set facet data"
            ass.set_available_datatype(e.datatype)
            ass.set_available_datasource(e.datasource)

        "compute scores"
        if (method == ScoringMethods.HARMONIC_SUM) or (method is None):
            self._harmonic_sum(evidence_scores, ass, scale_factor=2)
        if (method == ScoringMethods.SUM) or (method is None):
            self._sum(evidence_scores, ass)
        if (method == ScoringMethods.MAX) or (method is None):
            self._max(evidence_scores, ass)

        return ass

    def _harmonic_sum(self, evidence_scores, ass, max_entries = 100, scale_factor = 1):
        har_sum_score = ass.get_scoring_method(ScoringMethods.HARMONIC_SUM)
        datasource_scorers = {}
        for e in evidence_scores:
            if e.datasource not in datasource_scorers:
                datasource_scorers[e.datasource]=HarmonicSumScorer(buffer=max_entries)
            datasource_scorers[e.datasource].add(e.score)
        '''compute datasource scores'''
        overall_scorer = HarmonicSumScorer(buffer=max_entries)
        for datasource in datasource_scorers:
            har_sum_score.datasources[datasource]=datasource_scorers[datasource].score(scale_factor=scale_factor)
            overall_scorer.add(har_sum_score.datasources[datasource])
        '''compute datatype scores'''
        datatypes_scorers = dict()
        for ds in har_sum_score.datasources:
            dt = Config.DATASOURCE_TO_DATATYPE_MAPPING[ds]
            if dt not in datatypes_scorers:
                datatypes_scorers[dt]=HarmonicSumScorer(buffer=max_entries)
            datatypes_scorers[dt].add(har_sum_score.datasources[ds])
        for datatype in datatypes_scorers:
            har_sum_score.datatypes[datatype]=datatypes_scorers[datatype].score(scale_factor=scale_factor)
        '''compute overall scores'''
        har_sum_score.overall = overall_scorer.score(scale_factor=scale_factor)

        return ass

    def _sum(self, evidence_scores, ass):
        sum_score = ass.get_scoring_method(ScoringMethods.SUM)
        for e in evidence_scores:
            sum_score.overall+=e.score
            sum_score.datatypes[e.datatype]+=e.score
            sum_score.datasources[e.datasource]+=e.score

        return

    def _max(self, evidence_score, ass):
        max_score = ass.get_scoring_method(ScoringMethods.MAX)
        for e in evidence_score:
            if e.score > max_score.datasources[e.datasource]:
                max_score.datasources[e.datatype] = e.score
                if e.score > max_score.datatypes[e.datatype]:
                    max_score.datatypes[e.datatype]=e.score
                    if e.score > max_score.overall:
                        max_score.overall=e.score

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
        step = 1e4
        logging.info('removing data')
        rows_deleted = self.session.query(
                TargetToDiseaseAssociationScoreMap).delete(synchronize_session=False)

        logging.info('removing data finished')
        if rows_deleted:
            logging.info('deleted %i rows from elasticsearch_load' % rows_deleted)
        c, i = 0, 0
        rows_to_insert =[]
        for row in self.session.query(ElasticsearchLoad.data).filter(and_(
                        ElasticsearchLoad.index.like(Config.ELASTICSEARCH_DATA_INDEX_NAME+'%'),
                        ElasticsearchLoad.active==True,
                        )
                    ).yield_per(step):
            c+=1

            evidence = Evidence(row.data).evidence
            for efo in evidence['_private']['efo_codes']:
                i+=1
                rows_to_insert.append(dict(target_id=evidence['target']['id'],
                                              disease_id=efo,
                                              evidence_id=evidence['id'],
                                              is_direct=efo==evidence['disease']['id'],
                                              association_score=evidence['scores']['association_score'],
                                              datasource=evidence['sourceID'],
                                              ))

            if i%step == 0:
                self.adapter.engine.execute(TargetToDiseaseAssociationScoreMap.__table__.insert(),rows_to_insert)
                del rows_to_insert
                rows_to_insert = []
            if i % step == 0:
                logging.info("%i rows inserted to score-data table, %i evidence strings analysed" %(i, c))
        self.adapter.engine.execute(TargetToDiseaseAssociationScoreMap.__table__.insert(),rows_to_insert)
        logging.info("%i rows inserted to score-data table, %i evidence strings analysed" %(i, c))


        try:
            self.session.commit()
        except:
            self.session.rollback()

class ScoreStorer():
    def __init__(self, adapter, es_loader, chunk_size=1e4):

        self.adapter=adapter
        self.session=adapter.session
        self.chunk_size = chunk_size
        self.cache = {}
        self.counter = 0
        self.es_loader = es_loader

    def put(self, id, score):

        # self.cache[id] = score
        self.counter +=1
        if (len(self.cache) % self.chunk_size) == 0:
            self.flush()
        self.es_loader.put(Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME,
                           Config.ELASTICSEARCH_DATA_ASSOCIATION_DOC_NAME,
                           id,
                           score.to_json(),
                           create_index = False,
                           routing = score.target['id'])


    def flush(self):


        if self.cache:
            JSONObjectStorage.store_to_pg_core(self.adapter,
                                              Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME,
                                              Config.ELASTICSEARCH_DATA_ASSOCIATION_DOC_NAME,
                                              self.cache,
                                              delete_prev=False,
                                              quiet=True,
                                             )
            self.counter+=len(self.cache)
            # if (self.counter % global_reporting_step) == 0:
            #     logging.info("%s precalculated scores inserted in elasticsearch_load table" %(millify(self.counter)))

            self.session.flush()
            self.cache = {}


    def close(self):

        self.flush()
        self.session.commit()


    def __enter__(self):
        return self


    def __exit__(self, type, value, traceback):
        self.close()

class StatusQueueReporter(Process):
    def __init__(self,
                 target_disease_pair_q,
                 score_data_q,
                 target_disease_pair_loading_finished,
                 score_computation_finished,
                 data_storage_finished,
                 target_disease_pairs_generated_count,
                 scores_computed,
                 scores_submitted_to_storage,
                 start_time
                 ):
        super(StatusQueueReporter, self).__init__()
        self.target_disease_pair_q= target_disease_pair_q
        self.score_data_q = score_data_q
        self.target_disease_pair_loading_finished = target_disease_pair_loading_finished
        self.score_computation_finished = score_computation_finished
        self.data_storage_finished = data_storage_finished
        self.target_disease_pairs_generated_count = target_disease_pairs_generated_count
        self.scores_computed = scores_computed
        self.scores_submitted_to_storage = scores_submitted_to_storage
        self.start_time = start_time
        self.score_computation_finish_time = None





    def run(self):
        logging.info("reporter worker started")
        while not (self.target_disease_pair_loading_finished.is_set() and
                   self.score_computation_finished.is_set() and
                   self.data_storage_finished.is_set()):
            # try:
            time.sleep(60*1)
            logging.info("""
=========== QUEUES ============
target_disease_pair_q: %s
score_data_q: %s
=========== EVENTS ============
target_disease_pair_loading_finished: %s
score_computation_finished: %s
data_storage_finished: %s
=========== STATS  ============
generated target-disease pairs: %s
scores computed: %s
scores computation rate: %s per sec
scores submitted for storage: %s
"""%(not self.target_disease_pair_q.empty(),
     not self.score_data_q.empty(),
     self.target_disease_pair_loading_finished.is_set(),
     self.score_computation_finished.is_set(),
     self.data_storage_finished.is_set(),
     millify(self.target_disease_pairs_generated_count.value),
     millify(self.scores_computed.value),
     millify(self.get_score_speed()),
     millify(self.scores_submitted_to_storage.value),
     ))
                # logging.info("data to process: %i"%self.task_q.qsize())
                # logging.info("results processed: %i"%self.result_q.qsize())
                # if self.tasks_q.empty():
                #     break
            # except:
            #     logging.error("reporter error")

            #  if c % global_reporting_step ==0:
            #     total_jobs = self.target_disease_pairs_generated_count.value
            #     logging.info('%1.1f%% combinations computed, %s with data, %s remaining'%(c/total_jobs*100,
            #                                                                               millify(combination_with_data),
            #                                                                               millify(total_jobs)))
            if self.target_disease_pair_loading_finished.is_set():
                if (self.scores_computed.value == self.target_disease_pairs_generated_count.value):
                    self.score_computation_finished.set()
                if (self.scores_submitted_to_storage.value == self.scores_computed.value):
                    self.data_storage_finished.set()

            if self.data_storage_finished.is_set():
                break

        logging.info("reporter worker stopped")

    def get_score_speed(self):
        if self.score_computation_finished.is_set():
            if self.score_computation_finish_time is None:
                self.score_computation_finish_time = time.time()
            finish_time = self.score_computation_finish_time
        else:
            finish_time = time.time()
        return self.scores_computed.value/(finish_time-self.start_time)


class EvidenceGetter(Process):
    def __init__(self,
                 task_q,
                 result_q,
                 global_count,
                 start_time,
                 signal_finish,
                 producer_done,
                 lock):
        super(EvidenceGetter, self).__init__()
        self.task_q= task_q
        self.result_q= result_q
        self.adapter=Adapter()
        self.session=self.adapter.session
        # self.name = str(name)
        self.global_count = global_count
        self.start_time = start_time
        self.signal_finish = signal_finish
        self.producer_done = producer_done
        self.lock=lock


    def run(self):
        logging.info("%s started"%self.name)
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
            # self.task_q.task_done()
            self.result_q.put((target, disease, evidence))
            with self.lock:
                self.global_count.value +=1
            count = self.global_count.value
            if count %global_reporting_step == 0:
               logging.info('target-disease pair analysed: %s | analysis rate: %s pairs per second'%(millify(count), millify(count/(time.time()-self.start_time))))

        logging.debug("%s finished"%self.name)
        if self.producer_done.is_set():
            self.signal_finish.set()



class TargetDiseasePairProducer(Process):

    def __init__(self,
                 task_q,
                 n_consumers,
                 start_time,
                 signal_finish,
                 pairs_generated,
                 lock):
        super(TargetDiseasePairProducer, self).__init__()
        self.q= task_q
        # self.adapter=Adapter()
        # self.session=self.adapter.session
        self.es = Elasticsearch(Config.ELASTICSEARCH_URL, timeout = 10*60)
        self.n_consumers=n_consumers
        self.start_time = start_time
        self.signal_finish = signal_finish
        self.pairs_generated = pairs_generated
        self.lock = lock


    def run(self):
        logging.info("%s started"%self.name)
        # total_assocaition_pairs = self.session.query(TargetToDiseaseAssociationScoreMap).count()
        # logging.info("starting to analyse %s association pairs"%(millify(total_assocaition_pairs)))
        self.total_jobs = 0
        self.init_data_cache()
        c=0
        last_gene = ''
        for row in tqdm(self._get_data_stream(), desc='fetching target-disease pairs', unit=' pairs', unit_scale=True):
            c+=1
            if row['target_id'] != last_gene:
                '''produce pairs'''
                self.produce_pairs()
                last_gene = row['target_id']
            key = (row['target_id'], row['disease_id'])
            if key not in self.data_cache:
                self.data_cache[key] =[]
            self.data_cache[key].append(self.get_score_from_row(row))
            if c%5e5 == 0:
                logging.info(('%s rows read'%millify(c)))
            # print c
            # if c==10000:
            #     break



        self.produce_pairs()
        # for w in range(self.n_consumers):
        #     self.q.put(None)#kill consumers when done
        logging.info("task loading done: %s loaded in %is"%(millify(self.total_jobs), time.time()-self.start_time))
        with self.lock:
            self.pairs_generated.value = self.total_jobs
        self.signal_finish.set()
        logging.debug("%s finished"%self.name)


    def _get_data_stream(self,page_size = 10000):
        local_data =dict()

        query_body = {
              "query": {"match_all": {} },
              '_source':  {"include": ["target.id",
                                       "private.efo_codes",
                                       "disease.id",
                                       "scores.association_score",
                                       "sourceID",
                                       "id",
                                       ],
                           },
              "sort":["target.id", "disease.id"],
          }


        res = helpers.scan(client=self.es,
                           query=query_body,
                           scroll='2h',
                           index=Config.RELEASE_VERSION+'_'+Config.ELASTICSEARCH_DATA_INDEX_NAME+'*',
                           timeout="2h",
                           request_timeout=2*60*60,
                           size = 5000,
                           preserve_order=True
        )

        c=0
        e=0
        for hit in res:
            hit = hit['_source']
            c+=1
            target_id = hit['target']['id']
            for efo in hit['private']['efo_codes']:
                yield dict(target_id=target_id,
                           disease_id=efo,
                           association_score=hit['scores']['association_score'],
                           datasource=hit['sourceID'],
                           is_direct=efo == hit['disease']['id'],
                           )
                e+=1
            if c%1e4==0:
                logging.debug("loaded %s evidencestrings and expanded to %s"%(millify(c),millify(e)))
        logging.info("finished loading %s evidencestrings and expanded to %s"%(millify(c),millify(e)))


    def init_data_cache(self,):
        try:
            del self.data_cache
        except: pass
        self.data_cache = dict()

    def produce_pairs(self):
        c=0
        for key,evidence in self.data_cache.items():
            c+=1
            is_direct = False
            for e in evidence:
                if e.is_direct:
                    is_direct = True
                    break
            self.q.put((key[0],key[1], evidence, is_direct))
        self.init_data_cache()
        self.total_jobs +=c
        with self.lock:
            self.pairs_generated.value =  self.total_jobs

    def get_score_from_row(self, row):
        return EvidenceScore(score = row['association_score']*Config.SCORING_WEIGHTS[row['datasource']],
                             datatype= Config.DATASOURCE_TO_DATATYPE_MAPPING[row['datasource']],
                             datasource=row['datasource'],
                             is_direct=row['is_direct'])


class ScoreProducer(Process):

    def __init__(self,
                 evidence_data_q,
                 score_q,
                 start_time,
                 target_disease_pair_loading_finished,
                 signal_finish,
                 target_disease_pairs_generated_count,
                 global_counter,
                 lock):
        super(ScoreProducer, self).__init__()
        self.evidence_data_q = evidence_data_q
        self.score_q = score_q
        self.lock = lock
        self.adapter=Adapter()
        self.session=self.adapter.session
        self.start_time = start_time
        self.target_disease_pair_loading_finished = target_disease_pair_loading_finished
        self.signal_finish = signal_finish
        self.target_disease_pairs_generated_count = target_disease_pairs_generated_count
        self.global_counter = global_counter
        self.scorer = Scorer()
        self.gene_retriever = GeneRetriever(self.adapter)
        self.efo_retriever = EfoRetriever(self.adapter)


    def run(self):
        logging.info("%s started"%self.name)
        self.data_processing_started = False
        while not (((self.target_disease_pairs_generated_count ==  self.global_counter.value ) and
                        self.target_disease_pair_loading_finished.is_set()) or self.signal_finish.is_set()):

            try:
                data = self.evidence_data_q.get_nowait()
                if data:
                    self.signal_started()
                target, disease, evidence, is_direct = data
                if evidence:
                    score = self.scorer.score(target, disease, evidence, is_direct)
                    score.set_target_data(self.gene_retriever.get_gene(target))
                    score.set_disease_data(self.efo_retriever.get_efo(disease))
                    self.score_q.put((target, disease,score))
                with self.lock:
                    self.global_counter.value +=1
            except Empty:
                time.sleep(0.01)

        self.signal_finish.set()
        logging.debug("%s finished"%self.name)

    def signal_started(self):
        if self.data_processing_started == False:
            self.data_processing_started = True
            logging.info('starting calculating scores from %s'%self.name)


class ScoreStorerWorker(Process):
    def __init__(self,
                 score_q,
                 signal_finish,
                 score_computation_finished,
                 scores_submitted_to_storage,
                 target_disease_pairs_generated_count,
                 target_disease_pair_loading_finished,
                 lock,
                 chunk_size = 1e4
                 ):
        super(ScoreStorerWorker, self).__init__()
        self.q= score_q
        self.signal_finish = signal_finish
        self.score_computation_finished = score_computation_finished
        self.chunk_size=chunk_size
        self.lock = lock
        self.global_counter = scores_submitted_to_storage
        self.target_disease_pairs_generated_count = target_disease_pairs_generated_count
        self.target_disease_pair_loading_finished = target_disease_pair_loading_finished
        self.adapter=Adapter()
        self.session=self.adapter.session
        self.es = Elasticsearch(Config.ELASTICSEARCH_URL)

    def run(self):
        logging.info("worker %s started"%self.name)
        with Loader(self.es, chunk_size=self.chunk_size) as es_loader:
            with ScoreStorer(self.adapter, es_loader, chunk_size=self.chunk_size) as storer:
                while not (((self.target_disease_pairs_generated_count ==  self.global_counter.value ) and
                        self.target_disease_pair_loading_finished.is_set()) or self.signal_finish.is_set()):
                    try:
                        data = self.q.get_nowait()
                        target, disease, score = data
                        if score:
                            id = '%s-%s'%(target,disease)
                            storer.put(id,
                                       score)
                        with self.lock:
                                self.global_counter.value +=1
                    except Empty:
                        time.sleep(0.01)

        self.signal_finish.set()
        logging.debug("%s finished"%self.name)


class ScoringProcess():

    def __init__(self,
                 adapter,
                 es_loader):
        self.adapter=adapter
        self.session=adapter.session
        self.es_loader = es_loader
        # self.scorer = Scorer()

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
        '''create queues'''
        target_disease_pair_q = Queue(maxsize=10001)
        score_data_q = Queue(maxsize=10000)
        '''create events'''
        target_disease_pair_loading_finished = multiprocessing.Event()
        # evidence_data_retrieval_finished = multiprocessing.Event()
        score_computation_finished = multiprocessing.Event()
        data_storage_finished = multiprocessing.Event()
        '''create shared memory objects'''
        # evidence_got_count =  Value('i', 0)
        target_disease_pairs_generated_count =  Value('i', 0)
        scores_computed =  Value('i', 0)
        scores_submitted_to_storage =  Value('i', 0)
        '''create locks'''
        target_disease_pairs_generated_lock = multiprocessing.Lock()
        scores_computed_lock =  multiprocessing.Lock()
        scores_submitted_to_storage_lock =  multiprocessing.Lock()


        number_of_workers = Config.WORKERS_NUMBER or multiprocessing.cpu_count()
        number_of_workers = number_of_workers*2

        '''start target-disease pair producer'''
        target_disease_pair_producer = TargetDiseasePairProducer(target_disease_pair_q,
                                             number_of_workers,
                                             self.start_time,
                                             target_disease_pair_loading_finished,
                                             target_disease_pairs_generated_count,
                                             target_disease_pairs_generated_lock)
        target_disease_pair_producer.start()

        '''wait for preprocessing to finish'''
        while 1:
            if target_disease_pairs_generated_count.value >0:
                break
            time.sleep(1)
        '''start reporting worker'''
        reporter = StatusQueueReporter(target_disease_pair_q,
                                       score_data_q,
                                       target_disease_pair_loading_finished,
                                       score_computation_finished,data_storage_finished,
                                       target_disease_pairs_generated_count,
                                       scores_computed,
                                       scores_submitted_to_storage,
                                       self.start_time,
                                       )
        reporter.start()

        '''create scoring workers'''
        scorers = [ScoreProducer(target_disease_pair_q,
                                 score_data_q,
                                 self.start_time,
                                 target_disease_pair_loading_finished,
                                 score_computation_finished,
                                 target_disease_pairs_generated_count,
                                 scores_computed,
                                 scores_computed_lock,
                                 ) for i in range(number_of_workers)]
        for w in scorers:
            w.start()



        # JSONObjectStorage.delete_prev_data_in_pg(self.session,
        #                                  Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME,
        #                                  )
        self.session.commit()
        self.es_loader.create_new_index(Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME)
        #
        #
        #
        '''create data storage workers'''
        storers = [ScoreStorerWorker(score_data_q,
                                   data_storage_finished,
                                   score_computation_finished,
                                   scores_submitted_to_storage,
                                   target_disease_pairs_generated_count,
                                   target_disease_pair_loading_finished,
                                   scores_submitted_to_storage_lock,
                                   chunk_size=1000,
                                     ) for i in range(number_of_workers)]

        for w in storers:
            w.start()


        ''' wait for all the jobs to complete'''
        while not data_storage_finished.is_set():

            time.sleep(10)
        for w in scorers:
            if w.is_alive():
                w.terminate()
        for w in storers:
            if w.is_alive():
                time.sleep(1)
                w.terminate()
        reporter.terminate()
        logging.info('flushing data to index')
        self.es_loader.es.indices.flush('%s*'%Loader.get_versioned_index(Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME),
                                        wait_if_ongoing =True)


        logging.info("DONE")



