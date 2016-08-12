import logging
import multiprocessing
from elasticsearch import Elasticsearch


from tqdm import tqdm

from common import Actions
from common.DataStructure import JSONSerializable
from common.ElasticsearchLoader import Loader
from common.ElasticsearchQuery import ESQuery
from common.LookupHelpers import LookUpDataRetriever
from common.Redis import RedisQueue, RedisQueueWorkerProcess, RedisQueueStatusReporter
from modules.EFO import EFO
from modules.EvidenceString import Evidence, ExtendedInfoGene, ExtendedInfoEFO
from modules.GeneData import Gene
from settings import Config

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

        if 'facets' in gene._private and 'reactome' in gene._private['facets']:
            pathway_data['pathway_type_code'].extend(gene._private['facets']['reactome']['pathway_type_code'])
            pathway_data['pathway_code'].extend(gene._private['facets']['reactome']['pathway_code'])
        if gene.go:
            for item in gene.go:
                go_code, data = item['id'], item['value']
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

    def __bool__(self):
        return self.get_scoring_method(ScoringMethods.HARMONIC_SUM).overall != 0
    def __nonzero__(self):
        return self.__bool__()


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
            har_sum_score.datasources[datasource]=datasource_scorers[datasource].score(scale_factor=scale_factor, cap=1)
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




class TargetDiseaseEvidenceProducer(RedisQueueWorkerProcess):

    def __init__(self,
                 target_q,
                 r_path,
                 target_disease_pair_q,
                 ):
        super(TargetDiseaseEvidenceProducer, self).__init__(queue_in=target_q,
                                                            redis_path=r_path,
                                                            queue_out=target_disease_pair_q)

        self.es = Elasticsearch(Config.ELASTICSEARCH_URL, timeout = 10*60)
        self.es_query = ESQuery(self.es)
        self.target_disease_pair_q = target_disease_pair_q


    def process(self, data):
        target = data

        available_evidence = self.es_query.count_evidence_for_target(target)
        if available_evidence:
            self.init_data_cache()
            evidence_iterator = self.es_query.get_evidence_for_target_simple(target, available_evidence)
            # for evidence in tqdm(evidence_iterator,
            #                    desc='fetching evidence for target %s'%target,
            #                    unit=' evidence',
            #                    unit_scale=True,
            #                    total=available_evidence):
            for evidence in evidence_iterator:
                for efo in evidence['private']['efo_codes']:
                    key = (evidence['target']['id'], efo)
                    if key not in self.data_cache:
                        self.data_cache[key] = []
                    row = EvidenceScore(
                        score=evidence['scores']['association_score'] * Config.SCORING_WEIGHTS[evidence['sourceID']],
                        datatype=Config.DATASOURCE_TO_DATATYPE_MAPPING[evidence['sourceID']],
                        datasource=evidence['sourceID'],
                        is_direct=efo == evidence['disease']['id'])
                    self.data_cache[key].append(row)

            self.produce_pairs()

    def init_data_cache(self,):
        try:
            del self.data_cache
        except: pass
        self.data_cache = dict()

    def produce_pairs(self):
        for key,evidence in self.data_cache.items():
            is_direct = False
            for e in evidence:
                if e.is_direct:
                    is_direct = True
                    break
            self.target_disease_pair_q.put((key[0],key[1], evidence, is_direct))
        self.init_data_cache()




class ScoreProducer(RedisQueueWorkerProcess):

    def __init__(self,
                 evidence_data_q,
                 r_path,
                 score_q,
                 lookup_data
                 ):
        super(ScoreProducer, self).__init__(queue_in=evidence_data_q,
                                            redis_path=r_path,
                                            queue_out=score_q)
        self.evidence_data_q = evidence_data_q
        self.score_q = score_q
        self.scorer = Scorer()
        self.lookup_data = lookup_data

    def process(self, data):
        target, disease, evidence, is_direct = data
        if evidence:
            score = self.scorer.score(target, disease, evidence, is_direct)
            if score.get_scoring_method(
                    ScoringMethods.HARMONIC_SUM).overall != 0:  # skip associations only with data with score 0
                gene_data = Gene()
                gene_data.load_json(self.lookup_data.available_genes.get_gene(target))
                score.set_target_data(gene_data)
                disease_data = EFO()
                disease_data.load_json(self.lookup_data.available_efos.get_efo(disease))
                score.set_disease_data(disease_data)
                return (target, disease, score)



class ScoreStorerWorker(RedisQueueWorkerProcess):
    def __init__(self,
                 score_q,
                 r_path,
                 chunk_size = 1e4,
                 dry_run = False
                 ):
        super(ScoreStorerWorker, self).__init__(score_q, r_path)
        self.q = score_q
        self.chunk_size = chunk_size
        self.es = Elasticsearch(Config.ELASTICSEARCH_URL)
        self.loader = Loader(self.es,
                             chunk_size=self.chunk_size,
                             dry_run=dry_run)
        self.dry_run = dry_run


    def process(self, data):
        target, disease, score = data
        element_id = '%s-%s' % (target, disease)
        if score: #bypass associations with overall score=0
            self.loader.put(Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME,
                               Config.ELASTICSEARCH_DATA_ASSOCIATION_DOC_NAME,
                               element_id,
                               score.to_json(),
                               create_index=False,
                               routing=score.target['id'])
        else:
            logging.warning('Skipped association %s'%element_id)

    def close(self):
        self.loader.close()


class ScoringProcess():

    def __init__(self,
                 loader,
                 r_server):
        self.es = loader.es
        self.es_loader = loader
        self.es_query = ESQuery(self.es)
        self.r_server = r_server

    def process_all(self,
                    targets = [],
                    dry_run = False):
        self.score_target_disease_pairs(targets=targets,
                                        dry_run=dry_run)

    def score_target_disease_pairs(self,
                                   targets = [],
                                   dry_run = False):
        # # with ScoreStorer(self.adapter) as storer:

        # estimated_total = target_total*disease_total
        # logging.info("%s targets available | %s diseases available | %s estimated combinations to precalculate"%(millify(target_total),
        #                                                                                                         millify(disease_total),
        #                                                                                                         millify(estimated_total)))
        overwrite_indices = not dry_run
        if not dry_run:
            overwrite_indices = not bool(targets)
        if not targets:
            targets = list(self.es_query.get_all_target_ids_with_evidence_data())

        lookup_data = LookUpDataRetriever(self.es, self.r_server).lookup

        '''create queues'''
        number_of_workers = Config.WORKERS_NUMBER or multiprocessing.cpu_count()
        target_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|target_q',
                              max_size=number_of_workers*10,
                              job_timeout=3600,
                              r_server=self.r_server,
                              total=len(targets))
        target_disease_pair_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|target_disease_pair_q',
                                           max_size=10001,
                                           job_timeout=1200,
                                           r_server=self.r_server)
        score_data_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|score_data_q',
                                  max_size=10000,
                                  job_timeout=1200,
                                  r_server=self.r_server)

        q_reporter = RedisQueueStatusReporter([target_q,
                                               target_disease_pair_q,
                                               score_data_q
                                               ],
                                              interval=10,
                                              )
        q_reporter.start()


        '''create data storage workers'''
        storers = [ScoreStorerWorker(score_data_q,
                                     self.r_server.db,
                                     chunk_size=1000,
                                     dry_run = dry_run,
                                     ) for i in range(number_of_workers)]

        for w in storers:
            w.start()

        scorers = [ScoreProducer(target_disease_pair_q,
                                 self.r_server.db,
                                 score_data_q,
                                 lookup_data,
                                 ) for i in range(number_of_workers)]
        for w in scorers:
            w.start()


        '''start target-disease evidence producer'''
        readers = [TargetDiseaseEvidenceProducer(target_q,
                                                 self.r_server.db,
                                                 target_disease_pair_q,
                                                ) for i in range(number_of_workers*2)]
        for w in readers:
            w.start()


        if not dry_run:
            self.es_loader.create_new_index(Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME, recreate=overwrite_indices)



        for target in tqdm(targets,
                           desc='fetching evidence for targets',
                           unit=' targets',
                           unit_scale=True):
            target_q.put(target)

        '''wait for all workers to finish'''
        for w in readers:
            w.join()
        for w in scorers:
            w.join()
        for w in storers:
            w.join()

        logging.info('flushing data to index')
        self.es_loader.es.indices.flush('%s*'%Loader.get_versioned_index(Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME),
                                        wait_if_ongoing =True)


        logging.info("DONE")



