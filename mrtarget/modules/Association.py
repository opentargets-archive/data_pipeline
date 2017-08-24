import logging

from tqdm import tqdm
from mrtarget.common import TqdmToLogger

from mrtarget.common import Actions
from mrtarget.common.DataStructure import JSONSerializable, denormDict
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.common.LookupHelpers import LookUpDataRetriever, LookUpDataType
from mrtarget.common.Redis import RedisQueue, RedisQueueWorkerProcess, RedisQueueStatusReporter
from mrtarget.modules.EFO import EFO
from mrtarget.modules.HPA import HPAExpression, hpa2tissues
from mrtarget.modules.EvidenceString import Evidence, ExtendedInfoGene, ExtendedInfoEFO
from mrtarget.modules.GeneData import Gene
from mrtarget.Settings import Config


global_reporting_step = 5e5
logger = logging.getLogger(__name__)
tqdm_out = TqdmToLogger(logger,level=logging.INFO)


class AssociationActions(Actions):
    EXTRACT = 'extract'
    PROCESS = 'process'
    UPLOAD = 'upload'


class ScoringMethods():
    HARMONIC_SUM = 'harmonic-sum'
    SUM = 'sum'
    MAX = 'max'


class AssociationScore(JSONSerializable):

    def __init__(self):
        self.init_scores()

    def init_scores(self):
        """init scores to 0.0 and map to 2-maps init with 0.0"""
        self.overall = 0.0
        (self.datasources, self.datatypes) = denormDict(
            Config.DATASOURCE_TO_DATATYPE_MAPPING, (0.0, 0.0))


class Association(JSONSerializable):

    def __init__(self, target, disease, is_direct):
        self.target = {'id': target}
        self.disease = {'id': disease}
        self.is_direct = is_direct
        self.set_id()

        for method_key, method in ScoringMethods.__dict__.items():
            if not method_key.startswith('_'):
                self.set_scoring_method(method, AssociationScore())

        self.evidence_count = dict(total=0.0,
                                   datatypes={},
                                   datasources={})

        (self.evidence_count['datasources'],
         self.evidence_count['datatypes']) = denormDict(
            Config.DATASOURCE_TO_DATATYPE_MAPPING, (0.0, 0.0))

        self.private = {}
        self.private['facets'] = dict(datatype=[],
                                      datasource=[],
                                      free_text_search=[],
                                      expression_tissues=[])

    def get_scoring_method(self, method):
        if method not in ScoringMethods.__dict__.values():
            raise AttributeError("method need to be a valid ScoringMethods")
        return self.__dict__[method]

    def set_scoring_method(self, method, score):
        if method not in ScoringMethods.__dict__.values():
            raise AttributeError("method need to be a valid ScoringMethods")
        if not isinstance(score, AssociationScore):
            raise AttributeError("score need to be an instance"
                                 "of AssociationScore")
        self.__dict__[method] = score

    def set_id(self):
        self.id = '%s-%s' % (self.target['id'], self.disease['id'])

    def set_target_data(self, gene):
        """get generic gene info"""
        pathway_data = dict(pathway_type_code=[],
                            pathway_code=[])

        GO_terms = dict(biological_process=[],
                        cellular_component=[],
                        molecular_function=[],
                        )

        target_class = dict(level1=[],
                            level2=[])

        uniprot_keywords = []
        #TODO: handle domains
        genes_info=ExtendedInfoGene(gene)
        '''collect data to use for free text search'''

        for el in ['geneid', 'name', 'symbol']:
            self.private['facets']['free_text_search'].append(
                genes_info.data[el])

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
        if 'chembl' in gene.protein_classification and gene.protein_classification['chembl']:
            target_class['level1'].append([i['l1'] for i in gene.protein_classification['chembl'] if 'l1' in i])
            target_class['level2'].append([i['l2'] for i in gene.protein_classification['chembl'] if 'l2' in i])

        '''Add private objects used just for indexing'''

        if pathway_data['pathway_code']:
            self.private['facets']['reactome']= pathway_data
        if uniprot_keywords:
            self.private['facets']['uniprot_keywords'] = uniprot_keywords
        if GO_terms['biological_process'] or \
            GO_terms['molecular_function'] or \
            GO_terms['cellular_component'] :
            self.private['facets']['go'] = GO_terms
        if target_class['level1']:
            self.private['facets']['target_class'] = target_class

    def set_hpa_data(self, hpa):
        '''set a compat hpa expression data into the score object'''
        filteredHPA = hpa2tissues(hpa)
        if filteredHPA is not None and len(filteredHPA) > 0:
            self.private['facets']['expression_tissues'] = filteredHPA

    def set_disease_data(self, efo):
        """get generic efo info"""
        efo_info=ExtendedInfoEFO(efo)
        '''collect data to use for free text search'''
        self.private['facets']['free_text_search'].append(efo_info.data['efo_id'])
        self.private['facets']['free_text_search'].append(efo_info.data['label'])
        self.private['facets']['free_text_search'].extend(efo_info.data['therapeutic_area']['labels'])

        if efo_info:
            self.disease[ExtendedInfoEFO.root] = efo_info.data

    def set_available_datasource(self, ds):
        if ds not in self.private['facets']['datasource']:
            self.private['facets']['datasource'].append(ds)
            self.private['facets']['free_text_search'].append(ds)
    def set_available_datatype(self, dt):
        if dt not in self.private['facets']['datatype']:
            self.private['facets']['datatype'].append(dt)
            self.private['facets']['free_text_search'].append(dt)

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
        """
        An HarmonicSumScorer will ingest any number of numeric score, keep in memory the top max number
        defined by the buffer and calculate an harmonic sum of those
        Args:
            buffer: number of element to keep in memory to compute the harmonic sum
        """
        self.buffer = buffer
        self.data = []
        self.refresh()

    def add(self, score):
        """
        add a score to the pool of values
        Args:
            score (float):  a number to add to the pool ov values. is converted to float

        """
        score = float(score)
        if len(self.data)>= self.buffer:
            if score >self.min:
                self.data[self.data.index(self.min)] = score
                self.refresh()
        else:
            self.data.append(score)
            self.refresh()


    def refresh(self):
        """
        Store the minimum value of the pool

        """
        if self.data:
            self.min = min(self.data)
        else:
            self.min = 0.

    def score(self, *args,**kwargs):
        """
        Returns an harmonic sum for the pool of values
        Args:
            *args: forwarded to HarmonicSumScorer.harmonic_sum
        Keyword Args
            **kwargs: forwarded to HarmonicSumScorer.harmonic_sum
        Returns:
            harmonic_sum (float): the harmonic sum of the pool of values
        """
        return self.harmonic_sum(self.data, *args, **kwargs)

    @staticmethod
    def harmonic_sum(data,
                     scale_factor = 1,
                     cap = None):
        """
        Returns an harmonic sum for the data passed
        Args:
            data (list): list of floats to compute the harmonic sum from
            scale_factor (float): a scaling factor to multiply to each datapoint. Defaults to 1
            cap (float): if not None, never return an harmonic sum higher than the cap value.

        Returns:
            harmonic_sum (float): the harmonic sum of the data passed
        """
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

        ass = Association(target, disease, is_direct)

        # set evidence counts
        for e in evidence_scores:
            # make sure datatype is constrained
            if all([e.datatype in ass.evidence_count['datatypes'],
                    e.datasource in ass.evidence_count['datasources']]):
                ass.evidence_count['total']+=1
                ass.evidence_count['datatypes'][e.datatype]+=1
                ass.evidence_count['datasources'][e.datasource]+=1

                # set facet data
                ass.set_available_datatype(e.datatype)
                ass.set_available_datasource(e.datasource)

        # compute scores
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
            self.put_into_queue_out((key[0],key[1], evidence, is_direct))
        self.init_data_cache()

    def init(self):
        super(TargetDiseaseEvidenceProducer, self).init()
        self.es_query = ESQuery()

    def close(self):
        super(TargetDiseaseEvidenceProducer, self).close()




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
        self.lookup_data = lookup_data

    def init(self):
        super(ScoreProducer, self).init()
        self.scorer = Scorer()
        self.lookup_data.set_r_server(self.r_server)

    def process(self, data):
        target, disease, evidence, is_direct = data
        if evidence:
            score = self.scorer.score(target, disease, evidence, is_direct)
            if score.get_scoring_method(
                    ScoringMethods.HARMONIC_SUM).overall != 0:  # skip associations only with data with score 0
                gene_data = Gene()
                try:
                    gene_data.load_json(
                        self.lookup_data.available_genes.get_gene(target,
                                                                  self.r_server))
                except KeyError as e:
                    self.logger.debug('Cannot find gene code "%s" '
                                      'in lookup table' % target)
                    self.logger.exception(e)

                score.set_target_data(gene_data)

                # create a hpa expression empty jsonserializable class
                # to fill from Redis cache lookup_data
                hpa_data = HPAExpression()
                try:
                    hpa_data.load_json(
                        self.lookup_data.available_hpa.get_hpa(target,
                                                               self.r_server))
                    score.set_hpa_data(hpa_data)

                except KeyError:
                    pass

                except Exception as e:
                    self.logger.exception(e)

                disease_data = EFO()
                try:
                    disease_data.load_json(
                        self.lookup_data.available_efos.get_efo(disease, self.r_server))

                except KeyError as e:
                    self.logger.debug('Cannot find EFO code "%s" '
                                      'in lookup table' % disease)
                    self.logger.exception(e)

                score.set_disease_data(disease_data)

                if score: #bypass associations with overall score=0
                    return (target, disease, score.to_json())
                else:
                    logger.warning('Skipped association with score 0: %s-%s' % (target, disease))



class ScoreStorerWorker(RedisQueueWorkerProcess):
    def __init__(self,
                 score_q,
                 r_path,
                 chunk_size = 1e4,
                 dry_run = False,
                 es = None
                 ):
        super(ScoreStorerWorker, self).__init__(score_q, r_path)
        self.q = score_q
        self.chunk_size = chunk_size
        self.dry_run = dry_run
        self.es = None
        self.loader = None



    def process(self, data):
        if data is None:
            pass

        target, disease, score = data
        element_id = '%s-%s' % (target, disease)
        self.loader.put(Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME,
                               Config.ELASTICSEARCH_DATA_ASSOCIATION_DOC_NAME,
                               element_id,
                               score,
                               create_index=False,
                               # routing=score.target['id'],
                            )

    def init(self):
        super(ScoreStorerWorker, self).init()
        self.loader = Loader(chunk_size=self.chunk_size,
                             dry_run=self.dry_run)

    def close(self):
        super(ScoreStorerWorker, self).close()
        self.loader.close()


class ScoringProcess():

    def __init__(self,
                 loader,
                 r_server):
        self.es_loader = loader
        self.es = loader.es
        self.es_query = ESQuery(loader.es)
        self.r_server = r_server

    def process_all(self,
                    targets = [],
                    dry_run = False):
        self.score_target_disease_pairs(targets=targets,
                                        dry_run=dry_run)

    def score_target_disease_pairs(self,
                                   targets = [],
                                   dry_run = False):

        overwrite_indices = not dry_run
        if overwrite_indices:
            overwrite_indices = not bool(targets)


        lookup_data = LookUpDataRetriever(self.es,
                                          self.r_server,
                                          targets=targets,
                                          data_types=(
                                              LookUpDataType.DISEASE,
                                              LookUpDataType.TARGET,
                                              LookUpDataType.ECO,
                                              LookUpDataType.HPA
                                          ),
                                          autoload=True if not targets
                                          or len(targets) > 100
                                          else False).lookup

        if not targets:
            targets = list(self.es_query.get_all_target_ids_with_evidence_data())

        '''create queues'''
        number_of_workers = Config.WORKERS_NUMBER
        # too many storers
        number_of_storers = min(16, number_of_workers)
        queue_per_worker = 250
        if targets and len(targets) < number_of_workers:
            number_of_workers = len(targets)
        target_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|target_q',
                              max_size=number_of_workers*5,
                              job_timeout=3600,
                              r_server=self.r_server,
                              total=len(targets))
        target_disease_pair_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|target_disease_pair_q',
                                           max_size=queue_per_worker * number_of_workers,
                                           job_timeout=1200,
                                           batch_size=10,
                                           r_server=self.r_server,
                                           serialiser='pickle')
        score_data_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|score_data_q',
                                  max_size=queue_per_worker * number_of_storers,
                                  job_timeout=1200,
                                  batch_size=10,
                                  r_server=self.r_server,
                                  serialiser='pickle')

        q_reporter = RedisQueueStatusReporter([target_q,
                                               target_disease_pair_q,
                                               score_data_q
                                               ],
                                              interval=30,
                                              )
        q_reporter.start()


        '''create data storage workers'''
        storers = [ScoreStorerWorker(score_data_q,
                                     None,
                                     chunk_size=1000,
                                     dry_run = dry_run,
                                     ) for _ in range(number_of_storers)]

        for w in storers:
            w.start()

        scorers = [ScoreProducer(target_disease_pair_q,
                                 None,
                                 score_data_q,
                                 lookup_data,
                                 ) for _ in range(number_of_workers)]
        for w in scorers:
            w.start()


        '''start target-disease evidence producer'''
        readers = [TargetDiseaseEvidenceProducer(target_q,
                                                 None,
                                                 target_disease_pair_q,
                                                ) for _ in range(number_of_workers*2)]
        for w in readers:
            w.start()


        self.es_loader.create_new_index(Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME, recreate=overwrite_indices)
        self.es_loader.prepare_for_bulk_indexing(self.es_loader.get_versioned_index(Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME))



        for target in tqdm(targets,
                           desc='fetching evidence for targets',
                           unit=' targets',
                           file=tqdm_out,
                           unit_scale=True):
            target_q.put(target)
        target_q.set_submission_finished()

        '''wait for all workers to finish'''
        for w in readers:
            w.join()
        for w in scorers:
            w.join()
        for w in storers:
            w.join()

        logger.info('flushing data to index')
        self.es_loader.es.indices.flush('%s*'%Loader.get_versioned_index(Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME),
                                        wait_if_ongoing =True)


        logger.info("DONE")
