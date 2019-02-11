import logging
import copy

import functional
import functools
import itertools
from collections import defaultdict

from mrtarget.Settings import Config
from mrtarget.constants import Const
from mrtarget.common.DataStructure import JSONSerializable
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.common.connection import new_es_client, new_redis_client
from mrtarget.common.LookupHelpers import LookUpDataRetriever, LookUpDataType
from mrtarget.common.Scoring import ScoringMethods, HarmonicSumScorer
from mrtarget.modules.EFO import EFO
from mrtarget.modules.EvidenceString import Evidence, ExtendedInfoGene, ExtendedInfoEFO
from mrtarget.modules.GeneData import Gene
from mrtarget.modules.HPA import HPAExpression, hpa2tissues

import pylru as lru
import pypeln.process as pr


class AssociationScore(JSONSerializable):

    def __init__(self, datasources, datatypes):
        self.datasources = {}
        for datasource in datasources:
            self.datasources[datasource] = 0.0
        self.datatypes = {}
        for datatype in datatypes:
            self.datatypes[datatype] = 0.0


class Association(JSONSerializable):

    def __init__(self, target, disease, is_direct, datasources, datatypes):
        self.target = {'id': target}
        self.disease = {'id': disease}
        self.is_direct = is_direct
        self.set_id()

        for method_key, method in ScoringMethods.__dict__.items():
            if not method_key.startswith('_'):
                self.set_scoring_method(method, AssociationScore(datasources, datatypes))

        self.evidence_count = dict(total=0.0,
                                   datatypes={},
                                   datasources={})

        self.evidence_count['datasources'] = {}
        for datasource in datasources:
            self.evidence_count['datasources'][datasource] = 0.0

        self.evidence_count['datatypes'] = {}
        for datatype in datatypes:
            self.evidence_count['datatypes'][datatype] = 0.0




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

    def _inject_tractability_in_target(self, gene_obj):
        def _create_facet(categories_dict):
            if isinstance(categories_dict, dict):
                return functional.seq(categories_dict.viewitems())\
                    .filter(lambda e: e[1] > 0)\
                    .map(lambda e: e[0]).to_list()
            else:
                return []

        def _merge_facets(the_dict):
            if isinstance(the_dict, dict):
                return functional.seq(the_dict.viewitems())\
                    .flat_map(lambda e: itertools.imap(lambda el: e[0] + '_' + el,e[1]))\
                    .to_list()
            else:
                return []

        if gene_obj:
            # inject tractability data from the gene into the assoc
            trac_fields = ["smallmolecule", "antibody"]
            # trac_subfields = ["buckets", "categories"]

            if gene_obj.tractability:
                # we do have tractability data
                self.target['tractability'] = copy.deepcopy(gene_obj.tractability)

                # build facet for the types
                self.private['facets']['tractability'] = \
                    {cat: _create_facet(gene_obj.tractability[cat]['categories']) for cat in trac_fields}

                self.private['facets']['tractability']['combined'] = \
                    _merge_facets(self.private['facets']['tractability'])

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

        self._inject_tractability_in_target(gene)

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
    def __init__(self, score, datatype, datasource, is_direct):
        self.score = score
        self.datatype = datatype
        self.datasource = datasource
        self.is_direct = is_direct


class Scorer():
    '''
    Aggregates evidence for a given target-disease pair
    '''
    def __init__(self):
        pass

    def score(self,target, disease, evidence_scores, is_direct, datasources_to_datatypes):


        datasources = datasources_to_datatypes.keys()
        datatypes = set(datasources_to_datatypes.values())

        association = Association(target, disease, is_direct, datasources, datatypes)

        # set evidence counts
        for e in evidence_scores:
            # make sure datatype is constrained
            if all([e.datatype in association.evidence_count['datatypes'],
                    e.datasource in association.evidence_count['datasources']]):
                association.evidence_count['total']+=1
                association.evidence_count['datatypes'][e.datatype]+=1
                association.evidence_count['datasources'][e.datasource]+=1

                # set facet data
                association.set_available_datatype(e.datatype)
                association.set_available_datasource(e.datasource)

        # compute harmonic sum with quadratic (scale_factor) degradation
        #limit to first 100 entries and scale with afactor of 2
        self._harmonic_sum(evidence_scores, association, 100, 2, datasources_to_datatypes)

        return association

    def _harmonic_sum(self, evidence_scores, association, 
            max_entries, scale_factor, datasources_to_datatypes):
        har_sum_score = association.get_scoring_method(ScoringMethods.HARMONIC_SUM)
        datasource_scorers = {}
        for e in evidence_scores:
            if e.datasource not in datasource_scorers:
                datasource_scorers[e.datasource]= HarmonicSumScorer(buffer=max_entries)
            datasource_scorers[e.datasource].add(e.score)
        '''compute datasource scores'''
        overall_scorer = HarmonicSumScorer(buffer=max_entries)
        for datasource in datasource_scorers:
            '''cap datasource scores at this level so very big scores 
            do not take over smaller score around the range of 1'''
            har_sum_score.datasources[datasource]=datasource_scorers[datasource].score(scale_factor=scale_factor, cap=1)
            overall_scorer.add(har_sum_score.datasources[datasource])
        '''compute datatype scores'''
        datatypes_scorers = dict()
        for ds in har_sum_score.datasources:
            dt = datasources_to_datatypes[ds]
            if dt not in datatypes_scorers:
                datatypes_scorers[dt]= HarmonicSumScorer(buffer=max_entries)
            datatypes_scorers[dt].add(har_sum_score.datasources[ds])
        for datatype in datatypes_scorers:
            har_sum_score.datatypes[datatype]=datatypes_scorers[datatype].score(scale_factor=scale_factor)
        '''compute overall scores'''
        har_sum_score.overall = overall_scorer.score(scale_factor=scale_factor)

        return association

def produce_evidence_local_init(es_hosts, 
        scoring_weights, is_direct_do_not_propagate, datasources_to_datatypes):
    es = new_es_client(es_hosts)
    es_query = ESQuery(es)
    return es_query, scoring_weights, is_direct_do_not_propagate, datasources_to_datatypes

def produce_evidence(data, es_query, 
        scoring_weights, is_direct_do_not_propagate, datasources_to_datatypes):
    data_cache = {}
    target = data

    return_values = []

    available_evidence = es_query.count_evidence_for_target(target)
    if available_evidence:
        evidence_iterator = es_query.get_evidence_for_target_simple(target, available_evidence)
        for evidence in evidence_iterator:
            efo_list = [evidence['disease']['id']] \
                if evidence['sourceID'] in is_direct_do_not_propagate \
                else evidence['private']['efo_codes']

            for efo in efo_list:
                key = (evidence['target']['id'], efo)
                if key not in data_cache:
                    data_cache[key] = []

                data_source = evidence['sourceID']
                
                score = evidence['scores']['association_score']
                if data_source in scoring_weights:
                    score = score * scoring_weights[data_source]
                    
                is_direct = (efo == evidence['disease']['id'])
                data_type = datasources_to_datatypes[data_source]

                row = EvidenceScore(score, data_type, data_source, is_direct)
                data_cache[key].append(row)

        for key,evidence in data_cache.items():
            #if any of the evidence is direct, the assication is direct
            is_direct = False
            for e in evidence:
                if e.is_direct:
                    is_direct = True
                    break

            return_values.append((key[0],key[1], evidence, is_direct))

    return return_values

def produce_evidence_local_shutdown(status, es_query, 
        scoring_weights, is_direct_do_not_propagate, datasources_to_datatypes):
    pass


def score_producer_local_init(es_hosts, redis_host, redis_port, 
        lookup_data, datasources_to_datatypes, dry_run):

    #set the R server to lookup into
    r_server = new_redis_client(redis_host, redis_port)

    scorer = Scorer()
    
    lru_cache = lru.lrucache(10000)

    loader = Loader(new_es_client(es_hosts))

    return scorer, loader, r_server, lru_cache, lookup_data, datasources_to_datatypes, dry_run

def score_producer(data, 
        scorer, loader, r_server, lru_cache, lookup_data, datasources_to_datatypes, dry_run):
    target, disease, evidence, is_direct = data

    logger = logging.getLogger(__name__)

    if evidence:
        score = scorer.score(target, disease, evidence, is_direct, 
            datasources_to_datatypes)
        if score: # skip associations only with data with score 0

            # look for the gene in the lru cache
            gene_data = None
            hpa_data = None
            if target in lru_cache:
                gene_data, hpa_data = lru_cache[target]

            if not gene_data:
                gene_data = Gene()
                try:
                    gene_data.load_json(
                        lookup_data.available_genes.get_gene(target, r_server))

                except KeyError as e:
                    logger.debug('Cannot find gene code "%s" '
                                        'in lookup table' % target)
                    logger.exception(e)

            score.set_target_data(gene_data)

            # create a hpa expression empty jsonserializable class
            # to fill from Redis cache lookup_data
            if not hpa_data:
                hpa_data = HPAExpression()
                try:
                    hpa_data.update(
                        lookup_data.available_hpa.get_hpa(target, r_server))
                except KeyError:
                    pass
                except Exception as e:
                    logger.exception(e)

                # set everything in the lru_cache
                lru_cache[target] = (gene_data, hpa_data)

            try:
                score.set_hpa_data(hpa_data)
            except KeyError:
                pass
            except Exception as e:
                logger.exception(e)

            disease_data = EFO()
            try:
                disease_data.load_json(
                    lookup_data.available_efos.get_efo(disease, r_server))

            except KeyError as e:
                logger.debug('Cannot find EFO code "%s" '
                                    'in lookup table' % disease)
                logger.exception(e)

            score.set_disease_data(disease_data)


            element_id = '%s-%s' % (target, disease)
            if not dry_run:
                loader.put(Const.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME,
                    Const.ELASTICSEARCH_DATA_ASSOCIATION_DOC_NAME,
                    element_id, score)

        else:
            logger.warning('Skipped association with score 0: %s-%s' % (target, disease))

    return True

def score_producer_local_shutdown(status, 
        scorer, loader, r_server, lru_cache, lookup_data, datasources_to_datatypes, dry_run):

    #cleanup elasticsearch
    if not dry_run:
        loader.flush_all_and_wait(Const.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME)

class ScoringProcess():

    def __init__(self, redis_host, redis_port, es_hosts):

        self.logger = logging.getLogger(__name__)

        self.es_hosts = es_hosts
        self.es = new_es_client(self.es_hosts)
        self.es_loader = Loader(self.es)
        self.es_query = ESQuery(self.es)

        self.redis_host = redis_host
        self.redis_port = redis_port
        self.r_server = new_redis_client(self.redis_host, self.redis_port)

    def process_all(self, scoring_weights, is_direct_do_not_propagate,
            datasources_to_datatypes, dry_run, 
            num_workers_produce, num_workers_score, max_queued_produce_to_score):

        lookup_data = LookUpDataRetriever(self.es, self.r_server,
            targets=[],
            data_types=(
                LookUpDataType.DISEASE,
                LookUpDataType.TARGET,
                LookUpDataType.ECO,
                LookUpDataType.HPA
            )).lookup

        targets = list(self.es_query.get_all_target_ids_with_evidence_data())

        #setup elasticsearch
        if not dry_run:
            self.es_loader.create_new_index(Const.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME)
            self.es_loader.prepare_for_bulk_indexing(
                self.es_loader.get_versioned_index(Const.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME))

        self.logger.info('setting up stages')

        #bake the arguments for the setup into function objects
        produce_evidence_local_init_baked = functools.partial(produce_evidence_local_init, 
            self.es_hosts, scoring_weights, is_direct_do_not_propagate, datasources_to_datatypes)
        score_producer_local_init_baked = functools.partial(score_producer_local_init, 
            self.es_hosts, self.redis_host, self.redis_port,
            lookup_data, datasources_to_datatypes, dry_run)
        
        #this doesn't need to be in the external config, since its so content light
        #as to be meaningless
        max_queued_score_out = 10000

        #pipeline stage for making the lists of the target/disease pairs and evidence
        pipeline_stage = pr.flat_map(produce_evidence, targets, 
            workers=num_workers_produce,
            maxsize=max_queued_produce_to_score,
            on_start=produce_evidence_local_init_baked, 
            on_done=produce_evidence_local_shutdown)

        #pipeline stage for scoring the evidence sets
        #includes writing to elasticsearch
        pipeline_stage = pr.map(score_producer, pr.to_iterable(pipeline_stage), 
            workers=num_workers_score,
            maxsize=max_queued_score_out,
            on_start=score_producer_local_init_baked, 
            on_done=score_producer_local_shutdown)

        #loop over the end of the pipeline to make sure everything is finished
        for result in pr.to_iterable(pipeline_stage):
            pass

        #cleanup elasticsearch
        if not dry_run:
            self.logger.info('flushing data to index')
            self.es_loader.flush_all_and_wait(Const.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME)
            #restore old pre-load settings
            #note this automatically does all prepared indexes
            self.es_loader.restore_after_bulk_indexing()

        self.logger.info("DONE")

    """
    Run a series of QC tests on EFO elasticsearch index. Returns a dictionary
    of string test names and result objects
    """
    def qc(self, esquery):

        #number of eco entries
        association_count = 0
        #Note: try to avoid doing this more than once!
        for association in esquery.get_all_associations():
            association_count += 1
            if association_count % 1000 == 0:
                self.logger.debug("checking %d", association_count)

        #put the metrics into a single dict
        metrics = dict()
        metrics["association.count"] = association_count

        return metrics