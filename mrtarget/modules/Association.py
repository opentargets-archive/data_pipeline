import logging
import copy

import functional
import functools
import itertools
from collections import defaultdict

from mrtarget.common.connection import new_es_client
from mrtarget.common.esutil import ElasticsearchBulkIndexManager
from mrtarget.common.DataStructure import JSONSerializable
from mrtarget.common.connection import new_es_client, new_redis_client
from mrtarget.common.LookupHelpers import LookUpDataRetriever, LookUpDataType
from mrtarget.common.Scoring import ScoringMethods, HarmonicSumScorer
from mrtarget.modules.EFO import EFO
from mrtarget.common.EvidenceString import Evidence, ExtendedInfoGene, ExtendedInfoEFO
from mrtarget.modules.GeneData import Gene
from mrtarget.modules.HPA import HPAExpression, hpa2tissues
from opentargets_urlzsource import URLZSource

import elasticsearch
from elasticsearch import helpers
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MatchAll
import pypeln.process as pr
import simplejson as json


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

def produce_evidence_local_init(es_hosts, es_index_val_right,
        scoring_weights, is_direct_do_not_propagate, datasources_to_datatypes):
    es = new_es_client(es_hosts)
    return (es, es_index_val_right, scoring_weights, 
        is_direct_do_not_propagate, datasources_to_datatypes)

def get_evidence_for_target_simple(es, target, index):
    query_body = {
        "query": {
            "constant_score": {
                "filter": {
                    "term": {
                        "target.id": str(target)
                    }
                }
            }
        },
        '_source': {
            "includes": 
                ["target.id",
                    "private.efo_codes",
                    "disease.id",
                    "scores.association_score",
                    "sourceID",
                    "id",
                ]},
    }

    for ev in helpers.scan(client=es, query=query_body,
        index=index, size=1000):
        yield ev.to_dict()

def produce_evidence(target, es, es_index_val_right,
        scoring_weights, is_direct_do_not_propagate, datasources_to_datatypes):
    data_cache = {}
    return_values = []
    for evidence in get_evidence_for_target_simple(es, target, es_index_val_right):

        

        if evidence['sourceID'] in is_direct_do_not_propagate:
            efo_list = [evidence['disease']['id']]
        else:
            efo_list = evidence['private']['efo_codes']


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

        return_values.append((key[0],key[1], evidence.to_json(), is_direct))

    return return_values

def score_producer_local_init(redis_host, redis_port, 
        lookup_data, datasources_to_datatypes, dry_run):

    #set the R server to lookup into
    r_server = new_redis_client(redis_host, redis_port)

    scorer = Scorer()

    return scorer, r_server, lookup_data, datasources_to_datatypes, dry_run

def score_producer(data, 
        scorer, r_server, lookup_data, datasources_to_datatypes, dry_run):
    target, disease, evidence, is_direct = data

    if evidence:
        score = scorer.score(target, disease, evidence, is_direct, 
            datasources_to_datatypes)
        # skip associations only with data with score 0
        if score: 

            gene_data = Gene()
            gene_data.load_json(
                lookup_data.available_genes.get_gene(target, r_server))
            score.set_target_data(gene_data)

            # create a hpa expression empty jsonserializable class
            # to fill from Redis cache lookup_data
            hpa_data = HPAExpression()
            try:
                hpa_data.update(
                    lookup_data.available_hpa.get_hpa(target, r_server))
            except KeyError:
                pass
            except Exception as e:
                raise e
            try:
                score.set_hpa_data(hpa_data)
            except KeyError:
                pass
            except Exception as e:
                raise e


            disease_data = EFO()
            disease_data.load_json(
                lookup_data.available_efos.get_efo(disease, r_server))

            score.set_disease_data(disease_data)


            element_id = '%s-%s' % (target, disease)

            #convert the score into a JSON-compatible object
            #otherwise Python serialization consumes too much memory
            return (element_id, score.to_json())

        return None


class ScoringProcess():

    def __init__(self, es_hosts, es_index, es_doc, es_mappings, es_settings,
            es_index_gene, es_index_eco, es_index_val_right, es_index_hpa, es_index_efo,
            redis_host, redis_port, 
            workers_write, workers_production, workers_score, 
            queue_score, queue_produce, queue_write, 
            scoring_weights, is_direct_do_not_propagate,
            datasources_to_datatypes):

        self.logger = logging.getLogger(__name__)

        self.es_hosts = es_hosts
        self.es_index = es_index
        self.es_doc = es_doc
        self.es_mappings = es_mappings
        self.es_settings = es_settings
        self.es_index_gene = es_index_gene
        self.es_index_eco = es_index_eco
        self.es_index_val_right = es_index_val_right
        self.es_index_hpa = es_index_hpa
        self.es_index_efo = es_index_efo

        self.redis_host = redis_host
        self.redis_port = redis_port
        self.r_server = new_redis_client(self.redis_host, self.redis_port)
        self.workers_write = workers_write
        self.workers_production = workers_production
        self.workers_score = workers_score
        self.queue_write = queue_write
        self.queue_produce = queue_produce
        self.queue_score = queue_score

        self.scoring_weights = scoring_weights
        self.is_direct_do_not_propagate = is_direct_do_not_propagate
        self.datasources_to_datatypes = datasources_to_datatypes


    def get_targets(self, es):
        for target in Search().using(es).index(self.es_index_gene).query(MatchAll()).scan():
            yield str(target.meta.id)

    def process_all(self, dry_run, 
            ):

        es = new_es_client(self.es_hosts)

        lookup_data = LookUpDataRetriever(es, self.r_server,
            (
                LookUpDataType.DISEASE,
                LookUpDataType.TARGET,
                LookUpDataType.ECO,
                LookUpDataType.HPA
            ),
            gene_index=self.es_index_gene,
            eco_index=self.es_index_eco,
            hpa_index=self.es_index_hpa,
            efo_index=self.es_index_efo).lookup

        targets = self.get_targets(es)

        self.logger.info('setting up stages')

        #bake the arguments for the setup into function objects
        produce_evidence_local_init_baked = functools.partial(produce_evidence_local_init, 
            self.es_hosts, self.es_index_val_right,
            self.scoring_weights, self.is_direct_do_not_propagate, 
            self.datasources_to_datatypes)
        score_producer_local_init_baked = functools.partial(score_producer_local_init, 
            self.redis_host, self.redis_port, lookup_data, self.datasources_to_datatypes, 
            dry_run)
        
        #pipeline stage for making the lists of the target/disease pairs and evidence
        pipeline_stage1 = pr.flat_map(produce_evidence, targets, 
            workers=self.workers_production,
            maxsize=self.queue_produce,
            on_start=produce_evidence_local_init_baked)

        #pipeline stage for scoring the evidence sets
        #includes writing to elasticsearch
        pipeline_stage2 = pr.map(score_producer, pipeline_stage1, 
            workers=self.workers_score,
            maxsize=self.queue_score,
            on_start=score_producer_local_init_baked)

        with URLZSource(self.es_mappings).open() as mappings_file:
            mappings = json.load(mappings_file)

        with URLZSource(self.es_settings).open() as settings_file:
            settings = json.load(settings_file)

        with ElasticsearchBulkIndexManager(es, self.es_index, settings, mappings):

            #load into elasticsearch
            self.logger.info('stages created, running scoring and writing')
            client = es
            chunk_size = 1000 #TODO make configurable
            actions = self.elasticsearch_actions(pipeline_stage2, 
                self.es_index, self.es_doc)
            failcount = 0

            if not dry_run:
                results = None
                if self.workers_write > 0:
                    results = elasticsearch.helpers.parallel_bulk(client, actions,
                            thread_count=self.workers_write,
                            queue_size=self.queue_write, 
                            chunk_size=chunk_size)
                else:
                    results = elasticsearch.helpers.streaming_bulk(client, actions,
                            chunk_size=chunk_size)
                for success, details in results:
                    if not success:
                        failcount += 1

                if failcount:
                    raise RuntimeError("%s relations failed to index" % failcount)

        self.logger.info("DONE")

    """
    Generates elasticsearch action objects from the results iterator

    Output suitable for use with elasticsearch.helpers 
    """
    def elasticsearch_actions(self, results, index, doc):
        for r in results:
            if r is not None:
                element_id, score = r
                action = {}
                action["_index"] = index
                action["_type"] = doc
                action["_id"] = element_id
                #elasticsearch client uses https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/serializer.py#L24
                #to turn objects into JSON bodies. This in turn calls json.dumps() using simplejson if present.
                action["_source"] = score
                yield action
                    
    """
    Run a series of QC tests on EFO elasticsearch index. Returns a dictionary
    of string test names and result objects
    """
    def qc(self, es, index):

        #number of eco entries
        association_count = 0
        #Note: try to avoid doing this more than once!
        for association in Search().using(es).index(index).query(MatchAll()).scan():
            association_count += 1
            if association_count % 1000 == 0:
                self.logger.debug("checking %d", association_count)

        #put the metrics into a single dict
        metrics = dict()
        metrics["association.count"] = association_count

        return metrics