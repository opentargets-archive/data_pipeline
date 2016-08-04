import ctypes
import pprint
from collections import defaultdict, OrderedDict
import copy
from datetime import datetime
import json
import logging
import os
import pickle
import traceback

import math
from elasticsearch import Elasticsearch
from sqlalchemy import and_
import sys

from tqdm import tqdm

from common import Actions
from common.DataStructure import JSONSerializable, PipelineEncoder
from common.ElasticsearchLoader import JSONObjectStorage, Loader
from common.ElasticsearchQuery import ESQuery
from modules import GeneData
from modules.ECO import ECO, EcoRetriever, ECOLookUpTable
from modules.EFO import EFO, get_ontology_code_from_url, EfoRetriever, EFOLookUpTable
from modules.GeneData import Gene, GeneRetriever, GeneLookUpTable
from settings import Config
from dateutil import parser as smart_date_parser
import time
import multiprocessing

logger = logging.getLogger(__name__)
# logger = multiprocessing.get_logger()



'''line profiler code'''
try:
    from line_profiler import LineProfiler

    def do_profile(follow=[]):
        def inner(func):
            def profiled_func(*args, **kwargs):
                try:
                    profiler = LineProfiler()
                    profiler.add_function(func)
                    for f in follow:
                        profiler.add_function(f)
                    profiler.enable_by_count()
                    return func(*args, **kwargs)
                finally:
                    profiler.print_stats()
            return profiled_func
        return inner

except ImportError:
    def do_profile(follow=[]):
        "Helpful if you accidentally leave in production!"
        def inner(func):
            def nothing(*args, **kwargs):
                return func(*args, **kwargs)
            return nothing
        return inner
'''end of line profiler code'''

__author__ = 'andreap'

class EvidenceStringActions(Actions):
    PROCESS='process'
    UPLOAD='upload'


# def evs_lookup(dic, key, *keys):
#     '''
#     use like evs_lookup(d, *key1.key2.key3.split('.'))
#     :param dic:
#     :param key:
#     :param keys:
#     :return:
#     '''
#     if keys:
#         return evs_lookup(dic.get(key, {}), *keys)
#     return dic.get(key)
#
# def evs_set(dic,value, key, *keys):
#     '''use like evs_set(d, value, *key1.key2.key3.split('.'))
#     '''
#     if keys:
#         return evs_set(dic.get(key, {}), *keys)
#     dic[key]=value




class DataNormaliser(object):
    def __init__(self, min_value, max_value, old_min_value=0., old_max_value = 1., cap=True):
        self.min = float(min_value)
        self.max = float(max_value)
        self.old_min = old_min_value
        self.old_max = old_max_value
        self.cap = cap
    def __call__(self, value):
        return self.renormalize(value,
                                (self.old_min, self.old_max),
                                (self.min, self.max),
                                self.cap)

    @staticmethod
    def renormalize(n, start_range, new_range, cap=True):
        max_new_range = max(new_range)
        min_new_range = min(new_range)
        delta1 = start_range[1] - start_range[0]
        delta2 = new_range[1] - new_range[0]
        if delta1 or delta2:
            try:
                normalized = (delta2 * (n - start_range[0]) / delta1) + new_range[0]
            except ZeroDivisionError:
                normalized = 0.
        else:
            normalized = n
        if cap:
            if normalized > max_new_range:
                return max_new_range
            elif normalized < min_new_range:
                return min_new_range
        return normalized

class ExtendedInfo():
    data = dict()

    def extract_info(self, obj):
        raise NotImplementedError()

    def to_json(self):
        return json.dumps(self.data)

    def load_json(self, data):
        self.data = json.loads(data)

class ExtendedInfoGene(ExtendedInfo):
    root = "gene_info"

    def __init__(self, gene):
        if isinstance(gene, Gene):
            self.extract_info(gene)
        else:
            raise AttributeError("you need to pass a Gene not a: " + str(type(gene)))

    def extract_info(self, gene):
        self.data = dict(geneid = gene.id,
                          symbol=gene.approved_symbol or gene.ensembl_external_name,
                          name=gene.approved_name or gene.ensembl_description)

class ExtendedInfoEFO(ExtendedInfo):
    root = "efo_info"

    def __init__(self, efo):
        if isinstance(efo, EFO):
            self.extract_info(efo)
        else:
            raise AttributeError("you need to pass a EFO not a: " + str(type(efo)))

    def extract_info(self, efo):
        therapeutic_area_codes = set()
        therapeutic_area_labels = set()
        for path in efo.path:
            if len(path) >1:
                therapeutic_area_codes.add(get_ontology_code_from_url(path[0]['uri']))
                therapeutic_area_labels.add(get_ontology_code_from_url(path[0]['label']))
        self.data = dict( efo_id = efo.get_id(),
                          label=efo.label,
                          path=efo.path_codes,
                          therapeutic_area= dict(codes = list(therapeutic_area_codes),
                                                 labels = list(therapeutic_area_labels)))

class ExtendedInfoECO(ExtendedInfo):
    root = "evidence_codes_info"

    def __init__(self, eco):
        if isinstance(eco, ECO):
            self.extract_info(eco)
        else:
            raise AttributeError("you need to pass a ECO not a: " + str(type(eco)))

    def extract_info(self, eco):
        self.data = dict(eco_id = eco.get_id(),
                          label=eco.label),

class ProcessedEvidenceStorer():
    def __init__(self,  es_loader, chunk_size=1e4, quiet = False):


        self.chunk_size = chunk_size
        self.cache = {}
        self.counter = 0
        self.quiet = quiet
        self.es_loader=es_loader

    def put(self, id, ev):

        # self.cache[id] = ev
        self.counter +=1
        self.es_loader.put(Config.ELASTICSEARCH_DATA_INDEX_NAME+'-'+Config.DATASOURCE_TO_INDEX_KEY_MAPPING[ev.database],
                           ev.get_doc_name(),
                           id,
                           ev.to_json(),
                           create_index = True,
                           routing = ev.evidence['target']['id'])






    def close(self):
        pass

    def __enter__(self):
        return self


    def __exit__(self, type, value, traceback):
        self.close()

class EvidenceManagerLookUpData():
    def __init__(self):
        self.available_genes = None
        self.available_efos = None
        self.available_ecos = None
        self.uni2ens = None
        self.non_reference_genes = None
        self.available_gene_objects = None
        self.available_efo_objects = None
        self.available_eco_objects = None



class EvidenceManagerLookUpDataRetrieval():
    def __init__(self,
                 es = None,
                 r_server = None):

        self.es = es
        self.r_server = r_server
        if es is not None:
            self.esquery = ESQuery(es)
        self.lookup = EvidenceManagerLookUpData()
        start_time = time.time()
        load_bar = tqdm(desc='loading lookup data',
             total=3,
             unit=' steps',
             leave=False,)
        self._get_gene_info()
        logger.info("finished self._get_gene_info(), took %ss" % str(time.time() - start_time))
        load_bar.update()
        self._get_available_efos()
        logger.info("finished self._get_available_efos(), took %ss"%str(time.time()-start_time))
        load_bar.update()
        self._get_available_ecos()
        logger.info("finished self._get_available_ecos(), took %ss"%str(time.time()-start_time))
        load_bar.update()


    def _get_available_efos(self):
        logger.info('getting efos')
        self.lookup.available_efos = EFOLookUpTable(self.es,'EFO_LOOKUP', self.r_server)

    def _get_available_ecos(self):
        logger.info('getting ecos')
        self.lookup.available_ecos = ECOLookUpTable(self.es, 'ECO_LOOKUP', self.r_server)


    def _get_gene_info(self):
        logger.info('getting gene info')
        self.lookup.uni2ens = {}
        self.lookup.available_genes = GeneLookUpTable(self.es, 'GENE_LOOKUP', self.r_server)
        for gene_id in tqdm(self.lookup.available_genes.keys(),
                            desc='getting mappings uni2ens'):
            gene = GeneLookUpTable.get_gene(gene_id)
            if gene['uniprot_id']:
                self.lookup.uni2ens[gene['uniprot_id']] = gene.id
            for accession in gene['uniprot_accessions']:
                self.lookup.uni2ens[accession] = gene.id
        self._get_non_reference_gene_mappings()

    def _get_non_reference_gene_mappings(self):
        self.lookup.non_reference_genes = {}
        skip_header=True
        for line in file('resources/genes_with_non_reference_ensembl_ids.tsv'):
            if skip_header:
                skip_header=False
            symbol, ensg, assembly, chr, is_ref = line.split()
            if symbol not in self.lookup.non_reference_genes:
                self.lookup.non_reference_genes[symbol]=dict(reference='',
                                                      alternative=[])
            if is_ref == 't':
                self.lookup.non_reference_genes[symbol]['reference']=ensg
            else:
                self.lookup.non_reference_genes[symbol]['alternative'].append(ensg)


class EvidenceManager():
    def __init__(self, lookup_data,):
        self.available_genes = lookup_data.available_genes
        self.available_efos =  lookup_data.available_efos
        self.available_ecos =  lookup_data.available_ecos
        self.uni2ens =  lookup_data.uni2ens
        self.non_reference_genes =  lookup_data.non_reference_genes
        self._get_eco_scoring_values()
        # logger.debug("finished self._get_eco_scoring_values(), took %ss"%str(time.time()-start_time))
        self.uni_header=GeneData.UNI_ID_ORG_PREFIX
        self.ens_header=GeneData.ENS_ID_ORG_PREFIX
        # self.gene_retriever = GeneLookUpTable(self.es)
        # self.efo_retriever = EFOLookUpTable(self.es)
        # self.eco_retriever = ECOLookUpTable(self.es)
        self._get_score_modifiers()
        # logger.debug("finished self._get_score_modifiers(), took %ss"%str(time.time()-start_time))


    # @do_profile()#follow=[])
    def fix_evidence(self, evidence):

        evidence = evidence.evidence
        fixed = False
        '''fix errors in data here so nobody needs to ask corrections to the data provider'''

        '''fix missing version in gwas catalog data'''
        if 'variant2disease' in evidence:
            try:
                float(evidence['evidence']['variant2disease']['provenance_type']['database']['version'])
            except:
                evidence['evidence']['variant2disease']['provenance_type']['database']['version']=''
                fixed=True
            try:
                float(evidence['evidence']['variant2disease']['provenance_type']['database']['dbxref']['version'])
            except:
                evidence['evidence']['variant2disease']['provenance_type']['database']['dbxref']['version']=''
                fixed=True
        if 'gene2variant' in evidence:
            try:
                float(evidence['evidence']['gene2variant']['provenance_type']['database']['version'])
            except:
                evidence['evidence']['gene2variant']['provenance_type']['database']['version']=''
                fixed=True
            try:
                float(evidence['evidence']['gene2variant']['provenance_type']['database']['dbxref']['version'])
            except:
                evidence['evidence']['gene2variant']['provenance_type']['database']['dbxref']['version']=''
                fixed=True
        if evidence['sourceID'] == 'gwascatalog':
            evidence['sourceID'] = 'gwas_catalog'
            fixed=True
        '''split EVA in two datasources depending on the datatype'''
        if (evidence['sourceID'] == 'eva') and \
                (evidence['type']== 'somatic_mutation'):
            evidence['sourceID'] = 'eva_somatic'
            fixed=True
        '''move genetic_literature to genetic_association'''
        if evidence['type']=='genetic_literature':
            evidence['type']='genetic_association'

        if 'provenance_type' in evidence and \
                'database' in evidence['provenance_type'] and \
                'version' in evidence['provenance_type']['database']:
            evidence['provenance_type']['database']['version'] = str(evidence['provenance_type']['database']['version'])

        '''enforce eco-based score for genetic_association evidencestrings'''
        if evidence['type']=='genetic_association':
            available_score=None
            eco_uri = None
            try:
                available_score = evidence['evidence']['gene2variant']['resource_score']['value']
            except KeyError:
                logger.debug("malformed evidence string %s: KeyError in  evidence['evidence']['gene2variant']['resource_score']['value']"%evidence['id'])
            try:
                eco_uri = evidence['evidence']['gene2variant']['functional_consequence']
            except KeyError:
                logger.debug("malformed evidence string %s: KeyError in  evidence['evidence']['gene2variant']['functional_consequence']"%evidence['id'])


            if eco_uri in self.eco_scores:
                if 'resource_score' not in evidence['evidence']['gene2variant']:
                    evidence['evidence']['gene2variant']['resource_score']={}
                evidence['evidence']['gene2variant']['resource_score']['value'] = self.eco_scores[eco_uri]
                evidence['evidence']['gene2variant']['resource_score']['type'] = 'probability'
                if available_score !=self.eco_scores[eco_uri]:
                    fixed = True
            else:
                if 'uniprot_literature' != evidence['sourceID']:
                    logger.warning("Cannot find a score for eco code %s in evidence id %s"%(eco_uri, evidence['id']))

        '''use just one mutation per somatic data'''
        if 'known_mutations' in evidence and evidence['evidence']['known_mutations']:
            if len(evidence['evidence']['known_mutations'])==1:
                evidence['evidence']['known_mutations'] = evidence['evidence']['known_mutations'][0]
            else:
                raise AttributeError('only one mutation is allowed. %i submitted for evidence id %s'%(len(evidence['evidence']['known_mutations']),
                                                                                                      evidence['id']))


        '''remove identifiers.org from genes and map to ensembl ids'''
        target_id = evidence['target']['id'][0]
        new_target_id = None
        id_not_in_ensembl = False
        try:
            if target_id.startswith(self.uni_header):
                if '-' in target_id:
                    target_id = target_id.split('-')[0]
                uniprotid = target_id.split(self.uni_header)[1].strip()
                ensemblid = self.uni2ens[uniprotid]
                new_target_id=self.get_reference_ensembl_id(ensemblid)
            elif target_id.startswith(self.ens_header):
                ensemblid = target_id.split(self.ens_header)[1].strip()
                new_target_id=self.get_reference_ensembl_id(ensemblid)
            else:
                logger.warning("could not recognize target.id: %s | not added" % target_id)
                id_not_in_ensembl = True
        except KeyError:
            logger.error("cannot find an ensembl ID for: %s" % target_id)
            id_not_in_ensembl = True

        if id_not_in_ensembl:
            logger.warning("cannot find any ensembl ID for evidence for: %s. Offending target.id: %s" % (evidence['id'], target_id))

        # if new_target_id is None:
        #     raise AttributeError('cannot find any valid target id for evidence %s'%evidence['id'])
        evidence['target']['id'] = new_target_id


        '''remove identifiers.org from cttv activity  and target type ids'''
        if 'target_type' in evidence['target']:
            evidence['target']['target_type'] = evidence['target']['target_type'].split('/')[-1]
        if 'activity' in evidence['target']:
            evidence['target']['activity'] = evidence['target']['activity'].split('/')[-1]


        '''remove identifiers.org from efos'''
        disease_id = evidence['disease']['id'][0]
        new_disease_id = get_ontology_code_from_url(disease_id)
        if len(new_disease_id.split('_')) != 2:
            logger.warning("could not recognize disease.id: %s | added anyway" % disease_id)
        evidence['disease']['id'] = new_disease_id
        if not new_disease_id:
            logger.warning("No valid disease.id could be found in evidence: %s. Offending disease.id: %s"%(evidence['id'], disease_id))

        '''remove identifiers.org from ecos'''
        new_eco_ids = []
        if 'evidence_codes' in evidence['evidence']:
            eco_ids = evidence['evidence']['evidence_codes']
        elif 'variant2disease' in evidence['evidence']:
            if 'variant2disease' in evidence['evidence']:
                eco_ids = evidence['evidence']['variant2disease']['evidence_codes']
            if 'gene2variant' in evidence['evidence']:
                eco_ids.extend(evidence['evidence']['gene2variant']['evidence_codes'])
        elif 'target2drug' in evidence['evidence']:
            eco_ids = evidence['evidence']['target2drug']['evidence_codes']
            eco_ids.extend(evidence['evidence']['drug2clinic']['evidence_codes'])
        elif 'biological_model' in evidence['evidence']:
            eco_ids = evidence['evidence']['biological_model']['evidence_codes']
        else:
            eco_ids =[] #something wrong here...
        eco_ids = list(set(eco_ids))
        for idorg_eco_uri in eco_ids:
            code = get_ontology_code_from_url(idorg_eco_uri.strip())
            if code is not None:
                # if len(code.split('_')) != 2:
                   # logger.warning("could not recognize evidence code: %s in id %s | added anyway" %(evidence['id'],idorg_eco_uri))
                new_eco_ids.append(code)
        evidence['evidence']['evidence_codes'] = list(set(new_eco_ids))
        if not new_eco_ids:
            logger.warning("No valid ECO could be found in evidence: %s. original ECO mapping: %s"%(evidence['id'], str(eco_ids)[:100]))

        return Evidence(evidence), fixed

    def is_valid(self, evidence, datasource):
        '''check consistency of the data in the evidence'''

        ev = evidence.evidence
        evidence_id = ev['id']

        if not ev['target']['id']:
            logger.error("%s Evidence %s has no valid gene in target.id" % (datasource, evidence_id))
            return False
        gene_id = ev['target']['id']
        if gene_id not in self.available_genes:
            logger.error(
                "%s Evidence %s has an invalid gene id in target.id: %s" % (datasource, evidence_id, gene_id))
            return False
        if not ev['disease']['id']:
            logger.error("%s Evidence %s has no valid efo id in disease.id" % (datasource, evidence_id))
            return False
        efo_id = ev['disease']['id']
        if efo_id not in self.available_efos:
            logger.error(
                "%s Evidence %s has an invalid efo id in disease.id: %s" % (datasource, evidence_id, efo_id))
            return False
        for eco_id in ev['evidence']['evidence_codes']:
            if eco_id not in self.available_ecos:
                logger.error(
                    "%s Evidence %s has an invalid eco id in evidence.evidence_codes: %s" % (datasource, evidence_id, eco_id))
                return False

        return True

    def get_extended_evidence(self, evidence):

        extended_evidence = copy.copy(evidence.evidence)
        extended_evidence['private'] = dict()


        """get generic gene info"""
        genes_info = []
        pathway_data = dict(pathway_type_code=[],
                            pathway_code=[])
        GO_terms = dict(biological_process = [],
                        cellular_component=[],
                        molecular_function=[],
                        )
        uniprot_keywords = []
        #TODO: handle domains
        geneid =  extended_evidence['target']['id']
        # try:
        gene = self._get_gene_obj(geneid)
        genes_info=ExtendedInfoGene(gene)
        if 'reactome' in gene._private['facets']:
            pathway_data['pathway_type_code'].extend(gene._private['facets']['reactome']['pathway_type_code'])
            pathway_data['pathway_code'].extend(gene._private['facets']['reactome']['pathway_code'])
            # except Exception:
            #     logger.warning("Cannot get generic info for gene: %s" % aboutid)
        if gene.go:
            for go in gene.go:
                go_code, data = go['id'], go['value']
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
            extended_evidence["target"][ExtendedInfoGene.root] = genes_info.data

        if pathway_data['pathway_code']:
            pathway_data['pathway_type_code']=list(set(pathway_data['pathway_type_code']))
            pathway_data['pathway_code']=list(set(pathway_data['pathway_code']))


        """get generic efo info"""
        all_efo_codes=[]
        efo_info = []
        diseaseid = extended_evidence['disease']['id']
        # try:
        efo = self._get_efo_obj(diseaseid)
        efo_info=ExtendedInfoEFO(efo)
        # except Exception:
        #     logger.warning("Cannot get generic info for efo: %s" % aboutid)
        if efo_info:
            for path in efo_info.data['path']:
                    all_efo_codes.extend(path)
            extended_evidence["disease"][ExtendedInfoEFO.root] = efo_info.data
        all_efo_codes = list(set(all_efo_codes))

        """get generic eco info"""
        all_eco_codes =  extended_evidence['evidence']['evidence_codes']
        try:
            all_eco_codes.append(get_ontology_code_from_url(extended_evidence['evidence']['gene2variant']['functional_consequence']))
        except KeyError:
            pass
        ecos_info = []
        for eco_id in all_eco_codes:
            eco = self._get_eco_obj(eco_id)
            if eco is not None:
                ecos_info.append(ExtendedInfoECO(eco))
            else:
                logger.warning("Cannot get generic info for eco: %s" % eco_id)

        if ecos_info:
            data = []
            for eco_info in ecos_info:
                data.append(eco_info.data)
            extended_evidence['evidence'][ExtendedInfoECO.root] = data

        '''Add private objects used just for indexing'''

        extended_evidence['private']['efo_codes'] = all_efo_codes
        extended_evidence['private']['eco_codes'] = all_eco_codes
        extended_evidence['private']['datasource']= evidence.datasource
        extended_evidence['private']['datatype']= evidence.datatype
        extended_evidence['private']['facets']={}
        if pathway_data['pathway_code']:
            extended_evidence['private']['facets']['reactome']= pathway_data
        if uniprot_keywords:
            extended_evidence['private']['facets']['uniprot_keywords'] = uniprot_keywords
        if GO_terms['biological_process'] or \
            GO_terms['molecular_function'] or \
            GO_terms['cellular_component'] :
            extended_evidence['private']['facets']['go'] = GO_terms


        return Evidence(extended_evidence)




    def _get_gene_obj(self, geneid):
        gene = Gene(geneid)
        gene.load_json(self.available_genes[geneid])
        return gene

    def _get_efo_obj(self, efoid):
        efo = EFO(efoid)
        efo.load_json(self.available_efos[efoid])
        return efo

    def _get_eco_obj(self, ecoid):
        eco = ECO(ecoid)
        eco.load_json(self.available_ecos[ecoid])
        return eco


    def _get_non_reference_gene_mappings(self):
        self.non_reference_genes = {}
        skip_header=True
        for line in file('resources/genes_with_non_reference_ensembl_ids.tsv'):
            if skip_header:
                skip_header=False
            symbol, ensg, assembly, chr, is_ref = line.split()
            if symbol not in self.non_reference_genes:
                self.non_reference_genes[symbol]=dict(reference='',
                                                      alternative=[])
            if is_ref == 't':
                self.non_reference_genes[symbol]['reference']=ensg
            else:
                self.non_reference_genes[symbol]['alternative'].append(ensg)

    def _map_to_reference_ensembl_gene(self, ensg):
        for symbol, data in self.non_reference_genes.items():
            if ensg in data['alternative']:
                logger.warning("Mapped non reference ensembl gene id %s to %s for gene %s"%(ensg, data['reference'], symbol ))
                return data['reference']

    def get_reference_ensembl_id(self, ensemblid):
        if ensemblid not in self.available_genes:
            ensemblid = self._map_to_reference_ensembl_gene(ensemblid) or ensemblid
        return ensemblid

    def _get_eco_scoring_values(self):
        self.eco_scores=dict()
        for line in file('resources/eco_scores.tsv'):
            try:
                uri, label, score = line.strip().split('\t')
                self.eco_scores[uri]=float(score)
            except:
                logger.error("cannot parse line in eco_scores.tsv: %s"%(line.strip()))

    def _get_score_modifiers(self):
        self.score_modifiers = {}
        for datasource, values in Config.DATASOURCE_EVIDENCE_SCORE_AUTO_EXTEND_RANGE.items():
            self.score_modifiers[datasource]=DataNormaliser(values['min'],values['max'])




class Evidence(JSONSerializable):
    def __init__(self, evidence, datasource = ""):
        if isinstance(evidence, str) or isinstance(evidence, unicode):
            self.load_json(evidence)
        elif isinstance(evidence, dict):
            self.evidence = evidence
        else:
            raise AttributeError(
                "the evidence should be a dict or a json string to parse, not a " + str(type(evidence)))
        self.datasource = self.evidence['sourceID'] or datasource
        self._set_datatype()


    def _set_datatype(self,):

        # if 'type' in self.evidence:
        #     self.datatype = self.evidence['type']
        # else:
        translate_database = Config.DATASOURCE_TO_DATATYPE_MAPPING
        try:
            self.database = self.evidence['sourceID'].lower()
        except KeyError:
            self.database = self.datasource.lower()
        self.datatype = translate_database[self.database]

    def get_doc_name(self):
        return Config.ELASTICSEARCH_DATA_DOC_NAME+'-'+self.database

    def get_id(self):
        return self.evidence['id']

    def to_json(self):
        self.stamp_data_release()
        return json.dumps(self.evidence,
                          sort_keys=True,
                          # indent=4,
                          cls=PipelineEncoder)

    def stamp_data_release(self):
        self.evidence['data_release'] = Config.RELEASE_VERSION

        return

    def score_to_json(self):
        score = {}
        score['id']=self.evidence['id']
        score['sourceID']=self.evidence['sourceID']
        score['type']=self.evidence['type']
        score['target']={"id":self.evidence['target']['id'],
                         "gene_info":self.evidence['target']['gene_info']}

        score['disease']={"id":self.evidence['disease']['id'],
                         "efo_info":self.evidence['disease']['efo_info']}
        score['scores']= self.evidence['scores']
        score['private']= {"efo_codes":self.evidence['private']['efo_codes']}
        return json.dumps(score)

    def load_json(self, data):
        self.evidence = json.loads(data)

    def score_evidence(self, modifiers = {}):
        self.evidence['scores'] = dict(association_score=0.,
                                    )
        try:
            if self.evidence['type']=='known_drug':
                self.evidence['scores'] ['association_score']= \
                    float(self.evidence['evidence']['drug2clinic']['resource_score']['value']) * \
                    float(self.evidence['evidence']['target2drug']['resource_score']['value'])
            elif self.evidence['type']=='rna_expression':
                pvalue = self._get_score_from_pvalue_linear(self.evidence['evidence']['resource_score']['value'])
                log2_fold_change = self.evidence['evidence']['log2_fold_change']['value']
                fold_scale_factor = abs(log2_fold_change)/10.
                rank = self.evidence['evidence']['log2_fold_change']['percentile_rank']/100.
                score = pvalue*fold_scale_factor*rank
                if score > 1:
                    score = 1.
                self.evidence['scores']['association_score']=score

            elif self.evidence['type']=='genetic_association':
                if 'gene2variant' in  self.evidence['evidence']:
                    g2v_score = self.evidence['evidence']['gene2variant']['resource_score']['value']
                    if self.evidence['evidence']['variant2disease']['resource_score']['type'] =='pvalue':
                        v2d_score = self._get_score_from_pvalue_linear(self.evidence['evidence']['variant2disease']['resource_score']['value'])
                    elif self.evidence['evidence']['variant2disease']['resource_score']['type'] =='probability':
                        v2d_score = self.evidence['evidence']['variant2disease']['resource_score']['value']
                    else:
                        v2d_score = 0.
                    if self.evidence['sourceID']=='gwas_catalog':
                        sample_size =  self.evidence['evidence']['variant2disease']['gwas_sample_size']
                        score =self._score_gwascatalog(v2d_score, sample_size,g2v_score)
                    else:
                        score = g2v_score*v2d_score
                else:
                    if self.evidence['evidence']['resource_score']['type']=='probability':
                        score=self.evidence['evidence']['resource_score']['value']
                    elif self.evidence['evidence']['resource_score']['type']=='pvalue':
                        score=self._get_score_from_pvalue_linear(self.evidence['evidence']['resource_score']['value'])
                self.evidence['scores'] ['association_score']= score
            elif self.evidence['type']=='animal_model':
                self.evidence['scores'] ['association_score']= float(self.evidence['evidence']['disease_model_association']['resource_score']['value'])
            elif self.evidence['type']=='somatic_mutation':
                frequency = 1.
                if 'known_mutations' in self.evidence and self.evidence['evidence']['known_mutations']:
                    frequency = float(self.evidence['known_mutations']['number_mutated_samples'])/float(self.evidence['known_mutations']['known_mutations'])
                    frequency=DataNormaliser.renormalize(frequency,[0.,9.],[.5, 1.])
                self.evidence['scores'] ['association_score']= float(self.evidence['evidence']['resource_score']['value'])*frequency
            elif self.evidence['type']=='literature':
                score=  float(self.evidence['evidence']['resource_score']['value'])
                if self.evidence['sourceID']=='europepmc':
                    score = score/100.
                    if score >1:
                        score=1.
                self.evidence['scores'] ['association_score']= score
            elif self.evidence['type']=='affected_pathway':
                self.evidence['scores'] ['association_score']= float(self.evidence['evidence']['resource_score']['value'])
            # if self.evidence['sourceID']=='expression_atlas':
            #     pass
            # elif self.evidence['sourceID']=='uniprot':
            #     pass
            # elif self.evidence['sourceID']=='reactome':
            #     pass
            # elif self.evidence['sourceID']=='eva':
            #     pass
            # elif self.evidence['sourceID']=='phenodigm':
            #     pass
            # elif self.evidence['sourceID']=='gwas_catalog':
            #     pass
            # elif self.evidence['sourceID']=='cancer_gene_census':
            #     pass
            # elif self.evidence['sourceID']=='chembl':
            #     pass
            # elif self.evidence['sourceID']=='europmc':
            #     pass
            # elif self.evidence['sourceID']=='disgenet':
            #     pass
        except:
            logger.warn("Cannot score evidence %s of type %s"%(self.evidence['id'],self.evidence['type']))

        '''check for minimum score '''
        if self.evidence['scores'] ['association_score'] < Config.SCORING_MIN_VALUE_FILTER[self.evidence['sourceID']]:
            raise AttributeError("Evidence String Rejected since score is too low: %s. score: %f, min score: %f" % (self.get_id(),
                                                                                                                    self.evidence['scores'] ['association_score'],
                                                                                                                    Config.SCORING_MIN_VALUE_FILTER[self.evidence['sourceID']]
                                                                                                                    ))

        '''modify scores accodigng to weights'''
        datasource_weight = Config.DATASOURCE_EVIDENCE_SCORE_WEIGHT.get(self.evidence['sourceID'], 1.)
        if datasource_weight !=1:
            weighted_score =  self.evidence['scores'] ['association_score']*datasource_weight
            if weighted_score >1:
               weighted_score = 1.
            self.evidence['scores'] ['association_score'] =  weighted_score

        '''apply rescaling to scores'''
        if self.evidence['sourceID'] in modifiers:
            self.evidence['scores'] ['association_score'] =  modifiers[self.evidence['sourceID']]( self.evidence['scores'] ['association_score'])



    @staticmethod
    def _get_score_from_pvalue_linear( pvalue, range_min=1, range_max=1e-10):
        def get_log(n):
            try:
               return math.log10(n)
            except ValueError:
                return 300
        min_score = get_log(range_min)
        max_score = get_log(range_max)
        score = get_log(pvalue)
        return DataNormaliser.renormalize(score, [min_score, max_score], [0., 1.] )

    def _score_gwascatalog(self,pvalue,sample_size, severity):

        normalised_pvalue = self._get_score_from_pvalue_linear(pvalue, range_min=1, range_max=1e-15)


        normalised_sample_size = DataNormaliser.renormalize(sample_size, [0,5000], [0,1])

        return normalised_pvalue*normalised_sample_size*severity


class UploadError():
    def __init__(self, evidence, trace, id, logdir='errorlogs'):
        self.trace = trace
        if isinstance(evidence, Evidence):
            self.evidence = evidence.evidence
        elif isinstance(evidence, str):
            self.evidence = evidence
        else:
            self.evidence = repr(evidence)
        self.id = id
        try:
            self.database = evidence['sourceID']
        except:
            self.database = 'unknown'
        self.logdir = logdir

    def save(self):
        pass
        # dir = os.path.join(self.logdir, self.database)
        # if not os.path.exists(self.logdir):
        #     os.mkdir(self.logdir)
        # if not os.path.exists(dir):
        #     os.mkdir(dir)
        # filename = str(os.path.join(dir, self.id))
        # pickle.dump(self, open(filename + '.pkl', 'w'))
        # json.dump(self.evidence, open(filename + '.json', 'w'))




class EvidenceProcesser(multiprocessing.Process):

    def __init__(self,
                 input_q,
                 output_q,
                 lookup_data,
                 input_loading_finished,
                 output_computation_finished,
                 input_generated_count,
                 output_computed_count,
                 processing_errors_count,
                 input_processed_count,
                 lock):
        super(EvidenceProcesser, self).__init__()
        self.input_q = input_q
        self.output_q = output_q
        self.start_time = time.time()
        self.evidence_manager = EvidenceManager(lookup_data)
        self.input_loading_finished = input_loading_finished
        self.output_computation_finished = output_computation_finished
        self.input_generated_count = input_generated_count
        self.output_computed_count = output_computed_count
        self.processing_errors_count = processing_errors_count
        self.input_processed_count = input_processed_count
        self.start_time = time.time()#reset timer start
        self.lock = lock



    def run(self):
        logger.info("%s started"%self.name)
        self.data_processing_started = False
        while not ((self.input_generated_count.value == self.input_processed_count.value) and self.input_loading_finished.is_set()):
            data = self.input_q.get()
            with self.lock:
                self.input_processed_count.value +=1
            if data:
                idev, ev = data
                try:
                    fixed_ev, fixed = self.evidence_manager.fix_evidence(ev)
                    # logger.critical("%i processed"%self.output_computed_count.value)
                    if self.evidence_manager.is_valid(fixed_ev, datasource=fixed_ev.datasource):
                        '''add scoring to evidence string'''
                        fixed_ev.score_evidence(self.evidence_manager.score_modifiers)
                        '''extend data in evidencestring'''
                        ev_string_to_load = self.evidence_manager.get_extended_evidence(ev)
                        # logger.info('%s processed'%idev)
                    else:
                        # traceback.print_exc(limit=1, file=sys.stdout)
                        raise AttributeError("Invalid %s Evidence String" % (fixed_ev.datasource))
                    # if fixed:
                    #     fix+=1
                    self.output_q.put((idev, ev_string_to_load))
                    with self.lock:
                        self.output_computed_count.value +=1

                except Exception, error:
                    with self.lock:
                        self.processing_errors_count.value +=1
                    # UploadError(ev, error, idev).save()
                    # err += 1

                    if isinstance(error,AttributeError):
                        logger.error("Error loading data for id %s: %s" % (idev, str(error)))
                        # logger.error("%i %i"%(self.output_computed_count.value,self.processing_errors_count.value))
                    else:
                        logger.exception("Error loading data for id %s: %s" % (idev, str(error)))
                    # traceback.print_exc(limit=1, file=sys.stdout)


                if self.input_processed_count.value %5e4 ==0:
                    logger.info("%i processed | %i errors | processing %1.2f evidence per second"%(self.output_computed_count.value,
                                                                                                           self.processing_errors_count.value,
                                                                                                       float(self.input_processed_count.value)/(time.time()-self.start_time)))
            else:
                time.sleep(0.01)
        self.output_computation_finished.set()
        logger.info("%s finished"%self.name)






class EvidenceStorerWorker(multiprocessing.Process):
    def __init__(self,
                 processing_output_q,
                 processing_finished,
                 signal_finish,
                 submitted_to_storage,
                 output_generated_count,
                 lock,
                 chunk_size = 1e4,
                 ):
        super(EvidenceStorerWorker, self).__init__()
        self.q= processing_output_q
        self.signal_finish = signal_finish
        self.chunk_size=chunk_size
        self.processing_finished = processing_finished
        self.output_generated_count = output_generated_count
        self.total_loaded = submitted_to_storage
        self.es = Elasticsearch(Config.ELASTICSEARCH_URL)
        self.lock = lock

    def run(self):
        logger.info("worker %s started"%self.name)
        with Loader(self.es, chunk_size=self.chunk_size) as es_loader:
            with ProcessedEvidenceStorer( es_loader, chunk_size=self.chunk_size, quiet=False) as storer:
                while not (((self.output_generated_count.value == self.total_loaded.value) and \
                        self.processing_finished.is_set()) or self.signal_finish.is_set()):
                    if not self.q.empty():
                        output = self.q.get()
                        idev, ev = output
                        storer.put(idev,
                           ev)
                        with self.lock:
                            self.total_loaded.value+=1
                        if self.total_loaded.value % (self.chunk_size*5) ==0:
                            logger.info("pushed %i entries to es"%self.total_loaded.value)
                    else:
                        time.sleep(0.01)
                    # print self.name, (((self.output_generated_count.value == self.total_loaded.value) and \
                    #         self.processing_finished.is_set()) or self.signal_finish.is_set()), self.output_generated_count.value == self.total_loaded.value,self.processing_finished.is_set(),  self.signal_finish.is_set(), self.total_loaded.value


        self.signal_finish.set()
        logger.info("%s finished"%self.name)


class EvidenceStringProcess():

    def __init__(self,
                 es,
                 r_server):
        self.loaded_entries_to_pg = 0
        self.es = es
        self.es_query = ESQuery(es)
        self.r_server = r_server

    def process_all(self):
        self._process_evidence_string_data()



    def _process_evidence_string_data(self):


        base_id = 0
        err = 0
        fix = 0


        logger.debug("Starting Evidence Manager")


        evidence_start_time = time.time()
        lookup_data = EvidenceManagerLookUpDataRetrieval(self.es, self.r_server).lookup
        get_evidence_page_size = 5000

        '''create queues'''
        input_q = multiprocessing.Queue(maxsize=get_evidence_page_size+1)
        output_q = multiprocessing.Queue(maxsize=get_evidence_page_size)
        '''create events'''
        input_loading_finished = multiprocessing.Event()
        output_computation_finished = multiprocessing.Event()
        data_storage_finished = multiprocessing.Event()
        '''create shared memory objects'''

        input_generated_count =  multiprocessing.Value('i', 0)
        processing_errors_count =  multiprocessing.Value('i', 0)
        output_computed_count =  multiprocessing.Value('i', 0)
        input_processed_count = multiprocessing.Value('i', 0)
        submitted_to_storage_count =  multiprocessing.Value('i', 0)

        '''create locks'''
        data_processing_lock =  multiprocessing.Lock()
        data_storage_lock =  multiprocessing.Lock()

        workers_number = Config.WORKERS_NUMBER or multiprocessing.cpu_count()

        '''create workers'''
        scorers = [EvidenceProcesser(input_q,
                                     output_q,
                                     lookup_data,
                                     input_loading_finished,
                                     output_computation_finished,
                                     input_generated_count,
                                     output_computed_count,
                                     processing_errors_count,
                                     input_processed_count,
                                     data_processing_lock
                                     ) for i in range(workers_number)]
                                  # ) for i in range(2)]
        for w in scorers:
            w.start()

        storers = [EvidenceStorerWorker(output_q,
                                        output_computation_finished,
                                        data_storage_finished,
                                        submitted_to_storage_count,
                                        output_computed_count,
                                        data_storage_lock
                                     )  for i in range(workers_number)]
                                  # ) for i in range(1)]
        for w in storers:
            w.start()


        for row in tqdm(self.get_evidence(page_size = get_evidence_page_size),
                        desc='Reading available evidence_strings',
                        total = self.es_query.count_validated_evidence_strings(),
                        unit=' evidence',
                        unit_scale=True):
            ev = Evidence(row['evidence_string'], datasource= row['data_source_name'])
            idev = row['uniq_assoc_fields_hashdig']
            ev.evidence['id'] = idev
            input_q.put((idev, ev))
            input_generated_count.value += 1

            if input_generated_count.value % 1e4 == 0:
                logger.info("%i entries submitted for process" % (input_generated_count.value))
        input_loading_finished.set()

        '''wait for other processes to finish'''
        while not data_storage_finished.is_set():
                time.sleep(.1)
        for w in scorers:
            if w.is_alive():
                w.terminate()
        for w in storers:
            if w.is_alive():
                time.sleep(.1)
                w.terminate()
        logger.info("%i entries processed with %i errors and %i fixes" % (base_id, err, fix))


        logging.info('flushing data to index')
        self.es.indices.flush('%s*'%Loader.get_versioned_index(Config.ELASTICSEARCH_DATA_INDEX_NAME),
                              wait_if_ongoing=True)
        return





    def get_evidence(self, page_size = 5000):

        c = 0
        for row in self.es_query.get_validated_evidence_strings(size=page_size):
            c += 1
            if c % page_size == 0:
                logger.info("loaded %i ev from db to process" % c)
            yield row
        logger.info("loaded %i ev from db to process"%c)




