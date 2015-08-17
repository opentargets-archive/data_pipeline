from collections import defaultdict, OrderedDict
import copy
from datetime import datetime
import json
import logging
import os
import pickle
import traceback
from sqlalchemy import and_
import sys
from common import Actions
from common.DataStructure import JSONSerializable
from common.ElasticsearchLoader import JSONObjectStorage
from common.PGAdapter import LatestEvidenceString, ElasticsearchLoad, EvidenceString121
from modules import GeneData
from modules.ECO import ECO, EcoRetriever
from modules.EFO import EFO, get_ontology_code_from_url, EfoRetriever
from modules.GeneData import Gene, GeneRetriever
from settings import Config
from dateutil import parser as smart_date_parser


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
            raise AttributeError("you need to pass a Gene not a: " + type(gene))

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
            raise AttributeError("you need to pass a EFO not a: " + type(efo))

    def extract_info(self, efo):
        self.data = dict( efo_id = efo.get_id(),
                          label=efo.label,
                          path=efo.path_codes),

class ExtendedInfoECO(ExtendedInfo):
    root = "evidence_codes_info"

    def __init__(self, eco):
        if isinstance(eco, ECO):
            self.extract_info(eco)
        else:
            raise AttributeError("you need to pass a EFO not a: " + type(eco))

    def extract_info(self, eco):
        self.data = dict(eco_id = eco.get_id(),
                          label=eco.label),

class EvidenceManager():
    def __init__(self, adapter):
        self.adapter = adapter
        self.session = adapter.session
        self._get_available_genes()
        self._get_uni2ens()
        self._get_available_efos()
        self._get_available_ecos()
        self._get_eco_scoring_values()
        self.uni_header=GeneData.UNI_ID_ORG_PREFIX
        self.ens_header=GeneData.ENS_ID_ORG_PREFIX
        self.gene_retriever = GeneRetriever(adapter)
        self.efo_retriever = EfoRetriever(adapter)
        self.eco_retriever = EcoRetriever(adapter)
        self._get_score_modifiers()



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
        '''enforce eco-based score for genetic_association evidencestrings'''
        if evidence['type']=='genetic_association':
            available_score=None
            try:
                available_score = evidence['evidence']['gene2variant']['resource_score']['value']
            except KeyError:
                pass
            eco_uri = evidence['evidence']['gene2variant']['functional_consequence']

            if eco_uri in self.eco_scores:
                evidence['evidence']['gene2variant']['resource_score']['value'] = self.eco_scores[eco_uri]
                evidence['evidence']['gene2variant']['resource_score']['type'] = 'probability'
                if available_score !=self.eco_scores[eco_uri]:
                    fixed = True
            else:
                logging.warning("Cannot find a score for eco code %s in evidence id %s"%(eco_uri, evidence['id']))




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
                logging.warning("could not recognize target.id: %s | not added" % target_id)
                id_not_in_ensembl = True
        except KeyError:
            logging.error("cannot find an ensembl ID for: %s" % target_id)
            id_not_in_ensembl = True

        if id_not_in_ensembl:
            logging.warning("cannot find any ensembl ID for evidence for: %s. Offending target.id: %s" % (evidence['id'], target_id))

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
            logging.warning("could not recognize disease.id: %s | added anyway" % disease_id)
        evidence['disease']['id'] = new_disease_id
        if not new_disease_id:
            logging.warning("No valid disease.id could be found in evidence: %s. Offending disease.id: %s"%(evidence['id'], disease_id))

        '''remove identifiers.org from ecos'''
        new_eco_ids = []
        if 'evidence_codes' in evidence['evidence']:
            eco_ids = evidence['evidence']['evidence_codes']
        elif 'variant2disease' in evidence['evidence']:
            eco_ids = evidence['evidence']['variant2disease']['evidence_codes']
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
                   # logging.warning("could not recognize evidence code: %s in id %s | added anyway" %(evidence['id'],idorg_eco_uri))
                new_eco_ids.append(code)
        evidence['evidence']['evidence_codes'] = list(set(new_eco_ids))
        if not new_eco_ids:
            logging.warning("No valid ECO could be found in evidence: %s. original ECO mapping: %s"%(evidence['id'], str(eco_ids)[:100]))

        return Evidence(evidence), fixed

    def is_valid(self, evidence, datasource):
        '''check consistency of the data in the evidence'''

        ev = evidence.evidence
        evidence_id = ev['id']

        if not ev['target']['id']:
            logging.error("%s Evidence %s has no valid gene in target.id" % (datasource, evidence_id))
            return False
        gene_id = ev['target']['id']
        if gene_id not in self.available_genes:
            logging.error(
                "%s Evidence %s has an invalid gene id in target.id: %s" % (datasource, evidence_id, gene_id))
            return False
        if not ev['disease']['id']:
            logging.error("%s Evidence %s has no valid efo id in disease.id" % (datasource, evidence_id))
            return False
        efo_id = ev['disease']['id']
        if efo_id not in self.available_efos:
            logging.error(
                "%s Evidence %s has an invalid efo id in disease.id: %s" % (datasource, evidence_id, efo_id))
            return False
        for eco_id in ev['evidence']['evidence_codes']:
            if eco_id not in self.available_ecos:
                logging.error(
                    "%s Evidence %s has an invalid eco id in evidence.evidence_codes: %s" % (datasource, evidence_id, eco_id))
                return False
        return True

    def get_extended_evidence(self, evidence):

        extended_evidence = copy.copy(evidence.evidence)
        extended_evidence['_private'] = dict()


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
        gene = self._get_gene(geneid)
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
            extended_evidence["target"][ExtendedInfoGene.root] = genes_info.data

        if pathway_data['pathway_code']:
            pathway_data['pathway_type_code']=list(set(pathway_data['pathway_type_code']))
            pathway_data['pathway_code']=list(set(pathway_data['pathway_code']))


        """get generic efo info"""
        all_efo_codes=[]
        efo_info = []
        diseaseid = extended_evidence['disease']['id']
        # try:
        efo = self._get_efo(diseaseid)
        efo_info=ExtendedInfoEFO(efo)
        # except Exception:
        #     logging.warning("Cannot get generic info for efo: %s" % aboutid)
        if efo_info:
            for e in efo_info.data:
                for node in e['path']:
                    all_efo_codes.extend(node)
            extended_evidence["disease"][ExtendedInfoEFO.root] = efo_info.data
        all_efo_codes = list(set(all_efo_codes))

        """get generic eco info"""
        all_eco_codes =  extended_evidence['evidence']['evidence_codes']
        try:
            all_eco_codes.append(extended_evidence['evidence']['gene2variant']['functional_consequence'])
        except KeyError:
            pass
        ecos_info = []
        for eco_id in all_eco_codes:
            # try:
            eco = self._get_eco(eco_id)
            ecos_info.append(ExtendedInfoECO(eco))
            # except Exception:
            #     logging.warning("Cannot get generic info for eco: %s" % eco_id)

        if ecos_info:
            data = []
            for eco_info in ecos_info:
                data.append(eco_info.data)
            extended_evidence['evidence'][ExtendedInfoECO.root] = data

        '''Add private objects used just for indexing'''

        extended_evidence['_private']['efo_codes'] = all_efo_codes
        extended_evidence['_private']['eco_codes'] = all_eco_codes
        extended_evidence['_private']['datasource']= evidence.datasource
        extended_evidence['_private']['datatype']= evidence.datatype
        extended_evidence['_private']['facets']={}
        if pathway_data['pathway_code']:
            extended_evidence['_private']['facets']['reactome']= pathway_data
        if uniprot_keywords:
            extended_evidence['_private']['facets']['uniprot_keywords'] = uniprot_keywords
        if GO_terms['biological_process'] or \
            GO_terms['molecular_function'] or \
            GO_terms['cellular_component'] :
            extended_evidence['_private']['facets']['go'] = GO_terms


        return Evidence(extended_evidence)



    def _get_available_genes(self):
        self.available_genes = []
        for row in self.session.query(ElasticsearchLoad.id).filter(and_(
                        ElasticsearchLoad.index==Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                        ElasticsearchLoad.type==Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                        ElasticsearchLoad.active==True)
                    ).yield_per(1000):
            self.available_genes.append(row.id)
        self._get_non_reference_gene_mappings()

    def _get_available_efos(self):
        self.available_efos = []
        for row in self.session.query(ElasticsearchLoad.id).filter(and_(
                        ElasticsearchLoad.index==Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME,
                        ElasticsearchLoad.type==Config.ELASTICSEARCH_EFO_LABEL_DOC_NAME,
                        ElasticsearchLoad.active==True)
                    ).yield_per(1000):
            self.available_efos.append(row.id)

    def _get_available_ecos(self):
        self.available_ecos = []
        for row in self.session.query(ElasticsearchLoad.id).filter(and_(
                        ElasticsearchLoad.index==Config.ELASTICSEARCH_ECO_INDEX_NAME,
                        ElasticsearchLoad.type==Config.ELASTICSEARCH_ECO_DOC_NAME,
                        ElasticsearchLoad.active==True)
                    ).yield_per(1000):
            self.available_ecos.append(row.id)

    def _get_gene(self, geneid):
        return self.gene_retriever.get_gene(geneid)

    def _get_efo(self, efoid):
        return self.efo_retriever.get_efo(efoid)

    def _get_eco(self, ecoid):
        return self.eco_retriever.get_eco(ecoid)

    def _get_uni2ens(self):
        self.uni2ens = {}
        for row in self.session.query(ElasticsearchLoad.id, ElasticsearchLoad.data).filter(and_(
                        ElasticsearchLoad.index==Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                        ElasticsearchLoad.type==Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                        ElasticsearchLoad.active==True)
                    ).yield_per(1000):
            data = json.loads(row.data)
            if data['uniprot_id']:
                self.uni2ens[data['uniprot_id']] = row.id
            for accession in data['uniprot_accessions']:
                self.uni2ens[accession]=row.id

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
                logging.warning("Mapped non reference ensembl gene id %s to %s for gene %s"%(ensg, data['reference'], symbol ))
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
                logging.error("cannot parse line in eco_scores.tsv: %s"%(line.strip()))

    def _get_score_modifiers(self):
        self.score_modifiers = {}
        for datasource, values in Config.DATASOURCE_ASSOCIATION_SCORE_AUTO_EXTEND_RANGE.items():
            self.score_modifiers[datasource]=DataNormaliser(values['min'],values['max'])


class Evidence(JSONSerializable):
    def __init__(self, evidence, datasource = ""):
        if isinstance(evidence, str):
            self.load_json(evidence)
        elif isinstance(evidence, dict):
            self.evidence = evidence
        else:
            raise AttributeError(
                "the evidence should be a dict or a json string to parse, not a " + str(type(evidence)))
        self.datasource = evidence['sourceID'] or datasource
        self._set_datatype()


    def _set_datatype(self,):

        translate_database = Config.DATASOURCE_TO_DATATYPE_MAPPING
        try:
            self.database = self.evidence['sourceID'].lower()
        except KeyError:
            self.database = self.datasource.lower()
        self.datatype = translate_database[self.database]

    def get_doc_name(self):
        return Config.ELASTICSEARCH_DATA_DOC_NAME+'-'+self.database,


    def to_json(self):
        return json.dumps(self.evidence)

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
                pvalue = self.evidence['evidence']['resource_score']['value']
                self.evidence['scores'] ['association_score']= self._get_score_from_pvalue(pvalue)

            elif self.evidence['type']=='genetic_association':
                probability_g2v = self.evidence['evidence']['gene2variant']['resource_score']['value']
                pvalue_v2d = self.evidence['evidence']['variant2disease']['resource_score']['value']
                if self.evidence['sourceID']=='gwas_catalog':
                    sample_size =  self.evidence['evidence']['variant2disease']['gwas_sample_size']
                    score =self._score_gwascatalog(pvalue_v2d, sample_size,probability_g2v)
                else:
                    score = probability_g2v*pvalue_v2d
                self.evidence['scores'] ['association_score']= score
            elif self.evidence['type']=='animal_model':
                self.evidence['scores'] ['association_score']= float(self.evidence['evidence']['disease_model_association']['resource_score']['value'])
            elif self.evidence['type']=='somatic_mutation':
                self.evidence['scores'] ['association_score']= float(self.evidence['evidence']['resource_score']['value'])
            elif self.evidence['type']=='literature':
                self.evidence['scores'] ['association_score']= float(self.evidence['evidence']['resource_score']['value'])
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
            logging.warn("Cannot score evidence %s of type %s"%(self.evidence['id'],self.evidence['type']))

        '''modify scores accodigng to weights'''
        datasource_weight = Config.DATASOURCE_ASSOCIATION_SCORE_WEIGHT.get( self.evidence['sourceID'], 1.)
        if datasource_weight !=1:
            weighted_score =  self.evidence['scores'] ['association_score']*datasource_weight
            if weighted_score >1:
               weighted_score = 1.
            self.evidence['scores'] ['association_score'] =  weighted_score

        '''applay rescaling to scores'''
        if self.evidence['sourceID'] in modifiers:
            self.evidence['scores'] ['association_score'] =  modifiers[self.evidence['sourceID']]( self.evidence['scores'] ['association_score'])


    def _get_score_from_pvalue(self, pvalue):
        score = 0.
        if pvalue <= 1e-10:
            score=1.
        elif pvalue <= 1e-9:
            score=.9
        elif pvalue <= 1e-8:
            score=.8
        elif pvalue <= 1e-7:
            score=.7
        elif pvalue <= 1e-6:
            score=.6
        elif pvalue <= 1e-5:
            score=.5
        elif pvalue <= 1e-4:
            score=.4
        elif pvalue <= 1e-3:
            score=.3
        elif pvalue <= 1e-2:
            score=.2
        elif pvalue <= 1e-1:
            score=.1
        elif pvalue <= 1:
            score=0.
        return score

    def _score_gwascatalog(self,pvalue,sample_size, severity):

        normalised_pvalue = 0.
        if pvalue <= 1e-15:
            normalised_pvalue=1.
        elif pvalue <= 1e-10:
            normalised_pvalue=7/8.
        elif pvalue <= 1e-9:
            normalised_pvalue=6/8.
        elif pvalue <= 1e-8:
            normalised_pvalue=5/8.
        elif pvalue <= 1e-7:
            normalised_pvalue=4/8.
        elif pvalue <= 1e-6:
            normalised_pvalue=3/8.
        elif pvalue <= 1e-5:
            normalised_pvalue=2/8.
        elif pvalue <= 1:
            normalised_pvalue=1/8.

        normalised_sample_size = 0.
        if sample_size >= 100000 :
            normalised_sample_size=1.
        elif sample_size  >= 75000 :
            normalised_sample_size=13/14.
        elif sample_size  >= 50000 :
            normalised_sample_size=12/14.
        elif sample_size  >= 25000 :
            normalised_sample_size=11/14.
        elif sample_size  >= 10000 :
            normalised_sample_size=10/14.
        elif sample_size  >= 7500 :
            normalised_sample_size=9/14.
        elif sample_size  >= 5000 :
            normalised_sample_size=8/14.
        elif sample_size  >= 4000 :
            normalised_sample_size=7/14.
        elif sample_size  >= 3000 :
            normalised_sample_size=6/14.
        elif sample_size  >= 2000 :
            normalised_sample_size=5/14.
        elif sample_size  >= 1000 :
            normalised_sample_size=4/14.
        elif sample_size  >= 750 :
            normalised_sample_size=3/14.
        elif sample_size  >= 500 :
            normalised_sample_size=2/14.
        elif sample_size  >= 1 :
            normalised_sample_size=1/14.



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
            self.database = evidence['evidence']['sourceID']
        except:
            self.database = 'unknown'
        self.logdir = logdir

    def save(self):
        dir = os.path.join(self.logdir, self.database)
        if not os.path.exists(self.logdir):
            os.mkdir(self.logdir)
        if not os.path.exists(dir):
            os.mkdir(dir)
        filename = str(os.path.join(dir, self.id))
        pickle.dump(self, open(filename + '.pkl', 'w'))
        json.dump(self.evidence, open(filename + '.json', 'w'))


class DataNormaliser(object):
    def __init__(self, min_value, max_value):
        self.min = float(min_value)
        self.max = float(max_value)
    def __call__(self, value):
        return (value-self.min)/(self.max-self.min)






class EvidenceStringProcess():

    def __init__(self,
                 adapter):
        self.adapter=adapter
        self.session=adapter.session
        self.data=OrderedDict()
        self.loaded_entries_to_pg = 0

    def process_all(self):
        self._process_evidence_string_data()



    def _process_evidence_string_data(self):

        base_id = 0
        err = 0
        fix = 0
        evidence_manager = EvidenceManager(self.adapter,)
        self._delete_prev_data()
        # for row in self.session.query(LatestEvidenceString).yield_per(1000):
        for row in self.session.query(EvidenceString121).yield_per(1000):
            ev = Evidence(row.evidence_string, datasource= row.data_source_name)
            idev = row.uniq_assoc_fields_hashdig
            ev.evidence['id'] = idev
            base_id += 1
            try:
            # if 1:
                # print idev, row.data_source_name
                '''temporary: fix broken data '''
                ev, fixed = evidence_manager.fix_evidence(ev)
                if fixed:
                    fix += 1
                if evidence_manager.is_valid(ev, datasource=row.data_source_name):
                    '''add scoring to evidence string'''
                    ev.score_evidence(evidence_manager.score_modifiers)
                    '''extend data in evidencestring'''
                    ev_string_to_load = evidence_manager.get_extended_evidence(ev)

                    self.data[idev] = ev_string_to_load

                else:
                    # traceback.print_exc(limit=1, file=sys.stdout)
                    raise AttributeError("Invalid %s Evidence String" % (row.data_source_name))


            except Exception, error:
                UploadError(ev, error, idev).save()
                err += 1
                logging.exception("Error loading data for id %s: %s" % (idev, str(error)))
                # traceback.print_exc(limit=1, file=sys.stdout)
            if len(self.data)>=1000:
                self._store_evidence_string()
                logging.info("%i entries processed with %i errors and %i fixes" % (base_id, err, fix))
        self._store_evidence_string()
        self.session.commit()
        logging.info("%i entries processed with %i errors and %i fixes" % (base_id, err, fix))
        return



    def _delete_prev_data(self):
        JSONObjectStorage.delete_prev_data_in_pg(self.session,
                                                 Config.ELASTICSEARCH_DATA_INDEX_NAME)

    def _store_evidence_string(self):
        for key, value in self.data.iteritems():
            self.loaded_entries_to_pg += 1
            self.session.add(ElasticsearchLoad(id=key,
                                          index=Config.ELASTICSEARCH_DATA_INDEX_NAME,
                                          type=value.get_doc_name(),
                                          data=value.to_json(),
                                          active=True,
                                          date_created=datetime.now(),
                                          date_modified=datetime.now(),
                                          ))
        logging.info("%i rows of evidence strings inserted to elasticsearch_load" % self.loaded_entries_to_pg)
        self.session.flush()
        self.data=OrderedDict()



class EvidenceStringUploader():

    def __init__(self,
                 adapter,
                 loader):
        self.adapter=adapter
        self.session=adapter.session
        self.loader=loader

    def upload_all(self):
        JSONObjectStorage.refresh_index_data_in_es(self.loader,
                                         self.session,
                                         Config.ELASTICSEARCH_DATA_INDEX_NAME
                                         )

class EvidenceStringRetriever():
    """
    Will retrieve a Gene object form the processed json stored in postgres
    """
    def __init__(self,
                 adapter):
        self.adapter=adapter
        self.session=adapter.session

    def get_gene(self, evidenceid):
        json_data = JSONObjectStorage.get_data_from_pg(self.session,
                                                       Config.ELASTICSEARCH_DATA_INDEX_NAME,
                                                       Config.ELASTICSEARCH_DATA_DOC_NAME,
                                                       evidenceid)
        evidence = Evidence(json_data)
        return evidence