from collections import defaultdict, OrderedDict
import copy
from datetime import datetime
import json
import logging
import os
import pickle
from sqlalchemy import and_
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
        self.uni_header=GeneData.UNI_ID_ORG_PREFIX
        self.ens_header=GeneData.ENS_ID_ORG_PREFIX
        self.gene_retriever = GeneRetriever(adapter)
        self.efo_retriever = EfoRetriever(adapter)
        self.eco_retriever = EcoRetriever(adapter)



    def fix_evidence(self, evidence):

        evidence = evidence.evidence
        fixed = False
        # '''Temporary,  fix errors in data'''
        # if 'probability' in evidence['evidence']["association_score"]:
        #     if (isinstance(evidence['evidence']["association_score"]["probability"], int) or
        #             isinstance(evidence['evidence']["association_score"]["probability"], float)):
        #         evidence['evidence']["association_score"]["probability"] = {
        #                                                             "value": evidence['evidence']["association_score"]["probability"],
        #                                                             "method": ""}
        #         logging.warning("Malformed probability in evidence.association_score in entry %s | fixed to make it an object"%evidence['id'])
        #         fixed = 'Probability in evidence malformed'
        #     if (isinstance(evidence['evidence']["association_score"]["probability"], unicode) or
        #                     isinstance(evidence['evidence']["association_score"]["probability"], str)):
        #         evidence['evidence']["association_score"]["probability"] = {
        #                                                             "value": float(evidence['evidence']["association_score"]["probability"]),
        #                                                             "method": ""}
        #         logging.warning("Malformed probability in evidence.association_score in entry %s | fixed to make it an object"%evidence['id'])
        #         fixed = 'Probability in evidence chain malformed and as string'
        #
        # if 'pvalue' in evidence['evidence']["association_score"]:
        #     if evidence['evidence']["association_score"]['pvalue']:
        #         if (isinstance(evidence['evidence']['association_score']['pvalue']['value'], unicode) or
        #                 isinstance(evidence['evidence']['association_score']['pvalue']['value'], str)):
        #             evidence['evidence']['association_score']['pvalue']['value'] = float(evidence['evidence']['association_score']['pvalue']['value'])
        #             logging.warning("string instead of double in evidence.association_score.pvalue.value in entry %s | fixed to comply Java parser"%evidence['id'])
        #             fixed = 'malformed evidence.association_score.pvalue.value'
        #
        # if 'properties' in evidence['evidence']:
        #     if 'evidence_chain' in evidence['evidence']['properties']:
        #         evidence['evidence']['evidence_chain'] = evidence['evidence']['properties'].pop('evidence_chain')
        # if 'evidence_chain' in evidence['evidence']:
        #     for i, ec in enumerate(evidence['evidence']['evidence_chain']):
        #         if "probability" in ec['evidence']["association_score"]:
        #             if (isinstance(ec['evidence']["association_score"]["probability"], int) or
        #                     isinstance(ec['evidence']["association_score"]["probability"], float)):
        #                 evidence['evidence']['evidence_chain'][i]['evidence']["association_score"]["probability"] = {
        #                 "value": ec['evidence']["association_score"]["probability"],
        #                 "method": ""}
        #                 logging.warning("Malformed probability in evidence.evidence_chainevidence.association_score in entry %s fixed to make it an object"%evidence['id'])
        #                 fixed = 'Probability in evidence chain malformed'
        #             if (isinstance(ec['evidence']["association_score"]["probability"], unicode) or
        #                     isinstance(ec['evidence']["association_score"]["probability"], str)):
        #                 evidence['evidence']['evidence_chain'][i]['evidence']["association_score"]["probability"] = {
        #                                                                                                             "value": float(ec['evidence']["association_score"]["probability"]),
        #                                                                                                             "method": ""}
        #                 logging.warning("Malformed probability in evidence.evidence_chain.evidence.association_score in entry %s | fixed to make it an object"%evidence['id'])
        #                 fixed = 'Probability in evidence chain malformed and as string'
        #         if "pvalue" in ec['evidence']["association_score"]:
        #             if (isinstance(ec['evidence']["association_score"]["pvalue"]['value'], unicode) or
        #                     isinstance(ec['evidence']["association_score"]["pvalue"]['value'], str)):
        #                 evidence['evidence']['evidence_chain'][i]['evidence']["association_score"]["pvalue"] ['value']= float(ec['evidence']["association_score"]["pvalue"]['value'])
        #                 logging.warning("string instead of double in evidence.evidence_chain.evidence.association_score.pvalue.value in entry %s | fixed to comply Java parser"%evidence['id'])
        #                 fixed = 'malformed evidence.evidence_chain.evidence.association_score.pvalue.value'
        #             pvalue = evidence['evidence']['evidence_chain'][i]['evidence']["association_score"]["pvalue"]["value"]
        #             if  pvalue>1:
        #                 logging.error("pvalue in entry %s is higher than 1 %f"%(evidence['id'], pvalue))
        #                 raise AttributeError('Invalid pvalue value')
        #
        #         if 'date_asserted' in ec['evidence']:  # remove time, only load date
        #             if ec['evidence']['date_asserted']:
        #                 date = smart_date_parser.parse(ec['evidence']['date_asserted'])
        #                 evidence['evidence']['evidence_chain'][i]['evidence']['date_asserted'] = date.date().isoformat()
        #
        # if 'type' in evidence['evidence']:
        #     provenance_type = evidence['evidence'].pop('type')
        #     evidence['evidence']['provenance_type'] = provenance_type
        #     fixed = 'type instead of provenance_type'
        # if 'date_asserted' in evidence['evidence']:  # remove time, only load date
        #     if evidence['evidence']['date_asserted']:
        #         date = smart_date_parser.parse(evidence['evidence']['date_asserted'])
        #         evidence['evidence']['date_asserted'] = date.date().isoformat()
        #
        # '''remove identifiers.org from genes and map to ensembl ids'''
        # sbj_about = evidence['biological_subject']['about']
        # new_sbj_about = []
        # id_not_in_ensembl = []
        # for a in sbj_about:
        #     try:
        #         if a.startswith(self.uni_header):
        #             if '-' in a:
        #                 a = a.split('-')[0]
        #             uniprotid = a.split(self.uni_header)[1].strip()
        #             ensemblid = self.uni2ens[uniprotid]
        #             new_sbj_about.append(self.get_reference_ensembl_id(ensemblid))
        #         elif a.startswith(self.ens_header):
        #             ensemblid = a.split(self.ens_header)[1].strip()
        #             new_sbj_about.append(self.get_reference_ensembl_id(ensemblid))
        #         else:
        #             logging.warning("could not recognize biological_subject: %s | not added" % a)
        #             id_not_in_ensembl.append(a)
        #     except KeyError:
        #         logging.error("cannot find an ensembl ID for: %s" % a)
        #         id_not_in_ensembl.append(a)
        #
        # if id_not_in_ensembl and not new_sbj_about:
        #     logging.warning("cannot find any ensembl ID for evidence for: %s" % (evidence['id']))
        #
        # evidence['biological_subject']['about'] = list(set(new_sbj_about))
        #
        #
        # '''remove identifiers.org from cttv activity  and target type ids'''
        # if 'properties' in evidence['biological_subject']:
        #     if 'target_type' in evidence['biological_subject']['properties']:
        #         evidence['biological_subject']['properties']['target_type'] = evidence['biological_subject']['properties']['target_type'].split('/')[-1]
        #     if 'activity' in evidence['biological_subject']['properties']:
        #         evidence['biological_subject']['properties']['activity'] = evidence['biological_subject']['properties']['activity'].split('/')[-1]
        #
        #
        # '''remove identifiers.org from efos'''
        # obj_about = evidence['biological_object']['about']
        # new_obj_about = []
        # for idorg_efo_uri in obj_about:
        #     # if idorg_efo_uri in self.idorg2efos:
        #     #     efo_uri = self.idorg2efos[idorg_efo_uri]
        #     #     code = get_ontology_code_from_url(efo_uri.strip())
        #         code = get_ontology_code_from_url(idorg_efo_uri)
        #         if len(code.split('_')) != 2:
        #             logging.warning("could not recognize biological_object: %s | added anyway" % idorg_efo_uri)
        #         new_obj_about.append(code)
        # evidence['biological_object']['about'] = list(set(new_obj_about))
        # if not new_obj_about:
        #     logging.warning("No valid EFO could be found in evidence: %s. original EFO mapping: %s"%(evidence['id'], str(obj_about)[:100]))
        #
        # '''remove identifiers.org from ecos'''
        # eco_ids = evidence['evidence']['evidence_codes']
        # new_eco_ids = []
        # for idorg_eco_uri in eco_ids:
        #     # if idorg_eco_uri in self.idorg2ecos:
        #     #     eco_id = self.idorg2ecos[idorg_eco_uri]
        #     #     code = get_ontology_code_from_url(eco_id.strip())
        #         code = get_ontology_code_from_url(idorg_eco_uri.strip())
        #         if len(code.split('_')) != 2:
        #             logging.warning("could not recognize evidence code: %s in id %s | added anyway" %(evidence['id'],idorg_eco_uri))
        #         new_eco_ids.append(code)
        # evidence['evidence']['evidence_codes'] = list(set(new_eco_ids))
        # if not new_eco_ids:
        #     logging.warning("No valid ECO could be found in evidence: %s. original ECO mapping: %s"%(evidence['id'], str(eco_ids)[:100]))
        # ''' add GWAS as databse id'''
        # if 'GWAS' in evidence['evidence']['evidence_codes']:
        #     if 'provenance_type' not in evidence['evidence']:
        #         evidence['evidence']['provenance_type'] = {}
        #     if 'database' not in evidence['evidence']['provenance_type']:
        #         evidence['evidence']['provenance_type']['database'] = {}
        #     evidence['evidence']['provenance_type']['database']['id'] = 'gwas'
        #
        #
        # if 'evidence_chain' in evidence['evidence']:
        #     for i, ec in enumerate(evidence['evidence']['evidence_chain']):
        #         ec_eco_ids = ec['evidence']['evidence_codes']
        #         new_ec_eco_ids = []
        #         for idorg_eco_uri in ec_eco_ids:
        #             # if idorg_eco_uri in self.idorg2ecos:
        #             #     eco_id = self.idorg2ecos[idorg_eco_uri]
        #             #     code = get_ontology_code_from_url(eco_id.strip())
        #                 code = get_ontology_code_from_url(idorg_eco_uri.strip())
        #                 if len(code.split('_')) != 2:
        #                     logging.warning(
        #                         "could not recognize evidence code in evidence chain: %s in id %s | added anyway" % (evidence['id'],idorg_eco_uri,))
        #                 new_ec_eco_ids.append(code)
        #         evidence['evidence']['evidence_chain'][i]['evidence']['evidence_codes'] = list(set(new_ec_eco_ids))
        #         if not new_ec_eco_ids:
        #             logging.warning("No valid ECO could be found in evidence chain element %i of evidence: %s. original ECO mapping: %s"%(i, evidence['id'], str(ec_eco_ids)[:100]))

        return Evidence(evidence), fixed

    def is_valid(self, evidence, datasource):
        '''check consistency of the data in the evidence'''

        ev = evidence.evidence
        evidence_id = ev['id']
        #
        # if not ev['biological_subject']['about']:
        #     logging.error("%s Evidence %s has no valid gene in biological_subject.about" % (datasource, evidence_id))
        #     return False
        # for gene_id in ev['biological_subject']['about']:
        #     if gene_id not in self.available_genes:
        #         logging.error(
        #             "%s Evidence %s has an invalid gene id in biological_subject.about: %s" % (datasource, evidence_id, gene_id))
        #         return False
        # if not ev['biological_object']['about']:
        #     logging.error("%s Evidence %s has no valid efo id in biological_object.about" % (datasource, evidence_id))
        #     return False
        # for efo_id in ev['biological_object']['about']:
        #     if efo_id not in self.available_efos:
        #         logging.error(
        #             "%s Evidence %s has an invalid efo id in biological_object.about: %s" % (datasource, evidence_id, efo_id))
        #         return False
        # for eco_id in ev['evidence']['evidence_codes']:
        #     if eco_id not in self.available_ecos:
        #         logging.error(
        #             "%s Evidence %s has an invalid eco id in evidence.evidence_codes: %s" % (datasource, evidence_id, eco_id))
        #         return False
        # if 'evidence_chain' in ev['evidence']:
        #     for ec in ev['evidence']['evidence_chain']:
        #         for eco_id in ec['evidence']['evidence_codes']:
        #             if eco_id not in self.available_ecos:
        #                 logging.error(
        #                     "%s Evidence %s has an invalid eco id in evidence.evidence_chain.evidence.evidence_codes: %s" % (
        #                     datasource, evidence_id, eco_id))
        #                 return False
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
        # for aboutid in extended_evidence['biological_subject']['about']:
        #     # try:
        #     gene = self._get_gene(aboutid)
        #     genes_info.append(ExtendedInfoGene(gene))
        #     if 'reactome' in gene._private['facets']:
        #         pathway_data['pathway_type_code'].extend(gene._private['facets']['reactome']['pathway_type_code'])
        #         pathway_data['pathway_code'].extend(gene._private['facets']['reactome']['pathway_code'])
        #         # except Exception:
        #         #     logging.warning("Cannot get generic info for gene: %s" % aboutid)
        #     if gene.go:
        #         for go_code,data in gene.go.items():
        #             try:
        #                 category,term = data['term'][0], data['term'][2:]
        #                 if category =='P':
        #                     GO_terms['biological_process'].append(dict(code=go_code,
        #                                                                term=term))
        #                 elif category =='F':
        #                     GO_terms['molecular_function'].append(dict(code=go_code,
        #                                                                term=term))
        #                 elif category =='C':
        #                     GO_terms['cellular_component'].append(dict(code=go_code,
        #                                                                term=term))
        #             except:
        #                 pass
        #     if gene.uniprot_keywords:
        #         uniprot_keywords = gene.uniprot_keywords

        if genes_info:
            data = []
            for gene_info in genes_info:
                data.append(gene_info.data)
            extended_evidence["biological_subject"][ExtendedInfoGene.root] = data

        if pathway_data['pathway_code']:
            pathway_data['pathway_type_code']=list(set(pathway_data['pathway_type_code']))
            pathway_data['pathway_code']=list(set(pathway_data['pathway_code']))


        """get generic efo info"""
        all_efo_codes=[]
        efos_info = []
        # for aboutid in extended_evidence['biological_object']['about']:
        #     # try:
        #     efo = self._get_efo(aboutid)
        #     efos_info.append(ExtendedInfoEFO(efo))
        #     # except Exception:
        #     #     logging.warning("Cannot get generic info for efo: %s" % aboutid)

        if efos_info:
            data = []
            for efo_info in efos_info:
                data.append(efo_info.data)
                for e in efo_info.data:
                    for node in e['path']:
                        all_efo_codes.extend(node)
            extended_evidence["biological_object"][ExtendedInfoEFO.root] = data
        all_efo_codes = list(set(all_efo_codes))
        """get generic eco info"""
        ecos_info = []
        # for eco_id in extended_evidence['evidence']['evidence_codes']:
        #     # try:
        #     eco = self._get_eco(eco_id)
        #     ecos_info.append(ExtendedInfoECO(eco))
        #     # except Exception:
        #     #     logging.warning("Cannot get generic info for eco: %s" % eco_id)

        if ecos_info:
            data = []
            for eco_info in ecos_info:
                data.append(eco_info.data)
            extended_evidence['evidence'][ExtendedInfoECO.root] = data

        '''Add private objects used just for indexing'''

        extended_evidence['_private']['efo_codes'] = all_efo_codes
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
                logging.info("Mapped non reference ensembl gene id %s to %s for gene %s"%(ensg, data['reference'], symbol ))
                return data['reference']

    def get_reference_ensembl_id(self, ensemblid):
        if ensemblid not in self.available_genes:
            ensemblid = self._map_to_reference_ensembl_gene(ensemblid) or ensemblid
        return ensemblid



class Evidence(JSONSerializable):
    def __init__(self, evidence, datasource = ""):
        if isinstance(evidence, str):
            self.load_json(evidence)
        elif isinstance(evidence, dict):
            self.evidence = evidence
        else:
            raise AttributeError(
                "the evidence should be a dict or a json string to parse, not a " + str(type(evidence)))
        self.datasource = datasource
        self._set_datatype()


    def _set_datatype(self,):

        translate_database = defaultdict(lambda: "other")
        translate_database['expression_atlas'] = 'rna_expression'
        translate_database['uniprot'] = 'genetic_association'
        translate_database['reactome'] = 'affected_pathway'
        translate_database['eva'] = 'genetic_association'
        translate_database['phenodigm'] = 'animal_model'
        translate_database['gwas'] = 'genetic_association'
        translate_database['cancer_gene_census'] = 'somatic_mutation'
        translate_database['chembl'] = 'known_drug'
        translate_database['europmc'] = 'literature'
        translate_database['disgenet'] = 'literature'
        try:
            self.database = self.evidence['evidence']['provenance_type']['database']['id'].lower()
        except KeyError:
            self.database = self.datasource.lower()
        self.datatype = translate_database[self.database]

    def get_doc_name(self):
        return Config.ELASTICSEARCH_DATA_DOC_NAME+'-'+self.database,


    def to_json(self):
        return json.dumps(self.evidence)

    def load_json(self, data):
        self.evidence = json.loads(data)

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
            self.database = evidence['evidence']['provenance_type']['database']['id']
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
        evidence_manager = EvidenceManager(self.adapter)
        self._delete_prev_data()
        # for row in self.session.query(LatestEvidenceString).yield_per(1000):
        for row in self.session.query(EvidenceString121).yield_per(1000):
            ev = Evidence(row.evidence_string, datasource= row.data_source_name)
            idev = row.uniq_assoc_fields_hashdig
            ev.evidence['id'] = idev
            base_id += 1
            # try:
            if 1:
                # print idev, row.data_source_name
                '''temporary: fix broken data '''
                ev, fixed = evidence_manager.fix_evidence(ev)
                if fixed:
                    fix += 1
                if evidence_manager.is_valid(ev, datasource=row.data_source_name):
                    '''extend data in evidencestring'''
                    ev_string_to_load = evidence_manager.get_extended_evidence(ev)

                    self.data[idev] = ev_string_to_load

                else:
                    raise AttributeError("Invalid %s Evidence String" % (row.data_source_name))

            #
            # except Exception, error:
            #     UploadError(ev, error, idev).save()
            #     err += 1
            #     logging.exception("Error loading data for id %s: %s" % (idev, str(error)))
                # traceback.print_exc(limit=1, file=sys.stdout)
            if len(self.data)>1000:
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