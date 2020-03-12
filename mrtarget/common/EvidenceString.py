from __future__ import division
from builtins import str
from builtins import object
from past.utils import old_div
import copy
import json
import logging
import math

import csv

from mrtarget.common.DataStructure import JSONSerializable, PipelineEncoder
from mrtarget.common.IO import check_to_open,file_or_resource
from mrtarget.modules import GeneData
from mrtarget.modules.ECO import ECO
from mrtarget.modules.EFO import EFO, get_ontology_code_from_url
from mrtarget.modules.GeneData import Gene

from opentargets_urlzsource import URLZSource

class DataNormaliser(object):
    def __init__(self, min_value, max_value, old_min_value=0., old_max_value=1., cap=True):
        '''just set all initial values and ranges'''
        self.min = float(min_value)
        self.max = float(max_value)
        self.old_min = old_min_value
        self.old_max = old_max_value
        self.cap = cap

    def __call__(self, value):
        '''apply method to wrap the normalization function'''
        return self.renormalize(value,
                                (self.old_min, self.old_max),
                                (self.min, self.max),
                                self.cap)

    @staticmethod
    def renormalize(n, start_range, new_range, cap=True):
        '''apply the function f(x) to n using and old (start_range) and a new range

        where f(x) = (dNewRange / dOldRange * (n - old_range_lower_bound)) + new_lower
        if cap is True then f(n) will be capped to new range boundaries
        '''
        n = float(n)
        max_new_range = max(new_range)
        min_new_range = min(new_range)
        delta1 = start_range[1] - start_range[0]
        delta2 = new_range[1] - new_range[0]
        if delta1 or delta2:
            try:
                normalized = (old_div(delta2 * (n - start_range[0]), delta1)) + new_range[0]
            except ZeroDivisionError:
                normalized = new_range[0]
        else:
            normalized = n
        if cap:
            if normalized > max_new_range:
                return max_new_range
            elif normalized < min_new_range:
                return min_new_range
        return normalized


class ExtendedInfo(object):
    data = dict()

    def extract_info(self, obj):
        raise NotImplementedError()

    def to_json(self):
        return json.dumps(self.data)

    def load_json(self, data):
        self.data = json.loads(data)


class ExtendedInfoGene(ExtendedInfo):
    '''minimal info from Gene class'''
    root = "gene_info"

    def __init__(self, gene):
        if isinstance(gene, Gene):
            self.extract_info(gene)
        else:
            raise AttributeError("you need to pass a Gene not a: " + str(type(gene)))

    def extract_info(self, gene):
        self.data = dict(geneid=gene.id,
                         symbol=gene.approved_symbol or gene.ensembl_external_name,
                         name=gene.approved_name or gene.ensembl_description)


class ExtendedInfoEFO(ExtendedInfo):
    '''getting from and EFO obj label id and building 2 sets of area codes
    and area labels
    '''
    root = "efo_info"

    def __init__(self, efo):
        if isinstance(efo, EFO):
            self.extract_info(efo)
        else:
            raise AttributeError("you need to pass a EFO not a: " + str(type(efo)))

    def extract_info(self, efo):
        self.data = dict(efo_id=efo.get_id(),
            label=efo.label,
            path=efo.path_codes,
            therapeutic_area=dict(
                codes=list(efo.therapeutic_codes),
                labels=list(efo.therapeutic_labels)))


class ExtendedInfoECO(ExtendedInfo):
    root = "evidence_codes_info"

    def __init__(self, eco):
        if isinstance(eco, ECO):
            self.extract_info(eco)
        else:
            raise AttributeError("you need to pass a ECO not a: " + str(type(eco)))

    def extract_info(self, eco):
        self.data = dict(eco_id=eco.get_id(),
                         label=eco.label),


class EvidenceManager(object):
    def __init__(self, lookup_data, eco_scores_uri, excluded_biotypes, datasources_to_datatypes):
        self.logger = logging.getLogger(__name__)
        self.available_genes = lookup_data.available_genes
        self.available_efos = lookup_data.available_efos
        self.available_ecos = lookup_data.available_ecos
        self.non_reference_genes = lookup_data.non_reference_genes

        #pre-load eco scores into memory
        self.eco_scores = {}
        with URLZSource(eco_scores_uri).open() as r_file:
            for row in csv.DictReader(r_file, fieldnames=["uri", "code", "score"], 
                    dialect='excel-tab'):
                eco_uri = row["uri"]
                self.eco_scores[eco_uri] = float(row["score"])


        self.uni_header = GeneData.UNI_ID_ORG_PREFIX
        self.ens_header = GeneData.ENS_ID_ORG_PREFIX

        self.excluded_biotypes = excluded_biotypes
        self.datasources_to_datatypes = datasources_to_datatypes


    # @do_profile()#follow=[])
    def fix_evidence(self, evidence):

        evidence = evidence.evidence
        fixed = False

        # fix errors in data here so nobody needs to ask corrections to the data provider
        # fix missing version in gwas catalog data
        if 'variant2disease' in evidence:
            try:
                float(evidence['evidence']['variant2disease']['provenance_type']['database']['version'])
            except:
                evidence['evidence']['variant2disease']['provenance_type']['database']['version'] = ''
                fixed = True
            try:
                float(evidence['evidence']['variant2disease']['provenance_type']['database']['dbxref']['version'])
            except:
                evidence['evidence']['variant2disease']['provenance_type']['database']['dbxref']['version'] = ''
                fixed = True
        if 'gene2variant' in evidence:
            try:
                float(evidence['evidence']['gene2variant']['provenance_type']['database']['version'])
            except:
                evidence['evidence']['gene2variant']['provenance_type']['database']['version'] = ''
                fixed = True
            try:
                float(evidence['evidence']['gene2variant']['provenance_type']['database']['dbxref']['version'])
            except:
                evidence['evidence']['gene2variant']['provenance_type']['database']['dbxref']['version'] = ''
                fixed = True
        # Split EVA in two datasources depending on the datatype
        if (evidence['sourceID'] == 'eva') and \
                (evidence['type'] == 'somatic_mutation'):
            evidence['sourceID'] = 'eva_somatic'
            fixed = True
        # Move genetic_literature to genetic_association
        if evidence['type'] == 'genetic_literature':
            evidence['type'] = 'genetic_association'

        if 'provenance_type' in evidence and \
                        'database' in evidence['provenance_type'] and \
                        'version' in evidence['provenance_type']['database']:
            evidence['provenance_type']['database']['version'] = str(evidence['provenance_type']['database']['version'])

        # Enforce eco-based score for genetic_association evidencestrings
        if evidence['type'] == 'genetic_association':
            available_score = None
            eco_uri = None
            try:
                available_score = evidence['evidence']['gene2variant']['resource_score']['value']
            except KeyError:
                if 'resource_score' in evidence['evidence'] and \
                                'value' in evidence['evidence']['resource_score']:
                    available_score = evidence['evidence']['resource_score']['value']
            try:
                eco_uri = evidence['evidence']['gene2variant']['functional_consequence']
                if 'evidence_codes' in evidence['evidence']:
                    eco_uri = evidence['evidence']['evidence_codes']
            except KeyError:
                if 'evidence_codes' in evidence['evidence']:
                    eco_uri = evidence['evidence']['evidence_codes'][0]
                    eco_uri.rstrip()

            if eco_uri in self.eco_scores:
                if 'gene2variant' in evidence['evidence']:
                    if 'resource_score' not in evidence['evidence']['gene2variant']:
                        evidence['evidence']['gene2variant']['resource_score'] = {}
                        evidence['evidence']['gene2variant']['resource_score']['value'] = self.eco_scores[eco_uri]
                        evidence['evidence']['gene2variant']['resource_score']['type'] = 'probability'
                        if available_score != self.eco_scores[eco_uri]:
                            fixed = True
            else:
                self.logger.warning("Cannot find a score for eco code %s in evidence id %s" % (eco_uri, evidence['id']))

        # Remove identifiers.org from genes and map to ensembl ids
        self.fix_target_id(evidence, self.available_genes, self.non_reference_genes )

        # Remove identifiers.org from cttv activity  and target type ids
        if 'target_type' in evidence['target']:
            evidence['target']['target_type'] = evidence['target']['target_type'].split('/')[-1]
        if 'activity' in evidence['target']:
            evidence['target']['activity'] = evidence['target']['activity'].split('/')[-1]

        # Remove identifiers.org from efos
        self.fix_disease_id(evidence)

        # Remove identifiers.org from ecos
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
            eco_ids = []  # something wrong here...
        eco_ids = list(set(eco_ids))
        for idorg_eco_uri in eco_ids:
            code = get_ontology_code_from_url(idorg_eco_uri.strip())
            if code is not None:
                # if len(code.split('_')) != 2:
                # self.logger.warning("could not recognize evidence code: %s in id %s | added anyway" %(evidence['id'],
                # idorg_eco_uri))
                new_eco_ids.append(code)
        evidence['evidence']['evidence_codes'] = list(set(new_eco_ids))
        if not new_eco_ids:
            self.logger.warning("No valid ECO could be found in evidence: %s. original ECO mapping: %s" % (
                evidence['id'], str(eco_ids)[:100]))

        return Evidence(evidence,self.datasources_to_datatypes), fixed

    def normalise_target_id(self, evidence, available_genes,non_reference_genes ):

        target_id = evidence['target']['id']
        new_target_id = None
        id_not_in_ensembl = False
        try:
            if target_id.startswith(GeneData.UNI_ID_ORG_PREFIX):
                if '-' in target_id:
                    target_id = target_id.split('-')[0]
                uniprotid = target_id.split(GeneData.UNI_ID_ORG_PREFIX)[1].strip()
                ensemblid = available_genes.get_uniprot2ensembl(uniprotid)
                new_target_id = self.get_reference_ensembl_id(ensemblid,
                                                                         available_genes=available_genes,
                                                                         non_reference_genes=non_reference_genes)
            elif target_id.startswith(GeneData.ENS_ID_ORG_PREFIX):
                ensemblid = target_id.split(GeneData.ENS_ID_ORG_PREFIX)[1].strip()
                new_target_id = self.get_reference_ensembl_id(ensemblid,
                                                                         available_genes=available_genes,
                                                                         non_reference_genes=non_reference_genes)
            else:
                self.logger.warning("could not recognize target.id: %s | not added" % target_id)
                id_not_in_ensembl = True
        except KeyError:
            self.logger.error("cannot find an ensembl ID for: %s" % target_id)
            id_not_in_ensembl = True

        return new_target_id, id_not_in_ensembl

    def is_excluded_by_biotype(self, datasource, gene_id):
        is_excluded = False
        if datasource in self.excluded_biotypes:
            gene_obj = self.available_genes.get_gene(gene_id)
            if gene_obj['biotype'] in self.excluded_biotypes[datasource]:
                is_excluded = True

        return is_excluded

    def fix_target_id(self, evidence, available_genes, non_reference_genes, logger=logging.getLogger(__name__)) :
        target_id = evidence['target']['id']

        try:
            new_target_id, id_not_in_ensembl = self.normalise_target_id(
                evidence, available_genes, non_reference_genes)
        except KeyError:
            self.logger.error("cannot find an ensembl ID for: %s" % target_id)
            id_not_in_ensembl = True
            new_target_id = target_id

        if id_not_in_ensembl:
            self.logger.warning("cannot find any ensembl ID for evidence for: %s. Offending target.id: %s",
                            evidence['target']['id'], target_id)

        evidence['target']['id'] = new_target_id

    def fix_disease_id(self, evidence, logger=logging.getLogger(__name__)):
        disease_id = evidence['disease']['id']
        new_disease_id = get_ontology_code_from_url(disease_id)
        if len(new_disease_id.split('_')) != 2:
            self.logger.warning("could not recognize disease.id: %s | added anyway" % disease_id)
        evidence['disease']['id'] = new_disease_id
        if not new_disease_id:
            self.logger.warning("No valid disease.id could be found in evidence: %s. Offending disease.id: %s" % (
                evidence['id'], disease_id))


    def check_is_valid_evs(self, evidence, datasource):
        """check consistency of the data in the evidence and returns a tuple with (is_valid, problem_str)"""
        ev = evidence.evidence
        evidence_id = ev['id']

        if not ev['target']['id']:
            problem_str = "%s Evidence %s has no valid gene in target.id" % (datasource, evidence_id)
            return False, problem_str
        gene_id = ev['target']['id']
        if gene_id not in self.available_genes:
            problem_str = "%s Evidence %s has an invalid gene id in target.id: %s" % (datasource, evidence_id, gene_id)
            return False, problem_str
        if not ev['disease']['id']:
            problem_str = "%s Evidence %s has no valid efo id in disease.id" % (datasource, evidence_id)
            return False, problem_str
        efo_id = ev['disease']['id']
        if efo_id not in self.available_efos:
            problem_str = "%s Evidence %s has an invalid efo id in disease.id: %s" % (datasource, evidence_id, efo_id)
            return False, problem_str
        if self.is_excluded_by_biotype(datasource, gene_id):
            problem_str = "%s Evidence %s gene_id %s is an excluded biotype" % \
                                        (datasource, evidence_id, gene_id)
            return False, problem_str
        # well, it seems this evidence is probably valid
        return True, ''

    def is_valid(self, evidence, datasource):
        '''check consistency of the data in the evidence'''

        ev = evidence.evidence
        evidence_id = ev['id']

        if not ev['target']['id']:
            self.logger.error("%s Evidence %s has no valid gene in target.id" % (datasource, evidence_id))
            return False
        gene_id = ev['target']['id']
        if gene_id not in self.available_genes:
            self.logger.error(
                "%s Evidence %s has an invalid gene id in target.id: %s" % (datasource, evidence_id, gene_id))
            return False
        if not ev['disease']['id']:
            self.logger.error("%s Evidence %s has no valid efo id in disease.id" % (datasource, evidence_id))
            return False
        efo_id = ev['disease']['id']
        if efo_id not in self.available_efos:
            self.logger.error(
                "%s Evidence %s has an invalid efo id in disease.id: %s" % (datasource, evidence_id, efo_id))
            return False
        return True

    def get_extended_evidence(self, evidence):

        extended_evidence = copy.copy(evidence.evidence)
        extended_evidence['private'] = dict()

        # Get generic gene info
        genes_info = []
        pathway_data = dict(pathway_type_code=[],
                            pathway_code=[])
        GO_terms = dict(biological_process=[],
                        cellular_component=[],
                        molecular_function=[],
                        )
        target_class = dict(level1=[],
                            level2=[])
        uniprot_keywords = []
        # TODO: handle domains
        geneid = extended_evidence['target']['id']
        # try:
        gene = self._get_gene_obj(geneid)
        genes_info = ExtendedInfoGene(gene)
        if 'facets' in gene._private and 'reactome' in gene._private['facets']:
            pathway_data['pathway_type_code'].extend(gene._private['facets']['reactome']['pathway_type_code'])
            pathway_data['pathway_code'].extend(gene._private['facets']['reactome']['pathway_code'])
            # except Exception:
            #     self.logger.warning("Cannot get generic info for gene: %s" % aboutid)
        if gene.go:
            for go in gene.go:
                go_code, data = go['id'], go['value']
                try:
                    category, term = data['term'][0], data['term'][2:]
                    if category == 'P':
                        GO_terms['biological_process'].append(dict(code=go_code,
                                                                   term=term))
                    elif category == 'F':
                        GO_terms['molecular_function'].append(dict(code=go_code,
                                                                   term=term))
                    elif category == 'C':
                        GO_terms['cellular_component'].append(dict(code=go_code,
                                                                   term=term))
                except:
                    pass
        if gene.uniprot_keywords:
            uniprot_keywords = gene.uniprot_keywords

        if genes_info:
            extended_evidence["target"][ExtendedInfoGene.root] = genes_info.data

        if pathway_data['pathway_code']:
            pathway_data['pathway_type_code'] = list(set(pathway_data['pathway_type_code']))
            pathway_data['pathway_code'] = list(set(pathway_data['pathway_code']))
        if 'chembl' in gene.protein_classification and gene.protein_classification['chembl']:
            target_class['level1'].append([i['l1'] for i in gene.protein_classification['chembl'] if 'l1' in i])
            target_class['level2'].append([i['l2'] for i in gene.protein_classification['chembl'] if 'l2' in i])

        # Get generic efo info
        # can it happen you get no efo codes but just one disease?
        all_efo_codes = []
        diseaseid = extended_evidence['disease']['id']
        efo = self._get_efo_obj(diseaseid)
        efo_info = ExtendedInfoEFO(efo)

        if efo_info:
            for path in efo_info.data['path']:
                all_efo_codes.extend(path)
            extended_evidence["disease"][ExtendedInfoEFO.root] = efo_info.data

        all_efo_codes = list(set(all_efo_codes))

        # Get generic eco info
        try:
            all_eco_codes = extended_evidence['evidence']['evidence_codes']
            try:
                all_eco_codes.append(
                    get_ontology_code_from_url(extended_evidence['evidence']['gene2variant']['functional_consequence']))
            except KeyError:
                pass
            ecos_info = []
            for eco_id in all_eco_codes:
                eco = self._get_eco_obj(eco_id)
                if eco is not None:
                    ecos_info.append(ExtendedInfoECO(eco))
                else:
                    self.logger.warning("eco uri %s is not in the ECO LUT so it will not be considered as included", eco_id)

            if ecos_info:
                data = []
                for eco_info in ecos_info:
                    data.append(eco_info.data)
                extended_evidence['evidence'][ExtendedInfoECO.root] = data
        except Exception as e:
            extended_evidence['evidence'][ExtendedInfoECO.root] = None
            all_eco_codes = []
            # self.logger.exception("Cannot get generic info for eco: %s:"%str(e))

        # Add private objects used just for faceting
        extended_evidence['private']['efo_codes'] = all_efo_codes
        extended_evidence['private']['eco_codes'] = all_eco_codes
        extended_evidence['private']['datasource'] = evidence.datasource
        extended_evidence['private']['datatype'] = evidence.datatype
        extended_evidence['private']['facets'] = {}
        if pathway_data['pathway_code']:
            extended_evidence['private']['facets']['reactome'] = pathway_data
        if uniprot_keywords:
            extended_evidence['private']['facets']['uniprot_keywords'] = uniprot_keywords
        if GO_terms['biological_process'] or \
                GO_terms['molecular_function'] or \
                GO_terms['cellular_component']:
            extended_evidence['private']['facets']['go'] = GO_terms

        if target_class['level1']:
            extended_evidence['private']['facets']['target_class'] = target_class

        return Evidence(extended_evidence, self.datasources_to_datatypes)

    def _get_gene_obj(self, geneid):
        gene = Gene(geneid)
        gene.load_json(self.available_genes.get_gene(geneid))
        return gene

    def _get_efo_obj(self, efoid):
        efo = EFO(efoid)
        efo.load_json(self.available_efos.get_efo(efoid))
        return efo

    def _get_eco_obj(self, ecoid):
        try:
            eco = ECO(ecoid)
            eco.load_json(self.available_ecos.get_eco(ecoid))
            return eco
        except KeyError:
            return None

    def _get_non_reference_gene_mappings(self):
        self.non_reference_genes = {}
        skip_header = True
        for line in file(file_or_resource('genes_with_non_reference_ensembl_ids.tsv')):
            if skip_header:
                skip_header = False
            symbol, ensg, assembly, chr, is_ref = line.split()
            if symbol not in self.non_reference_genes:
                self.non_reference_genes[symbol] = dict(reference='',
                                                        alternative=[])
            if is_ref == 't':
                self.non_reference_genes[symbol]['reference'] = ensg
            else:
                self.non_reference_genes[symbol]['alternative'].append(ensg)

    @staticmethod
    def _map_to_reference_ensembl_gene(ensg, non_reference_genes, logger=logging.getLogger(__name__)):
        for symbol, data in list(non_reference_genes.items()):
            if ensg in data['alternative']:
                logger.warning(
                    "Mapped non reference ensembl gene id %s to %s for gene %s" % (ensg, data['reference'], symbol))
                return data['reference']
    @staticmethod
    def get_reference_ensembl_id(ensemblid, available_genes, non_reference_genes):
        if ensemblid not in available_genes:
            ensemblid = EvidenceManager._map_to_reference_ensembl_gene(ensemblid, non_reference_genes) or ensemblid
        return ensemblid



class Evidence(JSONSerializable):
    def __init__(self, evidence, datasources_to_datatypes):
        self.logger = logging.getLogger(__name__)
        if isinstance(evidence, dict):
            self.evidence = evidence
        else:
            self.load_json(evidence)
            
        self.datasource = self.evidence['sourceID']
        self.datatype = datasources_to_datatypes[self.datasource]

    def get_id(self):
        return self.evidence['id']

    def to_json(self):
        return json.dumps(self.evidence,
                          sort_keys=True,
                          # indent=4,
                          cls=PipelineEncoder)

    def load_json(self, data):
        self.evidence = json.loads(data)

    def score_evidence(self):
        self.evidence['scores'] = dict(association_score=0.,
                                       )
        try:
            if self.evidence['type'] == 'known_drug':
                self.evidence['scores']['association_score'] = \
                    float(self.evidence['evidence']['drug2clinic']['resource_score']['value']) * \
                    float(self.evidence['evidence']['target2drug']['resource_score']['value'])
            elif self.evidence['type'] == 'rna_expression':
                pvalue = self._get_score_from_pvalue_linear(self.evidence['evidence']['resource_score']['value'])
                log2_fold_change = self.evidence['evidence']['log2_fold_change']['value']
                fold_scale_factor = abs(log2_fold_change) / 10.
                rank = self.evidence['evidence']['log2_fold_change']['percentile_rank'] / 100.
                score = pvalue * fold_scale_factor * rank
                if score > 1:
                    score = 1.
                self.evidence['scores']['association_score'] = score

            elif self.evidence['type'] == 'genetic_association':
                score = 0.
                if 'gene2variant' in self.evidence['evidence']:

                    # Score calculation for phewas catalog and the 23andme dataset:
                    if self.evidence['sourceID'] in ['phewas_catalog','twentythreeandme']:
                        no_of_cases = self.evidence['unique_association_fields']['cases']
                        score = self._score_phewas_data(self.evidence['sourceID'],
                                                        self.evidence['evidence']['variant2disease']['resource_score'][
                                                            'value'],
                                                        no_of_cases)

                    # Score calculation for genetics portal sourced evidence:
                    elif  self.evidence['sourceID'] == 'ot_genetics_portal':
                        try:
                            # Locus 2 gene core directly used as evidence score:
                            score =  self.evidence['evidence']['gene2variant']['resource_score']['value']
                        except KeyError:
                            self.logger.error("Cannot score gentics portal evidence: variant: {}, study: {}".format(
                                self.evidence['variant']['id'], self.evidence['unique_association_fields']['study']))
                            raise

                    # Scoring other genetics evidences:
                    else:
                        g2v_score = self.evidence['evidence']['gene2variant']['resource_score']['value']

                        if self.evidence['evidence']['variant2disease']['resource_score']['type'] == 'pvalue':
                            v2d_score = self._get_score_from_pvalue_linear(
                                self.evidence['evidence']['variant2disease']['resource_score']['value'])
                        elif self.evidence['evidence']['variant2disease']['resource_score']['type'] == 'probability':
                            v2d_score = self.evidence['evidence']['variant2disease']['resource_score']['value']
                        else:
                            # this should not happen?
                            v2d_score = 0.

                        # GWAS Catalog is still a supported evidence source:
                        if self.evidence['sourceID'] == 'gwas_catalog':
                            sample_size = self.evidence['evidence']['variant2disease']['gwas_sample_size']
                            p_value = self.evidence['evidence']['variant2disease']['resource_score']['value']

                            # this is something to take into account for postgap data when I refactor this
                            r2_value = float(1)
                            if 'r2' in self.evidence['unique_association_fields']:
                                r2_value = float(self.evidence['unique_association_fields']['r2'])

                            score = self._score_gwascatalog(p_value, sample_size, g2v_score, r2_value)
                        else:
                            score = g2v_score * v2d_score

                else:
                    if self.evidence['evidence']['resource_score']['type'] == 'probability':
                        score = self.evidence['evidence']['resource_score']['value']
                    elif self.evidence['evidence']['resource_score']['type'] == 'pvalue':
                        score = self._get_score_from_pvalue_linear(self.evidence['evidence']['resource_score']['value'])
                self.evidence['scores']['association_score'] = score

            elif self.evidence['type'] == 'animal_model':
                self.evidence['scores']['association_score'] = float(
                    self.evidence['evidence']['disease_model_association']['resource_score']['value'])
            elif self.evidence['type'] == 'somatic_mutation':
                # If score is a probability use it directly, if it is a p-value (IntOGen since Nov 2019) linearise it
                if self.evidence['evidence']['resource_score']['type']== 'pvalue':
                    self.evidence['scores']['association_score'] = self._get_score_from_pvalue_linear(float(self.evidence['evidence']['resource_score']['value']),
                                                               range_min=0.1, out_range_min=0.25)
                else:
                    self.evidence['scores']['association_score'] = float(self.evidence['evidence']['resource_score']['value'])
            elif self.evidence['type'] == 'literature':
                score = float(self.evidence['evidence']['resource_score']['value'])
                if self.evidence['sourceID'] == 'europepmc':
                    score = score / 100.
                    if score > 1:
                        score = 1.
                self.evidence['scores']['association_score'] = score
            elif self.evidence['type'] == 'affected_pathway':
                # TODO: Implement two types of scoring for sysbio - based on p-value range & based on rank-based score range
                if self.evidence['sourceID'] == 'sysbio':
                    score = float(self.evidence['evidence']['resource_score']['value'])
                elif self.evidence['evidence']['resource_score']['type']== 'pvalue':
                    score = self._get_score_from_pvalue_linear(float(self.evidence['evidence']['resource_score']['value']),
                                                               range_min=1e-4, range_max=1e-14,
                                                               out_range_min=0.5, out_range_max=1.0)
                else:
                    score = float(
                        self.evidence['evidence']['resource_score']['value'])
                self.evidence['scores']['association_score'] = score

        except Exception as e:
            self.logger.error(
                "Cannot score evidence %s of type %s. Error: %s" % (self.evidence['id'], self.evidence['type'], e))

    @staticmethod
    def _get_score_from_pvalue_linear(pvalue, range_min=1, range_max=1e-10, out_range_min=0., out_range_max=1.):
        """rescale transformed p-values from [range_min, range_max] to [out_range_min, out_range_max]"""
        def get_log(n):
            try:
                return math.log10(n)
            except ValueError:
                return math.log10(range_max)

        min_score = get_log(range_min)
        max_score = get_log(range_max)
        score = get_log(pvalue)
        return DataNormaliser.renormalize(score, [min_score, max_score], [out_range_min, out_range_max])

    def _score_gwascatalog(self, pvalue, sample_size, g2v_value, r2_value):

        normalised_pvalue = self._get_score_from_pvalue_linear(pvalue, range_min=1, range_max=1e-15)

        normalised_sample_size = DataNormaliser.renormalize(sample_size, [0, 5000], [0, 1])

        score = normalised_pvalue * normalised_sample_size * g2v_value * r2_value

        return score

    def _score_phewas_data(self, source, pvalue, no_of_cases):
        if source == 'phewas_catalog':
            max_cases = 8800
            range_min = 0.05
            range_max = 1e-25
        elif source == 'twentythreeandme':
            max_cases = 297901
            range_min = 0.05
            range_max = 1e-30
        normalised_pvalue = self._get_score_from_pvalue_linear(float(pvalue), range_min, range_max)
        normalised_no_of_cases = DataNormaliser.renormalize(no_of_cases, [0, max_cases], [0, 1])
        score = normalised_pvalue * normalised_no_of_cases
        return score

