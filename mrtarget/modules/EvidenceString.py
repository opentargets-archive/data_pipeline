import copy
import json
import logging
import math
import os
from collections import Counter
import addict

import pickle
from tqdm import tqdm

from mrtarget.Settings import Config, file_or_resource
from mrtarget.common import Actions
from mrtarget.common import TqdmToLogger
from mrtarget.common.DataStructure import JSONSerializable, PipelineEncoder
from mrtarget.common.ElasticsearchLoader import Loader, LoaderWorker
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.common.LookupHelpers import LookUpDataRetriever, LookUpDataType
from mrtarget.common.Redis import RedisQueue, RedisQueueStatusReporter, RedisQueueWorkerProcess
from mrtarget.common.connection import new_es_client
from mrtarget.modules import GeneData
from mrtarget.common.Scoring import HarmonicSumScorer
from mrtarget.modules.ECO import ECO
from mrtarget.modules.EFO import EFO, get_ontology_code_from_url
from mrtarget.modules.GeneData import Gene
# from mrtarget.modules.Literature import Publication, PublicationFetcher

logger = logging.getLogger(__name__)
tqdm_out = TqdmToLogger(logger, level=logging.INFO)


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


class EvidenceStringActions(Actions):
    PROCESS = 'process'
    UPLOAD = 'upload'


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
                normalized = (delta2 * (n - start_range[0]) / delta1) + new_range[0]
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


class ExtendedInfo():
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
        therapeutic_area_codes = set()
        therapeutic_area_labels = set()
        for i, path_codes in enumerate(efo.path_codes):
            if len(path_codes) > 1:
                therapeutic_area_codes.add(path_codes[0])
                therapeutic_area_labels.add(efo.path_labels[i][0])
        self.data = dict(efo_id=efo.get_id(),
                         label=efo.label,
                         path=efo.path_codes,
                         therapeutic_area=dict(codes=list(therapeutic_area_codes),
                                               labels=list(therapeutic_area_labels)))


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


# class ExtendedInfoLiterature(ExtendedInfo):
#     root = "literature"

#     def __init__(self, literature):
#         if isinstance(literature, Publication):
#             self.extract_info(literature)
#         else:
#             raise AttributeError("you need to pass a Publication not a: " + str(type(literature)))

#     def extract_info(self, literature):

#         self.data = dict(abstract=literature.abstract,
#                          journal=literature.journal,
#                          title=literature.title,
#                          authors=literature.authors,
#                          doi=literature.doi,
#                          pub_type=literature.pub_type,
#                          mesh_headings=literature.mesh_headings,
#                          keywords=literature.keywords,
#                          chemicals=literature.chemicals,
#                          noun_chunks=literature.text_mined_entities['nlp'].get('chunks'),
#                          top_chunks=literature.text_mined_entities['nlp'].get('top_chunks'),
#                          date=literature.pub_date,
#                          journal_reference=literature.journal_reference)


class EvidenceManager():
    def __init__(self, lookup_data):
        self.logger = logging.getLogger(__name__)
        self.available_genes = lookup_data.available_genes
        self.available_efos = lookup_data.available_efos
        self.available_ecos = lookup_data.available_ecos
        self.uni2ens = lookup_data.uni2ens
        self.non_reference_genes = lookup_data.non_reference_genes
        self._get_eco_scoring_values()
        # self.logger.debug("finished self._get_eco_scoring_values(), took %ss"%str(time.time()-start_time))
        self.uni_header = GeneData.UNI_ID_ORG_PREFIX
        self.ens_header = GeneData.ENS_ID_ORG_PREFIX
        # self.gene_retriever = GeneLookUpTable(self.es)
        # self.efo_retriever = EFOLookUpTable(self.es)
        # self.eco_retriever = ECOLookUpTable(self.es)
        self._get_score_modifiers()
        self.available_publications = {}
        if 'available_publications' in lookup_data.__dict__:
            self.available_publications = lookup_data.available_publications


            # self.logger.debug("finished self._get_score_modifiers(), took %ss"%str(time.time()-start_time))

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
        '''split EVA in two datasources depending on the datatype'''
        if (evidence['sourceID'] == 'eva') and \
                (evidence['type'] == 'somatic_mutation'):
            evidence['sourceID'] = 'eva_somatic'
            fixed = True
        '''move genetic_literature to genetic_association'''
        if evidence['type'] == 'genetic_literature':
            evidence['type'] = 'genetic_association'

        if 'provenance_type' in evidence and \
                        'database' in evidence['provenance_type'] and \
                        'version' in evidence['provenance_type']['database']:
            evidence['provenance_type']['database']['version'] = str(evidence['provenance_type']['database']['version'])

        '''enforce eco-based score for genetic_association evidencestrings'''
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
                if evidence['sourceID'] not in ['uniprot_literature', 'gene2phenotype']:
                    self.logger.warning("Cannot find a score for eco code %s in evidence id %s" % (eco_uri, evidence['id']))

        # '''use just one mutation per somatic data'''
        # if 'known_mutations' in evidence['evidence'] and evidence['evidence']['known_mutations']:
        #     if len(evidence['evidence']['known_mutations']) == 1:
        #         evidence['evidence']['known_mutations'] = evidence['evidence']['known_mutations'][0]
        #     else:
        #         raise AttributeError('only one mutation is allowed. %i submitted for evidence id %s' % (
        #         len(evidence['evidence']['known_mutations']),
        #         evidence['id']))

        '''remove identifiers.org from genes and map to ensembl ids'''
        EvidenceManager.fix_target_id(evidence,
                                      self.uni2ens,
                                      self.available_genes,
                                      self.non_reference_genes
                                      )


        '''remove identifiers.org from cttv activity  and target type ids'''
        if 'target_type' in evidence['target']:
            evidence['target']['target_type'] = evidence['target']['target_type'].split('/')[-1]
        if 'activity' in evidence['target']:
            evidence['target']['activity'] = evidence['target']['activity'].split('/')[-1]

        '''remove identifiers.org from efos'''
        EvidenceManager.fix_disease_id(evidence)

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

        return Evidence(evidence), fixed

    @staticmethod
    def normalise_target_id(evidence, uni2ens, available_genes,non_reference_genes ):

        target_id = evidence['target']['id']
        new_target_id = None
        id_not_in_ensembl = False
        try:
            if target_id.startswith(GeneData.UNI_ID_ORG_PREFIX):
                if '-' in target_id:
                    target_id = target_id.split('-')[0]
                uniprotid = target_id.split(GeneData.UNI_ID_ORG_PREFIX)[1].strip()
                ensemblid = uni2ens[uniprotid]
                new_target_id = EvidenceManager.get_reference_ensembl_id(ensemblid,
                                                                         available_genes=available_genes,
                                                                         non_reference_genes=non_reference_genes)
            elif target_id.startswith(GeneData.ENS_ID_ORG_PREFIX):
                ensemblid = target_id.split(GeneData.ENS_ID_ORG_PREFIX)[1].strip()
                new_target_id = EvidenceManager.get_reference_ensembl_id(ensemblid,
                                                                         available_genes=available_genes,
                                                                         non_reference_genes=non_reference_genes)
            else:
                logger.warning("could not recognize target.id: %s | not added" % target_id)
                id_not_in_ensembl = True
        except KeyError:
            logger.error("cannot find an ensembl ID for: %s" % target_id)
            id_not_in_ensembl = True

        return new_target_id, id_not_in_ensembl

    def inject_loci(self, ev):
        gene_id = ev.evidence['target']['id']
        loc = addict.Dict()

        if gene_id in self.available_genes:
            # setting gene loci info
            gene_obj = self.available_genes[gene_id]
            chr = gene_obj['chromosome']
            pos_begin = gene_obj['gene_start']
            pos_end = gene_obj['gene_end']

            loc[chr].gene_begin = pos_begin
            loc[chr].gene_end = pos_end

            # setting variant loci info if any
            # only snps are supported at the moment
            if 'variant' in ev.evidence and \
                    'chrom' in ev.evidence['variant'] and \
                    'pos' in ev.evidence['variant']:
                vchr = ev.evidence['variant']['chrom']

                vpos_begin = ev.evidence['variant']['pos']
                vpos_end = ev.evidence['variant']['pos']

                loc[vchr].variant_begin = vpos_begin
                loc[vchr].variant_end = vpos_end

            # setting all loci into the evidence
            ev.evidence['loci'] = loc.to_dict()

        else:
            self.logger.error('inject_loci cannot find gene id %s', gene_id)


    @staticmethod
    def fix_target_id(evidence,uni2ens, available_genes, non_reference_genes, logger=logging.getLogger(__name__)) :
        target_id = evidence['target']['id']

        try:
            new_target_id, id_not_in_ensembl = EvidenceManager.normalise_target_id(evidence,
                                                                                   uni2ens,
                                                                                   available_genes,
                                                                                   non_reference_genes)
        except KeyError:
            logger.error("cannot find an ensembl ID for: %s" % target_id)
            id_not_in_ensembl = True
            new_target_id = target_id

        if id_not_in_ensembl:
            logger.warning("cannot find any ensembl ID for evidence for: %s. Offending target.id: %s",
                            evidence['target']['id'], target_id)

        evidence['target']['id'] = new_target_id

    @staticmethod
    def fix_disease_id(evidence, logger=logging.getLogger(__name__)):
        disease_id = evidence['disease']['id']
        new_disease_id = get_ontology_code_from_url(disease_id)
        if len(new_disease_id.split('_')) != 2:
            logger.warning("could not recognize disease.id: %s | added anyway" % disease_id)
        evidence['disease']['id'] = new_disease_id
        if not new_disease_id:
            logger.warning("No valid disease.id could be found in evidence: %s. Offending disease.id: %s" % (
                evidence['id'], disease_id))

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
        # for eco_id in ev['evidence']['evidence_codes']:
        #     if eco_id not in self.available_ecos:
        #         self.logger.error(
        #             "%s Evidence %s has an invalid eco id in evidence.evidence_codes: %s" % (
        #             datasource, evidence_id, eco_id))
        #         return False

        return True

    def get_extended_evidence(self, evidence):

        extended_evidence = copy.copy(evidence.evidence)
        extended_evidence['private'] = dict()

        """get generic gene info"""
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
        if 'reactome' in gene._private['facets']:
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

        """get generic efo info"""
        all_efo_codes = []
        efo_info = []
        diseaseid = extended_evidence['disease']['id']
        # try:
        efo = self._get_efo_obj(diseaseid)
        efo_info = ExtendedInfoEFO(efo)
        # except Exception:
        #     self.logger.warning("Cannot get generic info for efo: %s" % aboutid)
        if efo_info:
            for path in efo_info.data['path']:
                all_efo_codes.extend(path)
            extended_evidence["disease"][ExtendedInfoEFO.root] = efo_info.data
        all_efo_codes = list(set(all_efo_codes))

        """get generic eco info"""
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
                    self.logger.warning("Cannot get generic info for eco: %s" % eco_id)

            if ecos_info:
                data = []
                for eco_info in ecos_info:
                    data.append(eco_info.data)
                extended_evidence['evidence'][ExtendedInfoECO.root] = data
        except Exception as e:
            extended_evidence['evidence'][ExtendedInfoECO.root] = None
            all_eco_codes = []
            self.logger.exception("Cannot get generic info for eco: %s:"%str(e))

        '''Add private objects used just for faceting'''

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

        # ''' Add literature data '''
        # if inject_literature:
        #     if 'literature' in extended_evidence and \
        #                     'references' in extended_evidence['literature']:
        #         try:
        #             pmid_url = extended_evidence['literature']['references'][0]['lit_id']
        #             pmid = pmid_url.split('/')[-1]
        #             pub = None
        #             if pmid in self.available_publications:
        #                 pub = self.available_publications[pmid]
        #             else:
        #                 try:
        #                     pub_dict = pub_fetcher.get_publications(pmid)
        #                     if pub_dict:
        #                         pub = pub_dict[pmid]
        #                 except KeyError as e:
        #                     self.logger.warning('Cannot find publication %s in elasticsearch. Not injecting data' % pmid)

        #             if pub is not None:
        #                 literature_info = ExtendedInfoLiterature(pub)
        #                 extended_evidence['literature']['date'] = literature_info.data['date']
        #                 extended_evidence['literature']['abstract'] = literature_info.data['abstract']
        #                 extended_evidence['literature']['journal_data'] = literature_info.data['journal']
        #                 extended_evidence['literature']['title'] = literature_info.data['title']
        #                 journal_reference = ''
        #                 if 'journal_reference' in literature_info.data and  literature_info.data['journal_reference']:
        #                     if 'volume' in literature_info.data['journal_reference']:
        #                         journal_reference += literature_info.data['journal_reference']['volume']
        #                     if 'issue' in literature_info.data['journal_reference']:
        #                         journal_reference += "(%s)" % literature_info.data['journal_reference']['issue']
        #                     if  'pgn' in literature_info.data['journal_reference']:
        #                         journal_reference += ":%s" % literature_info.data['journal_reference']['pgn']
        #                 extended_evidence['literature']['journal_reference'] = journal_reference
        #                 extended_evidence['literature']['authors'] = literature_info.data['authors']
        #                 extended_evidence['private']['facets']['literature'] = {}
        #                 extended_evidence['literature']['doi'] = literature_info.data.get('doi')
        #                 extended_evidence['literature']['pub_type'] = literature_info.data.get('pub_type')
        #                 extended_evidence['private']['facets']['literature'][
        #                     'mesh_headings'] = literature_info.data.get(
        #                     'mesh_headings')
        #                 extended_evidence['private']['facets']['literature']['chemicals'] = literature_info.data.get(
        #                     'chemicals')
        #                 extended_evidence['private']['facets']['literature']['noun_chunks'] = literature_info.data.get(
        #                     'noun_chunks')
        #                 extended_evidence['private']['facets']['literature']['top_chunks'] = literature_info.data.get(
        #                     'top_chunks')
        #                 extended_evidence['private']['facets']['literature']['keywords'] = literature_info.data.get(
        #                     'keywords')

        #         except Exception:
        #             self.logger.exception(
        #                 'Error in publication data injection - skipped for evidence id: ' + extended_evidence['id'])

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
        try:
            eco = ECO(ecoid)
            eco.load_json(self.available_ecos[ecoid])
            return eco
        except KeyError:
            self.logger.debug('data for ECO code %s could not be injected'%ecoid)
            return

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
        for symbol, data in non_reference_genes.items():
            if ensg in data['alternative']:
                logger.warning(
                    "Mapped non reference ensembl gene id %s to %s for gene %s" % (ensg, data['reference'], symbol))
                return data['reference']
    @staticmethod
    def get_reference_ensembl_id(ensemblid, available_genes, non_reference_genes):
        if ensemblid not in available_genes:
            ensemblid = EvidenceManager._map_to_reference_ensembl_gene(ensemblid, non_reference_genes) or ensemblid
        return ensemblid

    def _get_eco_scoring_values(self):
        self.eco_scores = dict()
        for line in file(file_or_resource('eco_scores.tsv')):
            try:
                uri, label, score = line.strip().split('\t')
                uri.rstrip()
                self.eco_scores[uri] = float(score)
            except:
                self.logger.error("cannot parse line in eco_scores.tsv: %s" % (line.strip()))

    def _get_score_modifiers(self):
        self.score_modifiers = {}
        for datasource, values in Config.DATASOURCE_EVIDENCE_SCORE_AUTO_EXTEND_RANGE.items():
            self.score_modifiers[datasource] = DataNormaliser(values['min'], values['max'])


class Evidence(JSONSerializable):
    def __init__(self, evidence, datasource=""):
        if isinstance(evidence, str) or isinstance(evidence, unicode):
            self.load_json(evidence)
        elif isinstance(evidence, dict):
            self.evidence = evidence
        else:
            raise AttributeError(
                "the evidence should be a dict or a json string to parse, not a " + str(type(evidence)))
        self.datasource = self.evidence['sourceID'] or datasource
        self._set_datatype()

    def _set_datatype(self, ):

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
        return Config.ELASTICSEARCH_DATA_DOC_NAME + '-' + self.database

    def get_id(self):
        return self.evidence['id']

    def to_json(self):
        self.stamp_data_release()
        return json.dumps(self.evidence,
                          sort_keys=True,
                          # indent=4,
                          cls=PipelineEncoder)

        return

    def score_to_json(self):
        score = {}
        score['id'] = self.evidence['id']
        score['sourceID'] = self.evidence['sourceID']
        score['type'] = self.evidence['type']
        score['target'] = {"id": self.evidence['target']['id'],
                           "gene_info": self.evidence['target']['gene_info']}

        score['disease'] = {"id": self.evidence['disease']['id'],
                            "efo_info": self.evidence['disease']['efo_info']}
        score['scores'] = self.evidence['scores']
        score['private'] = {"efo_codes": self.evidence['private']['efo_codes']}
        return json.dumps(score)

    def load_json(self, data):
        self.evidence = json.loads(data)

    def score_evidence(self, modifiers={}, global_stats = None):
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

                    if self.evidence['sourceID'] in ['phewas_catalog','twentythreeandme']:
                        no_of_cases = self.evidence['unique_association_fields']['cases']
                        score = self._score_phewas_data(self.evidence['sourceID'],
                                                        self.evidence['evidence']['variant2disease']['resource_score'][
                                                            'value'],
                                                        no_of_cases)
                    else:
                        g2v_score = self.evidence['evidence']['gene2variant']['resource_score']['value']

                        if self.evidence['evidence']['variant2disease']['resource_score']['type'] == 'pvalue':
                            v2d_score = self._get_score_from_pvalue_linear(
                                self.evidence['evidence']['variant2disease']['resource_score']['value'])
                        elif self.evidence['evidence']['variant2disease']['resource_score']['type'] == 'probability':
                            v2d_score = self.evidence['evidence']['variant2disease']['resource_score']['value']
                        else:
                            '''this should not happen?'''
                            v2d_score = 0.

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
                frequency = 1.
                if 'known_mutations' in self.evidence['evidence'] and self.evidence['evidence']['known_mutations']:
                    sample_total_coverage = 1.
                    max_sample_size = 1.
                    for mutation in self.evidence['evidence']['known_mutations']:
                        if 'number_samples_with_mutation_type' in mutation:
                            sample_total_coverage += int(mutation['number_samples_with_mutation_type'])
                            if int(mutation['number_mutated_samples']) > max_sample_size:
                                max_sample_size = int(mutation['number_mutated_samples'])
                    if sample_total_coverage > max_sample_size:
                        sample_total_coverage = max_sample_size
                    frequency = DataNormaliser.renormalize(sample_total_coverage / max_sample_size, [0., 9.], [.5, 1.])
                self.evidence['scores']['association_score'] = float(
                    self.evidence['evidence']['resource_score']['value']) * frequency
            elif self.evidence['type'] == 'literature':
                score = float(self.evidence['evidence']['resource_score']['value'])
                if self.evidence['sourceID'] == 'europepmc':
                    score = score / 100.
                    if score > 1:
                        score = 1.
                self.evidence['scores']['association_score'] = score
            elif self.evidence['type'] == 'affected_pathway':
                if self.evidence['evidence']['resource_score']['type']== 'pvalue':
                    score = self._get_score_from_pvalue_linear(float(self.evidence['evidence']['resource_score']['value']),
                                                               range_min=1e-4,
                                                               range_max=1e-14)
                else:
                    score = float(
                        self.evidence['evidence']['resource_score']['value'])
                self.evidence['scores']['association_score'] = score

        except Exception as e:
            self.logger.error(
                "Cannot score evidence %s of type %s. Error: %s" % (self.evidence['id'], self.evidence['type'], e))

        '''check for minimum score '''
        if self.evidence['scores']['association_score'] < Config.SCORING_MIN_VALUE_FILTER[self.evidence['sourceID']]:
            raise AttributeError(
                "Evidence String Rejected since score is too low: %s. score: %f, min score: %f" % (self.get_id(),
                                                                                                   self.evidence[
                                                                                                       'scores'][
                                                                                                       'association_score'],
                                                                                                   Config.SCORING_MIN_VALUE_FILTER[
                                                                                                       self.evidence[
                                                                                                           'sourceID']]
                                                                                                   ))

        # scale score according to global stats IFF sourceID is included
        # in sources to apply from Config class in Settings.py
        if (global_stats is not None) and \
                (self.evidence['sourceID'] in Config.GLOBAL_STATS_SOURCES_TO_APPLY):
            evidence_pmids = EvidenceGlobalCounter.get_literature(self.evidence)

            experiment_id = EvidenceGlobalCounter.get_experiment(self.evidence)

            global_counts = [1]
            for pmid in evidence_pmids:
                global_counts.append(max(global_stats.get_target_and_disease_uniques_for_literature(pmid)))
            if experiment_id is not None:
                global_counts.append(max(global_stats.get_target_and_disease_uniques_for_experiment(experiment_id)))
            max_count = max(global_counts)
            if max_count >1:
                modifier = HarmonicSumScorer.sigmoid_scaling(max_count)
                self.evidence['scores']['association_score'] *= modifier

        '''modify scores accodigng to weights'''
        datasource_weight = Config.DATASOURCE_EVIDENCE_SCORE_WEIGHT.get(self.evidence['sourceID'], 1.)
        if datasource_weight != 1:
            weighted_score = self.evidence['scores']['association_score'] * datasource_weight
            if weighted_score > 1:
                weighted_score = 1.
            self.evidence['scores']['association_score'] = weighted_score

        '''apply rescaling to scores'''
        if self.evidence['sourceID'] in modifiers:
            self.evidence['scores']['association_score'] = modifiers[self.evidence['sourceID']](
                self.evidence['scores']['association_score'])

    @staticmethod
    def _get_score_from_pvalue_linear(pvalue, range_min=1, range_max=1e-10):
        def get_log(n):
            try:
                return math.log10(n)
            except ValueError:
                return math.log10(range_max)

        min_score = get_log(range_min)
        max_score = get_log(range_max)
        score = get_log(pvalue)
        return DataNormaliser.renormalize(score, [min_score, max_score], [0., 1.])

    def _score_gwascatalog(self, pvalue, sample_size, g2v_value, r2_value):

        normalised_pvalue = self._get_score_from_pvalue_linear(pvalue, range_min=1, range_max=1e-15)

        normalised_sample_size = DataNormaliser.renormalize(sample_size, [0, 5000], [0, 1])

        score = normalised_pvalue * normalised_sample_size * g2v_value * r2_value

        # self.logger.debug("gwas score: %f | pvalue %f %f | sample size%f %f |severity %f" % (score, pvalue,
        # normalised_pvalue, sample_size,normalised_sample_size, severity))
        return score

    # def _score_postgap(self):
    #     """Calculate the variant-to-gene score for a row.
    #
    #     Arguments:
    #     r       -- A row from a pandas DataFrame of the POSTGAP data.
    #     vep_map -- A dict of numeric values associated with VEP terms
    #
    #     Returns:
    #     v2g     -- The variant-to-gene score.
    #     """
    #     GTEX_CUTOFF = 0.999975
    #     # K = 0.1 / 0.35
    #     # VEP_THRESHOLD = 0.65
    #
    #     # stage 1 cannot being implemented on the evs because evs doesn't contain vep defs
    #     # if not pd.isnull(r.vep_terms):
    #     #     vep_terms = r.vep_terms.split(',')
    #     #     vep_score = max([
    #     #         0 if t == 'start_retained_variant' else vep_map[t]  # start_retained_variant needs adding to map
    #     #         for t in vep_terms
    #     #     ])
    #     #     if vep_score >= VEP_THRESHOLD:
    #     #         return (K * (vep_score - VEP_THRESHOLD)) + 0.9
    #
    #     # stage 2
    #     if (r.GTEx > GTEX_CUTOFF) or (r.PCHiC > 0) or (r.DHS > 0) or (r.Fantom5 > 0):
    #         gtex = 1 if (r.GTEx > GTEX_CUTOFF) else 0
    #         pchic = 1 if (r.PCHiC > 0) else 0
    #         dhs = 1 if (r.DHS > 0) else 0
    #         fantom5 = 1 if (r.Fantom5 > 0) else 0
    #         vep_score = ((gtex * 13) + (fantom5 * 3) + (dhs * 1.5) + (pchic * 1.5)) / 19
    #         return vep_score * 0.4 + 0.5
    #
    #     # stage 3
    #     if (r.Nearest > 0):
    #         return 0.5

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


class EvidenceProcesser(RedisQueueWorkerProcess):
    def __init__(self,
                 score_q,
                 r_path,
                 loader_q,
                 chunk_size=1e4,
                 dry_run=False,
                 lookup_data=None,
                #  inject_literature=False,
                 global_stats = None,
                 ):
        super(EvidenceProcesser, self).__init__(score_q, r_path, loader_q, ignore_errors=[AttributeError])
        self.q = score_q
        self.chunk_size = chunk_size
        self.dry_run = dry_run
        self.es = None
        self.loader = None
        self.lookup_data = lookup_data
        # self.evidence_manager = EvidenceManager(lookup_data)
        self.evidence_manager = None
        # self.inject_literature = inject_literature
        # self.pub_fetcher = None
        self.global_stats = global_stats

    def init(self):
        super(EvidenceProcesser, self).init()
        self.logger = logging.getLogger(__name__)
        self.lookup_data.set_r_server(self.get_r_server())
        # self.pub_fetcher = PublicationFetcher(new_es_client(hosts=Config.ELASTICSEARCH_NODES_PUB))

        # moved from __init__ as this is executed on a process so it should need be process mem
        self.evidence_manager = EvidenceManager(self.lookup_data)

    def process(self, data):
        idev, ev_raw = data
        fixed_ev, fixed = self.evidence_manager.fix_evidence(ev_raw)
        if self.evidence_manager.is_valid(fixed_ev, datasource=fixed_ev.datasource):
            '''add scoring to evidence string'''
            fixed_ev.score_evidence(self.evidence_manager.score_modifiers,
                                    self.global_stats)
            '''extend data in evidencestring'''

            ev = self.evidence_manager.get_extended_evidence(fixed_ev)
        else:
            raise AttributeError("Invalid %s Evidence String" % (fixed_ev.datasource))

        self.evidence_manager.inject_loci(ev)
        loader_args = (
            Config.ELASTICSEARCH_DATA_INDEX_NAME + '-' + Config.DATASOURCE_TO_INDEX_KEY_MAPPING[ev.database],
            ev.get_doc_name(),
            idev,
            ev.to_json(),
        )
        # remove routing doesnt make sense with one node
        # loader_kwargs = dict(create_index=False,
        #                      routing=ev.evidence['target']['id'])

        loader_kwargs = {"create_index": False}
        return loader_args, loader_kwargs


class EvidenceGlobalCounter():
    '''stores aggregated stats about evidence properties used in the computation of the score'''
    GLOBAL_COUNTERS = ['target',
                       'disease',
                       ]

    def __init__(self):
        self.total = self._init_counter()
        self.experiment = {}
        self._all_experiment_ids = set()
        self.literature = {}
        self._all_literature_ids = set()


    def _init_counter(self):
        d = {'total':0}
        for i in self.GLOBAL_COUNTERS:
            d[i]=Counter()
        return d

    def digest(self, ev):
        '''takes an evidence in dict format and populate the appropiate counters'''
        self._inject_counts(self.total, ev)
        for lit in self.get_literature(ev):
            if lit not in self.literature:
                self.literature[lit]= self._init_counter()
            self._inject_counts(self.literature[lit], ev)
        exp = self.get_experiment(ev)
        if exp:
            if exp not in self.experiment:
                self.experiment[exp] = self._init_counter()
            self._inject_counts(self.experiment[exp], ev)

    def get_target_and_disease_uniques_for_literature(self, lit_id):
        '''

        :param lit_id: literature id
        :return: tuple of target and disease unique ids linked to the literature id
        '''
        try:
            literature_data = self.literature[lit_id]
        except KeyError as e:
            if lit_id in self._all_literature_ids:
                return (1,1)
            else:
                return (0,0)
        return len(literature_data['target']), len(literature_data['disease'])

    def get_target_and_disease_uniques_for_experiment(self, exp_id):
        '''

        :param exp_id: experiment id
        :return: tuple of target and disease unique ids linked to the experiment id
        '''
        try:
            experiment_data = self.experiment[exp_id]
        except KeyError as e:
            if exp_id in self._all_experiment_ids:
                return (1,1)
            else:
                return (0,0)
        return len(experiment_data['target']), len(experiment_data['disease'])

    def compress(self):
        '''removes all the entries with a single occurrence and assume the counts are 1 when a query raise a keyerror'''
        self._all_literature_ids = set(self.literature.keys())
        for lit_id in self._all_literature_ids:
            if self.literature[lit_id]['total'] == 1:
                del self.literature[lit_id]

        self._all_experiment_ids = set(self.experiment.keys())
        for exp_id in self._all_experiment_ids:
            if self.experiment[exp_id]['total'] == 1:
                del self.experiment[exp_id]


    @staticmethod
    def _inject_counts(target, ev):
        target['target'][EvidenceGlobalCounter.get_target(ev)] += 1
        target['disease'][EvidenceGlobalCounter.get_disease(ev)] += 1
        target['total']+= 1


    @staticmethod
    def get_target(ev):
        return ev['target']['id']

    @staticmethod
    def get_disease(ev):
        return ev['disease']['id']

    @staticmethod
    def get_literature(ev):
        try:
            return [i['lit_id'].split('/')[-1] for i in ev['literature']['references']]
        except KeyError as e:
            return []
    @staticmethod
    def get_experiment(ev):
        try:
            return ev['evidence']['unique_experiment_reference']
        except KeyError as e:
            pass

class EvidenceStringProcess():
    def __init__(self,
                 es,
                 r_server):
        self.loaded_entries_to_pg = 0
        self.es = es
        self.es_query = ESQuery(es)
        self.r_server = r_server
        # self.es_pub = es_pub
        self.logger = logging.getLogger(__name__)

    def process_all(self, datasources=[], dry_run=False):
        return self._process_evidence_string_data(datasources=datasources,
                                                  dry_run=dry_run)

    def _process_evidence_string_data(self,
                                      datasources=[],
                                      dry_run=False):

        self.logger.debug("Starting Evidence Manager")
        '''get lookup data and stats'''
        lookup_data_types = [LookUpDataType.TARGET, LookUpDataType.DISEASE, LookUpDataType.ECO]
        # if inject_literature:
        #     lookup_data_types = [LookUpDataType.PUBLICATION, LookUpDataType.TARGET, LookUpDataType.DISEASE,
        #                          LookUpDataType.ECO]
        #     # lookup_data_types.append(LookUpDataType.PUBLICATION)

        lookup_data = LookUpDataRetriever(self.es,
                                          self.r_server,
                                          data_types=lookup_data_types,
                                          autoload=True,
                                        #   es_pub=self.es_pub,
                                          ).lookup

        global_stat_cache= 'global_stats.pkl'
        if os.path.exists(global_stat_cache):
            global_stats = pickle.load(open(global_stat_cache))
        else:
            global_stats = self.get_global_stats(lookup_data.uni2ens,
                                                 lookup_data.available_genes,
                                                 lookup_data.non_reference_genes)
            if self.logger.level == logging.DEBUG:
                pickle.dump(global_stats, open(global_stat_cache,'w'), protocol=pickle.HIGHEST_PROTOCOL)

        # lookup_data.available_genes.load_uniprot2ensembl()
        get_evidence_page_size = 5000
        '''prepare es indices'''
        loader = Loader(self.es)
        overwrite_indices = not dry_run
        if not dry_run:
            overwrite_indices = not bool(datasources)
        for k, v in Config.DATASOURCE_TO_INDEX_KEY_MAPPING:
            loader.create_new_index(Config.ELASTICSEARCH_DATA_INDEX_NAME + '-' + v, recreate=overwrite_indices)
            loader.prepare_for_bulk_indexing(loader.get_versioned_index(Config.ELASTICSEARCH_DATA_INDEX_NAME + '-' + v))
        loader.create_new_index(
            Config.ELASTICSEARCH_DATA_INDEX_NAME + '-' + Config.DATASOURCE_TO_INDEX_KEY_MAPPING['default'],
            recreate=overwrite_indices)
        loader.prepare_for_bulk_indexing(loader.get_versioned_index(Config.ELASTICSEARCH_DATA_INDEX_NAME + '-' +
                                                                    Config.DATASOURCE_TO_INDEX_KEY_MAPPING['default']))
        if datasources and overwrite_indices:
            self.self.logger.info('deleting data for datasources %s' % ','.join(datasources))
            self.es_query.delete_evidence_for_datasources(datasources)

        '''create queues'''
        self.logger.info('limiting the number or workers to a max of 16 or cpucount')
        number_of_workers = max(16, Config.WORKERS_NUMBER)
        # too many storers
        number_of_storers = min(16, number_of_workers / 2 + 1,)
        queue_per_worker = 250

        evidence_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|evidence_q',
                                max_size=queue_per_worker * number_of_storers,
                                job_timeout=1200,
                                batch_size=1,
                                r_server=self.r_server,
                                serialiser='pickle')
        store_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|store_evidence_q',
                             max_size=queue_per_worker * 5 * number_of_storers,
                             job_timeout=1200,
                             batch_size=10,
                             r_server=self.r_server,
                             serialiser='pickle')

        q_reporter = RedisQueueStatusReporter([evidence_q,
                                               store_q,
                                               ],
                                              interval=30,
                                              )
        q_reporter.start()

        self.logger.info('evidence processer process with %d processes', number_of_workers)
        self.logger.info('trying with less workers because global stats')
        scorers = [EvidenceProcesser(evidence_q,
                                    None,
                                    store_q,
                                    lookup_data=lookup_data,
                                    # inject_literature=inject_literature,
                                    global_stats=global_stats)
                                    for _ in range(number_of_workers)]
        for w in scorers:
            w.start()

        self.logger.info('loader worker process with %d processes', number_of_storers)
        loaders = [LoaderWorker(store_q,
                                None,
                                chunk_size=1000 / number_of_storers,
                                dry_run=dry_run
                                ) for _ in range(number_of_storers)]
        for w in loaders:
            w.start()

        targets_with_data = set()
        for row in tqdm(self.get_evidence(page_size=get_evidence_page_size, datasources=datasources),
                        desc='Reading available evidence_strings',
                        total=self.es_query.count_validated_evidence_strings(datasources=datasources),
                        unit=' evidence',
                        file=tqdm_out,
                        unit_scale=True):
            ev = Evidence(row['evidence_string'], datasource=row['data_source_name'])
            idev = row['uniq_assoc_fields_hashdig']
            ev.evidence['id'] = idev
            evidence_q.put((idev, ev))
            targets_with_data.add(ev.evidence['target']['id'][0])

        evidence_q.set_submission_finished()

        self.logger.info('collecting loaders')
        for w in loaders:
            w.join()

        self.logger.info('collecting scorers')
        for w in scorers:
            w.join()

        self.logger.info('collecting reporter')
        q_reporter.join()


        self.logger.info('flushing data to index')
        self.es.indices.flush('%s*' % Loader.get_versioned_index(Config.ELASTICSEARCH_DATA_INDEX_NAME),
                              wait_if_ongoing=True)

        self.logger.info('Processed data for %i targets' % len(targets_with_data))
        self.logger.info("DONE")

        return list(targets_with_data)



    def get_evidence(self, page_size=5000, datasources=[]):

        c = 0
        for row in self.es_query.get_validated_evidence_strings(size=page_size, datasources=datasources):
            c += 1
            if c % 1e5 == 0:
                self.logger.debug("loaded %i ev from db to process" % c)
            yield row
        self.logger.info("loaded %i ev from db to process" % c)

    def get_global_stats(self, uni2ens, available_genes, non_reference_genes, page_size=5000,):
        global_stats = EvidenceGlobalCounter()
        for row in tqdm(self.get_evidence(page_size),
                        desc='getting global stats on  available evidence_strings',
                        total=self.es_query.count_validated_evidence_strings(),
                        unit=' evidence',
                        file=tqdm_out,
                        unit_scale=True):
            ev = Evidence(row['evidence_string'], datasource=row['data_source_name']).evidence

            EvidenceManager.fix_target_id(ev, uni2ens, available_genes, non_reference_genes)
            EvidenceManager.fix_disease_id(ev)

            # only include into global stats computation the evidences coming from configuration
            # Config class in Settings.py
            if ev["sourceID"] in Config.GLOBAL_STATS_SOURCES_TO_INCLUDE:
                global_stats.digest(ev=ev)

        global_stats.compress()
        return global_stats
