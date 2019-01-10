import logging
import os
import time

import pickle
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.modules.ChEMBL import ChEMBLLookup
from mrtarget.common.LookupTables import ECOLookUpTable
from mrtarget.common.LookupTables import EFOLookUpTable
from mrtarget.common.LookupTables import HPALookUpTable
from mrtarget.common.LookupTables import GeneLookUpTable
from opentargets_ontologyutils.rdf_utils import OntologyClassReader
import opentargets_ontologyutils.hpo
import opentargets_ontologyutils.mp
from mrtarget.Settings import Config, file_or_resource
from mrtarget.common import require_all


class LookUpData():
    def __init__(self):
        self.logger = logging.getLogger(__name__)

        self.available_genes = None
        self.available_efos = None
        self.available_ecos = None
        self.available_hpa = None
        self.uni2ens = None
        self.non_reference_genes = None
        self.chembl = None

        self.hpo_ontology = None
        self.mp_ontology = None
        self.efo_ontology = None

    def set_r_server(self, r_server):
        self.logger.debug('setting r_server to all lookup tables from external r_server')
        if self.available_ecos:
            self.available_ecos.r_server = r_server
            self.available_ecos._table.set_r_server(r_server)
        if self.available_efos:
            self.available_efos.r_server = r_server
            self.available_efos._table.set_r_server(r_server)
        if self.available_hpa:
            self.available_hpa.r_server = r_server
            self.available_hpa._table.set_r_server(r_server)
        if self.available_genes:
            self.available_genes.r_server = r_server
            self.available_genes._table.set_r_server(r_server)


class LookUpDataType(object):
    TARGET = 'target'
    DISEASE = 'disease'
    EFO = 'efo'
    HPO = 'hpo'
    ECO = 'eco'
    MP = 'mp'
    CHEMBL_DRUGS = 'chembl_drugs'
    HPA = 'hpa'


class LookUpDataRetriever(object):
    def __init__(self,
                 es,
                 r_server,
                 targets,
                 data_types,
                 hpo_uri,
                 mp_uri
                 ):
        self.es = es
        self.r_server = r_server
        self.esquery = ESQuery(self.es)
        self.lookup = LookUpData()

        self._logger = logging.getLogger(__name__)

        # TODO: run the steps in parallel to speedup loading times
        for dt in data_types:
            self._logger.info("get %s info"%dt)
            start_time = time.time()
            if dt == LookUpDataType.TARGET:
                self._get_gene_info(targets, True)
            elif dt == LookUpDataType.DISEASE:
                self.lookup.available_efos = EFOLookUpTable(self.es, 'EFO_LOOKUP', self.r_server)
            elif dt == LookUpDataType.ECO:
                self.lookup.available_ecos = ECOLookUpTable(self.es, 'ECO_LOOKUP', self.r_server)
            elif dt == LookUpDataType.MP:
                self._logger.debug("get MP info")
                self._get_mp(mp_uri)
            elif dt == LookUpDataType.HPO:
                self._logger.debug("get HPO info")
                self._get_hpo(hpo_uri)
            elif dt == LookUpDataType.CHEMBL_DRUGS:
                self._get_available_chembl_mappings()
            elif dt == LookUpDataType.HPA:
                self.lookup.available_hpa = HPALookUpTable(self.es, 'HPA_LOOKUP', self.r_server)

            self._logger.info("loaded %s in %ss" % (dt, str(int(time.time() - start_time))))

    def set_r_server(self, r_server):
        self.r_server = r_server
        self.lookup.set_r_server(r_server)
        self.esquery = ESQuery()

    def _get_gene_info(self, targets=[], autoload = True):
        self._logger.info('getting gene info')
        self.lookup.available_genes = GeneLookUpTable(self.es,
                                                      'GENE_LOOKUP',
                                                      self.r_server,
                                                      targets = targets,
                                                      autoload = autoload)
        self.lookup.uni2ens = self.lookup.available_genes.uniprot2ensembl
        self._get_non_reference_gene_mappings()

    def _get_non_reference_gene_mappings(self):
        self.lookup.non_reference_genes = {}
        skip_header=True
        for line in file(file_or_resource('genes_with_non_reference_ensembl_ids.tsv')):
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

    def _get_hpo(self, hpo_uri):
        '''
        Load HPO to accept phenotype terms that are not in EFO
        :return:
        '''
        obj = OntologyClassReader()
        opentargets_ontologyutils.hpo.get_hpo(obj, hpo_uri)
        obj.rdf_graph = None
        self.lookup.hpo_ontology = obj

    def _get_mp(self, mp_uri):
        '''
        Load MP to accept phenotype terms that are not in EFO
        :return:
        '''
        obj = OntologyClassReader()
        opentargets_ontologyutils.mp.load_mammalian_phenotype_ontology(obj, mp_uri)
        obj.rdf_graph = None
        self.lookup.mp_ontology = obj

    def _get_available_chembl_mappings(self):
        chembl_handler = ChEMBLLookup()
        chembl_handler.get_molecules_from_evidence()
        all_molecules = set()
        for target, molecules in  chembl_handler.target2molecule.items():
            all_molecules = all_molecules|molecules
        all_molecules = list(all_molecules)
        query_batch_size = 100
        for i in range(0, len(all_molecules) + 1, query_batch_size):
            chembl_handler._populate_synonyms_for_molecule(all_molecules[i:i + query_batch_size],
                                                           chembl_handler.molecule2synonyms,
                                                           chembl_handler._logger)
        self.lookup.chembl = chembl_handler
        




