import logging
import os
import time

import pickle
from tqdm import tqdm
from mrtarget.common import TqdmToLogger
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.modules.ChEMBL import ChEMBLLookup
from mrtarget.common.LookupTables import ECOLookUpTable
from mrtarget.common.LookupTables import EFOLookUpTable
# from mrtarget.common.LookupTables import HPOLookUpTable
from mrtarget.common.LookupTables import MPLookUpTable
from mrtarget.common.LookupTables import HPALookUpTable
from mrtarget.common.LookupTables import GeneLookUpTable
from mrtarget.common.LookupTables import LiteratureLookUpTable
from mrtarget.modules.Ontology import OntologyClassReader
from mrtarget.Settings import Config, file_or_resource
from mrtarget.common import require_all


class LookUpData():
    def __init__(self):
        self.logger = logging.getLogger(__name__)

        self.available_genes = None
        self.available_efos = None
        self.available_ecos = None
        # self.available_hpos = None
        # self.available_mps = None
        self.available_hpa = None
        self.uni2ens = None
        self.non_reference_genes = None
        self.available_gene_objects = None
        self.available_efo_objects = None
        self.available_eco_objects = None
        self.chembl = None
        self.available_publications = None

    def set_r_server(self, r_server):
        self.logger.debug('setting r_server to all lookup tables from external r_server')
        if self.available_ecos:
            self.available_ecos.r_server = r_server
            self.available_ecos._table.set_r_server(r_server)
        if self.available_efos:
            self.available_efos.r_server = r_server
            self.available_efos._table.set_r_server(r_server)
        # if self.available_hpos:
        #     self.available_hpos.r_server = r_server
        #     self.available_hpos._table.set_r_server(r_server)
        # if self.available_mps:
        #     self.available_mps.r_server = r_server
        #     self.available_mps._table.set_r_server(r_server)
        if self.available_hpa:
            self.available_hpa.r_server = r_server
            self.available_hpa._table.set_r_server(r_server)
        if self.available_genes:
            self.available_genes.r_server = r_server
            self.available_genes._table.set_r_server(r_server)
        if self.available_publications:
            self.available_publications.r_server = r_server
            self.available_publications._table.set_r_server(r_server)


class LookUpDataType(object):
    TARGET = 'target'
    DISEASE = 'disease'
    EFO = 'efo'
    HPO = 'hpo'
    ECO = 'eco'
    PUBLICATION = 'publication'
    MP = 'mp'
    MP_LOOKUP = 'mp_lookup'
    CHEMBL_DRUGS = 'chembl_drugs'
    HPA = 'hpa'


class LookUpDataRetriever(object):
    def __init__(self,
                 es=None,
                 r_server=None,
                 targets=[],
                 data_types=(LookUpDataType.TARGET,
                             LookUpDataType.DISEASE,
                             LookUpDataType.ECO),
                 autoload=True,
                 es_pub=None,
                 ):

        self.es = es
        self.es_pub = es_pub
        self.r_server = r_server

        self.esquery = ESQuery(self.es)

        require_all(self.es is not None, self.r_server is not None)

        self.lookup = LookUpData()
        self._logger = logging.getLogger(__name__)
        tqdm_out = TqdmToLogger(self._logger, level=logging.INFO)

        # TODO: run the steps in parallel to speedup loading times
        for dt in data_types:
            self._logger.info("get %s info"%dt)
        for dt in tqdm(data_types,
                       desc='loading lookup data',
                       unit=' steps',
                       file=tqdm_out,
                       leave=False):
            start_time = time.time()
            if dt == LookUpDataType.TARGET:
                self._get_gene_info(targets, autoload=autoload)
            elif dt == LookUpDataType.DISEASE:
                self._get_available_efos()
            elif dt == LookUpDataType.ECO:
                self._get_available_ecos()
            elif dt == LookUpDataType.MP_LOOKUP:
                self._get_available_mps()
            elif dt == LookUpDataType.MP:
                self._logger.debug("get MP info")
                self._get_mp()
            elif dt == LookUpDataType.HPO:
                self._logger.debug("get HPO info")
                self._get_hpo()
            elif dt == LookUpDataType.EFO:
                self._logger.debug("get EFO info")
                self._get_efo()
            elif dt == LookUpDataType.PUBLICATION:
                self._get_available_publications()
            elif dt == LookUpDataType.CHEMBL_DRUGS:
                self._get_available_chembl_mappings()
            elif dt == LookUpDataType.HPA:
                self._get_available_hpa()

            self._logger.info("finished loading %s data into redis, took %ss" % (dt, str(time.time() - start_time)))

    def set_r_server(self, r_server):
        self.r_server = r_server
        self.lookup.set_r_server(r_server)
        self.esquery = ESQuery()

    def _get_available_efos(self):
        self._logger.info('getting efos')
        self.lookup.available_efos = EFOLookUpTable(self.es, 'EFO_LOOKUP', self.r_server)

    # def _get_available_hpos(self):
    #     self._logger.info('getting hpos')
    #     self.lookup.available_efos = HPOLookUpTable(self.es, 'HPO_LOOKUP', self.r_server)
    #

    def _get_available_mps(self, autoload=True):
         self._logger.info('getting mps info from ES')
         self.lookup.available_mps = MPLookUpTable(self.es, 'MP_LOOKUP',self.r_server)

    def _get_available_ecos(self):
        self._logger.info('getting ecos')
        self.lookup.available_ecos = ECOLookUpTable(self.es, 'ECO_LOOKUP', self.r_server)


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

    def _get_hpo(self):
        '''
        Load HPO to accept phenotype terms that are not in EFO
        :return:
        '''
        cache_file = 'processed_hpo_lookup'
        obj = self._get_from_pickled_file_cache(cache_file)
        if obj is None:
            obj = OntologyClassReader()
            obj.load_hpo_classes()
            obj.rdf_graph = None
            self._set_in_pickled_file_cache(obj, cache_file)
        self.lookup.hpo_ontology = obj

    def _get_mp(self):
        '''
        Load MP to accept phenotype terms that are not in EFO
        :return:
        '''
        cache_file = 'processed_mp_lookup'
        obj = None
        obj = self._get_from_pickled_file_cache(cache_file)
        if obj is None:
            obj = OntologyClassReader()
            obj.load_mp_classes()
            obj.rdf_graph = None
            self._set_in_pickled_file_cache(obj, cache_file)
        self.lookup.mp_ontology = obj



    def _get_efo(self):
        '''
        Load EFO current and obsolete classes to report them to data providers
        :return:
        '''
        cache_file = 'processed_efo_lookup'
        obj = self._get_from_pickled_file_cache(cache_file)
        if obj is None:
            obj = OntologyClassReader()
            obj.load_open_targets_disease_ontology()
            obj.rdf_graph = None
            self._set_in_pickled_file_cache(obj, cache_file)
        self.lookup.efo_ontology = obj


    def _get_available_publications(self):
        self._logger.info('getting literature/publications')
        self.lookup.available_publications = LiteratureLookUpTable(self.es_pub, 'LITERATURE_LOOKUP', self.r_server)


    def _get_from_pickled_file_cache(self, file_id):
        file_path = os.path.join(Config.ONTOLOGY_CONFIG.get('pickle', 'cache_dir'), file_id+'.pck')
        if os.path.isfile(file_path):
            return pickle.load(open(file_path, 'rb'))

    def _set_in_pickled_file_cache(self, obj, file_id):
        if not os.path.isdir(os.path.join(Config.ONTOLOGY_CONFIG.get('pickle', 'cache_dir'))):
            os.makedirs(os.path.join(Config.ONTOLOGY_CONFIG.get('pickle', 'cache_dir')))
        file_path = os.path.join(Config.ONTOLOGY_CONFIG.get('pickle', 'cache_dir'), file_id+'.pck')
        pickle.dump(obj,
                    open(file_path, 'wb'),)

    def _get_available_chembl_mappings(self):
        chembl_handler = ChEMBLLookup()
        chembl_handler.get_molecules_from_evidence()
        all_molecules = set()
        for target, molecules in  chembl_handler.target2molecule.items():
            all_molecules = all_molecules|molecules
        all_molecules = list(all_molecules)
        query_batch_size = 100
        for i in range(0, len(all_molecules) + 1, query_batch_size):
            chembl_handler._populate_synonyms_for_molecule(all_molecules[i:i + query_batch_size])
        self.lookup.chembl = chembl_handler

    def _get_available_hpa(self):
        self._logger.info('getting expressions')
        self.lookup.available_hpa = HPALookUpTable(self.es, 'HPA_LOOKUP',
                                                   self.r_server)




