import logging
import os
import time

from mrtarget.common.LookupTables import ECOLookUpTable
from mrtarget.common.LookupTables import EFOLookUpTable
from mrtarget.common.LookupTables import HPALookUpTable
from mrtarget.common.LookupTables import GeneLookUpTable

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

        self.mp_ontology = None

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
    ECO = 'eco'
    HPA = 'hpa'


class LookUpDataRetriever(object):
    def __init__(self,
                 es,
                 r_server,
                 targets,
                 data_types,
                 hpo_uri = None,
                 mp_uri = None
                 ):
        self.es = es
        self.r_server = r_server

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
            elif dt == LookUpDataType.HPA:
                self.lookup.available_hpa = HPALookUpTable(self.es, 'HPA_LOOKUP', self.r_server)

            self._logger.info("loaded %s in %ss" % (dt, str(int(time.time() - start_time))))

    def set_r_server(self, r_server):
        self.r_server = r_server
        self.lookup.set_r_server(r_server)

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

        




