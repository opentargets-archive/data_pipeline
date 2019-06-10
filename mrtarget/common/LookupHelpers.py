import logging
import os
import time

from mrtarget.common.LookupTables import ECOLookUpTable
from mrtarget.common.LookupTables import EFOLookUpTable
from mrtarget.common.LookupTables import HPALookUpTable
from mrtarget.common.LookupTables import GeneLookUpTable

from mrtarget.common.IO import file_or_resource


class LookUpData():
    def __init__(self):
        self.available_genes = None
        self.available_efos = None
        self.available_ecos = None
        self.available_hpa = None
        self.non_reference_genes = None
        self.mp_ontology = None


class LookUpDataType(object):
    TARGET = 'target'
    DISEASE = 'disease'
    ECO = 'eco'
    HPA = 'hpa'

class LookUpDataRetriever(object):
    def __init__(self,
                 es,
                 data_types,
                 gene_index = None,
                 eco_index = None,
                 hpa_index = None,
                 efo_index = None
                 ):
        self.es = es

        self.lookup = LookUpData()

        self._logger = logging.getLogger(__name__)

        for dt in data_types:
            if dt == LookUpDataType.TARGET:
                self.lookup.available_genes = GeneLookUpTable(self.es, gene_index)
                self._get_non_reference_gene_mappings()
            elif dt == LookUpDataType.DISEASE:
                self.lookup.available_efos = EFOLookUpTable(self.es, efo_index)
            elif dt == LookUpDataType.ECO:
                self.lookup.available_ecos = ECOLookUpTable(self.es, eco_index)
            elif dt == LookUpDataType.HPA:
                self.lookup.available_hpa = HPALookUpTable(self.es, hpa_index)


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

        




