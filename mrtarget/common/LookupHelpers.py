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

class LookUpDataRetriever(object):
    def __init__(self, es,
            gene_index = None, 
            gene_cache_size = None,
            gene_cache_u2e_size = None,
            gene_cache_contains_size = None, 
            eco_index = None,
            eco_cache_size = None,
            hpa_index = None, 
            hpa_cache_size = None,
            efo_index = None,
            efo_cache_size = None,
            efo_cache_contains_size = None
            ):

        self.es = es
        self.lookup = LookUpData()
        self._logger = logging.getLogger(__name__)

        if gene_index is not None:
            self.lookup.available_genes = GeneLookUpTable(self.es, gene_index,
                gene_cache_size, gene_cache_u2e_size, gene_cache_contains_size)
            self._get_non_reference_gene_mappings()
        if efo_index is not None:
            self.lookup.available_efos = EFOLookUpTable(self.es, efo_index,
            efo_cache_size, efo_cache_contains_size)
        if eco_index is not None:
            self.lookup.available_ecos = ECOLookUpTable(self.es, eco_index, 
            eco_cache_size)
        if hpa_index is not None:
            self.lookup.available_hpa = HPALookUpTable(self.es, hpa_index, 
            hpa_cache_size)


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

        




