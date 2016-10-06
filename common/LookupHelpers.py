import logging
import time
from tqdm import tqdm
from common.ElasticsearchQuery import ESQuery
from modules.ECO import ECOLookUpTable
from modules.EFO import EFOLookUpTable
from modules.GeneData import GeneLookUpTable
from modules.Literature import LiteratureLookUpTable


class LookUpData():
    def __init__(self):
        self.available_genes = None
        self.available_efos = None
        self.available_ecos = None
        self.available_publications = None
        self.uni2ens = None
        self.non_reference_genes = None
        self.available_gene_objects = None
        self.available_efo_objects = None
        self.available_eco_objects = None


class LookUpDataRetriever():
    def __init__(self,
                 es = None,
                 r_server = None,
                 targets = []):

        self.es = es
        self.r_server = r_server
        if es is not None:
            self.esquery = ESQuery(es)
        self.lookup = LookUpData()
        self.logger = logging.getLogger(__name__)
        start_time = time.time()
        load_bar = tqdm(desc='loading lookup data',
             total=3,
             unit=' steps',
             leave=False,)
        self._get_gene_info(targets)
        self.logger.info("finished self._get_gene_info(), took %ss" % str(time.time() - start_time))
        load_bar.update()
        self._get_available_efos()
        self.logger.info("finished self._get_available_efos(), took %ss"%str(time.time()-start_time))
        load_bar.update()
        self._get_available_ecos()
        self.logger.info("finished self._get_available_ecos(), took %ss"%str(time.time()-start_time))
        load_bar.update()
        self._get_available_publications()
        self.logger.info("finished self._get_available_publications(), took %ss" % str(time.time() - start_time))
        load_bar.update()


    def _get_available_efos(self):
        self.logger.info('getting efos')
        self.lookup.available_efos = EFOLookUpTable(self.es,'EFO_LOOKUP', self.r_server)

    def _get_available_ecos(self):
        self.logger.info('getting ecos')
        self.lookup.available_ecos = ECOLookUpTable(self.es, 'ECO_LOOKUP', self.r_server)


    def _get_gene_info(self, targets=[]):
        self.logger.info('getting gene info')
        self.lookup.available_genes = GeneLookUpTable(self.es, 'GENE_LOOKUP', self.r_server, targets = targets)
        self.lookup.uni2ens = self.lookup.available_genes.uniprot2ensembl
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

    def _get_available_publications(self):
        self.logger.info('getting literature/publications')
        self.lookup.available_publications = LiteratureLookUpTable(self.es, 'LITERATURE_LOOKUP', self.r_server)