from yapsy.IPlugin import IPlugin
from mrtarget.modules.GeneData import Gene
from mrtarget.Settings import Config
from mrtarget.common.ElasticsearchQuery import ESQuery
from elasticsearch.exceptions import NotFoundError
from tqdm import tqdm
import sys
import logging
logging.basicConfig(level=logging.INFO)

class Ensembl(IPlugin):

    def __init__(self, *args, **kwargs):
        self._logger = logging.getLogger(__name__)

    def print_name(self):
        self._logger.info("ENSEMBL gene data plugin")

    def merge_data(self, genes, loader, r_server, tqdm_out):

        esquery = ESQuery(loader.es)

        try:
            count = esquery.count_elements_in_index(Config.ELASTICSEARCH_ENSEMBL_INDEX_NAME)
        except NotFoundError as ex:
            self._logger.error('no Ensembl index in ES. Skipping. Has the --ensembl step been run? Are you pointing to the correct index? %s' % ex)
            raise ex


        for row in tqdm(esquery.get_all_ensembl_genes(),
                        desc='loading genes from Ensembl',
                        unit_scale=True,
                        unit='genes',
                        file=tqdm_out,
                        leave=False,
                        total=esquery.count_elements_in_index(Config.ELASTICSEARCH_ENSEMBL_INDEX_NAME)):
            if row['id'] in genes:
                gene = genes.get_gene(row['id'])
                gene.load_ensembl_data(row)
                genes.add_gene(gene)
            else:
                gene = Gene()
                gene.load_ensembl_data(row)
                genes.add_gene(gene)

        self._clean_non_reference_genes(genes)

        self._logger.info("STATS AFTER ENSEMBL PARSING:\n" + genes.get_stats())

    def _clean_non_reference_genes(self, genes):
        for geneid, gene in genes.iterate():
            if not gene.is_ensembl_reference:
                genes.remove_gene(geneid)
