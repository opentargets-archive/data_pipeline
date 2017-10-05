from yapsy.IPlugin import IPlugin
from mrtarget.modules.GeneData import Gene
from mrtarget.Settings import Config
from tqdm import tqdm
import logging
logging.basicConfig(level=logging.INFO)

class Ensembl(IPlugin):

    def print_name(self):
        logging.info("This is plugin ENSEMBL")

    def merge_data(self, genes, esquery, tqdm_out):

        try:
            esquery.get_all_ensembl_genes()


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

        self._clean_non_reference_genes()

        logging.info("STATS AFTER ENSEMBL PARSING:\n" + genes.get_stats())

    def _clean_non_reference_genes(self):
        for geneid, gene in genes.iterate():
            if not gene.is_ensembl_reference:
                genes.remove_gene(geneid)
