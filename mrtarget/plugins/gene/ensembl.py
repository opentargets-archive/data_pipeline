from yapsy.IPlugin import IPlugin
from mrtarget.modules.GeneData import Gene
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MatchAll
import sys
import logging

class Ensembl(IPlugin):

    def __init__(self, *args, **kwargs):
        self._logger = logging.getLogger(__name__)

    def merge_data(self, genes, loader, r_server, data_config, es_config):

        es = loader.es
        index = es_config.ens.name

        for row in Search().using(es).index(index).query(MatchAll()).scan():
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
