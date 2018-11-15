from yapsy.IPlugin import IPlugin
from mrtarget.modules.GeneData import Gene
from mrtarget.Settings import Config
import requests
import logging

class HGNC(IPlugin):

    def __init__(self, *args, **kwargs):
        self._logger = logging.getLogger(__name__)

    def print_name(self):
        self._logger.info("HGNC gene data plugin")

    def merge_data(self, genes, loader, r_server):
        self._logger.info("HGNC parsing - requesting from URL %s" % Config.HGNC_COMPLETE_SET)

        r = requests.get(Config.HGNC_COMPLETE_SET)
        r.raise_for_status()
        data = r.json()

        for row in data['response']['docs']:
            gene = Gene()
            gene.load_hgnc_data_from_json(row)
            genes.add_gene(gene)

        self._logger.info("STATS AFTER HGNC PARSING:\n" + genes.get_stats())
