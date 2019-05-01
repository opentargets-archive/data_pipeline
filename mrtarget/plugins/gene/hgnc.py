import logging

from yapsy.IPlugin import IPlugin
import simplejson as json
import configargparse

from mrtarget.modules.GeneData import Gene
from opentargets_urlzsource import URLZSource


class HGNC(IPlugin):

    def __init__(self, *args, **kwargs):
        self._logger = logging.getLogger(__name__)

    def merge_data(self, genes, es, r_server, data_config, es_config):

        self._logger.info("HGNC parsing - requesting from URL %s", data_config.hgnc_complete_set)

        with URLZSource(data_config.hgnc_complete_set).open() as source:

            data = json.load(source)

            for row in data['response']['docs']:
                gene = Gene()
                gene.load_hgnc_data_from_json(row)
                genes.add_gene(gene)

            self._logger.info("STATS AFTER HGNC PARSING:\n" + genes.get_stats())
