
import logging

from yapsy.IPlugin import IPlugin
import simplejson as json
import configargparse

from mrtarget.modules.GeneData import Gene
from mrtarget.Settings import Config
from mrtarget.common import URLZSource


class HGNC(IPlugin):

    def __init__(self, *args, **kwargs):
        self._logger = logging.getLogger(__name__)


    def print_name(self):
        self._logger.info("HGNC gene data plugin")

    def merge_data(self, genes, loader, r_server):
        #handle plugin specific configuration here
        #helps separate the plugin from the rest of the pipeline
        #and makes it easier to manage custom plugins
        p = configargparse.get_argument_parser()

        p.add("--hgnc-complete-set", help="location of hgnc complete set",
            env_var="HGNC_COMPLETE_SET", action='store')

        #dont use parse_args because that will error
        #if there are extra arguments e.g. for plugins
        self.args = p.parse_known_args()[0]

        self._logger.info("HGNC parsing - requesting from URL %s" % self.args.hgnc_complete_set)

        with URLZSource(self.args.hgnc_complete_set).open() as source:

            data = json.load(source)

            for row in data['response']['docs']:
                gene = Gene()
                gene.load_hgnc_data_from_json(row)
                genes.add_gene(gene)

            self._logger.info("STATS AFTER HGNC PARSING:\n" + genes.get_stats())
