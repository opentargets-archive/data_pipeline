from yapsy.IPlugin import IPlugin
from opentargets_urlzsource import URLZSource
import traceback
import logging
import json
logging.basicConfig(level=logging.DEBUG)

class Safety(IPlugin):

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self.loader = None
        self.r_server = None
        self.esquery = None
        self.ensembl_current = {}
        self.symbols = {}
        self.safety = {}

    def print_name(self):
        self._logger.info("Target safety plugin")

    def merge_data(self, genes, loader, r_server, data_config):
        self.loader = loader
        self.r_server = r_server

        try:

            self.build_json(filename=data_config.safety)

            for gene_id, gene in genes.iterate():
                # extend gene with related safety data
                if gene.approved_symbol in self.safety:
                        gene.safety = dict()
                        gene.safety = self.safety[gene.approved_symbol]

        except Exception as ex:
            tb = traceback.format_exc()
            self._logger.error(tb)
            self._logger.error('Error %s' % ex)
            raise ex

    def build_json(self, filename):

        with URLZSource(filename).open() as r_file:
            safety_data = json.load(r_file)
            for genekey in safety_data:
                if genekey not in self.safety:
                    self.safety[genekey] = safety_data[genekey]
