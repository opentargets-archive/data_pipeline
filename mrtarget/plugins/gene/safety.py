from yapsy.IPlugin import IPlugin
from opentargets_urlzsource import URLZSource
import traceback
import logging
import json

class Safety(IPlugin):

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self.safety = {}

    def print_name(self):
        self._logger.info("Target safety plugin")

    def merge_data(self, genes, loader, r_server, data_config):

        self.build_json(filename=data_config.safety)

        for gene_id, gene in genes.iterate():
            # extend gene with related safety data
            if gene.approved_symbol in self.safety:
                    gene.safety = dict()
                    gene.safety = self.safety[gene.approved_symbol]

    def build_json(self, filename):

        with URLZSource(filename).open() as r_file:
            safety_data = json.load(r_file)
            for genekey in safety_data:
                if genekey not in self.safety:
                    self.safety[genekey] = safety_data[genekey]
