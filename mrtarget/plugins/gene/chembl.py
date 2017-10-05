from yapsy.IPlugin import IPlugin
from mrtarget.modules.GeneData import Gene
import logging
logging.basicConfig(level=logging.DEBUG)
class ChEMBL(IPlugin):
    def print_name(self):
        logging.info("This is plugin ChEMBL")
        gene = Gene()

    def merge_data(self, genes, esquery=None, tqdm_out):
        logging.info("TODO")