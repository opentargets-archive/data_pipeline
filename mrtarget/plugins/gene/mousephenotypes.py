from yapsy.IPlugin import IPlugin
from mrtarget.modules.GeneData import Gene
import logging
logging.basicConfig(level=logging.DEBUG)

class MousePhenotypes(IPlugin):

    def print_name(self):
        logging.info("This is plugin MousePhenotypes")
        gene = Gene()

    def merge_data(self, genes, esquery, tqdm_out):
        logging.info("TODO")
