from yapsy.IPlugin import IPlugin
from mrtarget.Settings import Config
import re
from tqdm import tqdm
import traceback
import logging
logging.basicConfig(level=logging.DEBUG)

class Hallmarks(IPlugin):

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self.loader = None
        self.r_server = None
        self.esquery = None
        self.ensembl_current = {}
        self.symbols = {}
        self.hallmarks = {}
        self.tqdm_out = None

    def print_name(self):
        self._logger.info("Hallmarks of cancer gene data plugin")

    def merge_data(self, genes, loader, r_server, tqdm_out):

        self.loader = loader
        self.r_server = r_server
        self.tqdm_out = tqdm_out

        try:

            self.build_json(filename=Config.HALLMARK_FILENAME)

            for gene_id, gene in tqdm(genes.iterate(),
                                      desc='Adding Hallmarks of cancer data',
                                      unit=' gene',
                                      file=self.tqdm_out):
                ''' extend gene with related Hallmark data '''
                if gene.approved_symbol in self.hallmarks:
                        gene.hallmarks = dict()
                        self._logger.info("Adding Hallmark data to gene %s" % (gene.approved_symbol))
                        gene.hallmarks = self.hallmarks[gene.approved_symbol]

        except Exception as ex:
            tb = traceback.format_exc()
            self._logger.error(tb)
            self._logger.error('Error %s' % ex)
            raise ex

    def build_json(self, filename=Config.HALLMARK_FILENAME):

        with open(filename, 'r') as input:
            n = 0
            for row in input:
                n += 1
                (CensusAnnotation, CensusId, GeneId, GeneSymbol, CellType, PMID, Category, Description, Display, Short, CellLine, Description_1) = tuple(row.rstrip().split('\t'))

                PMID = re.sub(r'^"|"$', '', PMID)
                Short = re.sub(r'^"|"$', '', Short)
                GeneSymbol = re.sub(r'^"|"$', '', GeneSymbol)
                Description_1 = re.sub(r'^"|"$', '', Description_1)
                Description = re.sub(r'^"|"$', '', Description)
                Description = PMID + ":" + Description

                if GeneSymbol not in self.hallmarks:
                    self.hallmarks[GeneSymbol] = dict()

                '''
                    Census Hallmark Sections:
                        "Function summary"
                        "Cell division control"
                        "Types of alteration in cancer"
                        "Role in cancer"
                        "Senescence"
                '''
                if Description_1 == 'function summary':
                    try:
                        self.hallmarks[GeneSymbol]["function_summary"].append(Description)
                    except KeyError:
                        self.hallmarks[GeneSymbol]["function_summary"] = list()
                        self.hallmarks[GeneSymbol]["function_summary"].append(Description)
                elif Description_1 == 'cell division control':
                    try:
                        self.hallmarks[GeneSymbol]["cell_division_control"].append(Description)
                    except KeyError:
                        self.hallmarks[GeneSymbol]["cell_division_control"] = list()
                        self.hallmarks[GeneSymbol]["cell_division_control"].append(Description)
                elif Description_1 == 'types of alteration in cancer':
                    try:
                        self.hallmarks[GeneSymbol]["alteration_in_cancer"].append(Description)
                    except KeyError:
                        self.hallmarks[GeneSymbol]["alteration_in_cancer"] = list()
                        self.hallmarks[GeneSymbol]["alteration_in_cancer"].append(Description)
                elif Description_1 == 'role in cancer':
                    try:
                        self.hallmarks[GeneSymbol]["role_in_cancer"].append(Description)
                    except KeyError:
                        self.hallmarks[GeneSymbol]["role_in_cancer"] = list()
                        self.hallmarks[GeneSymbol]["role_in_cancer"].append(Description)
                elif Description_1 == 'senescence':
                    try:
                        self.hallmarks[GeneSymbol]["senescence"].append(Description)
                    except KeyError:
                        self.hallmarks[GeneSymbol]["senescense"] = list()
                        self.hallmarks[GeneSymbol]["senescense"].append(Description)
                # Should be then the Hallmarks
                else:
                    # "Short"=> 'a' & 's'
                    h = {Description_1: Description, "Short": Short}

                    try:
                        self.hallmarks[GeneSymbol]["info"].append(h)
                    except KeyError:
                        self.hallmarks[GeneSymbol]["info"] = list()
                        self.hallmarks[GeneSymbol]["info"].append(h)
