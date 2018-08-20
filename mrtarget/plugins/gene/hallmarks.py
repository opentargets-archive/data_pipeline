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

        self.hallmarks_labels = ["escaping programmed cell death",
                                  "angiogenesis",
                                  "genome instability and mutations",
                                  "change of cellular energetics",
                                  "cell replicative immortality",
                                  "invasion and metastasis",
                                  "tumour promoting inflammation",
                                  "suppression of growth",
                                  "escaping immune response to cancer",
                                  "proliferative signalling",
                                 ]

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
                Description_1.rstrip()
                Description = re.sub(r'^"|"$', '', Description)

                if GeneSymbol not in self.hallmarks:
                    self.hallmarks[GeneSymbol] = dict()

                if Description_1 in self.hallmarks_labels:
                    promote  = False
                    suppress = False

                    if Short == 'a': promote = True
                    if Short == 's': suppress = True

                    line = {
                             "label": Description_1,
                             "description": Description,
                             "promote": promote,
                             "suppress": suppress,
                             "pmid": PMID
                            }

                    try:
                        self.hallmarks[GeneSymbol]["cancer_hallmarks"].append(line)
                    except KeyError:
                        self.hallmarks[GeneSymbol]["cancer_hallmarks"] = list()
                        self.hallmarks[GeneSymbol]["cancer_hallmarks"].append(line)

                elif Description_1 == 'function summary':
                    line = {"pmid": PMID, "description": Description}

                    try:
                        self.hallmarks[GeneSymbol]["function_summary"].append(line)
                    except KeyError:
                        self.hallmarks[GeneSymbol]["function_summary"] = list()
                        self.hallmarks[GeneSymbol]["function_summary"].append(line)

                else:
                    line = {
                             "attribute_name": Description_1,
                             "description": Description,
                             "pmid": PMID
                           }

                    try:
                        self.hallmarks[GeneSymbol]["attributes"].append(line)
                    except KeyError:
                        self.hallmarks[GeneSymbol]["attributes"] = list()
                        self.hallmarks[GeneSymbol]["attributes"].append(line)



