from yapsy.IPlugin import IPlugin
from opentargets_urlzsource import URLZSource
import re
import traceback
import logging
import csv
import configargparse

class Hallmarks(IPlugin):

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self.loader = None
        self.r_server = None
        self.esquery = None
        self.ensembl_current = {}
        self.symbols = {}
        self.hallmarks = {}

        self.hallmarks_labels = ["escaping programmed cell death",
                                  "angiogenesis",
                                  "genome instability and mutations",
                                  "change of cellular energetics",
                                  "cell replicative immortality",
                                  "invasion and metastasis",
                                  "tumour promoting inflammation",
                                  "suppression of growth",
                                  "escaping immune response to cancer",
                                  "proliferative signalling"]

    def print_name(self):
        self._logger.info("Hallmarks of cancer gene data plugin")

    def merge_data(self, genes, loader, r_server, data_config):
        self.loader = loader
        self.r_server = r_server

        try:

            self.build_json(filename=data_config.hallmark)

            for gene_id, gene in genes.iterate():
                ''' extend gene with related Hallmark data '''
                if gene.approved_symbol in self.hallmarks:
                        gene.hallmarks = dict()
                        gene.hallmarks = self.hallmarks[gene.approved_symbol]

        except Exception as ex:
            tb = traceback.format_exc()
            self._logger.error(tb)
            self._logger.error('Error %s' % ex)
            raise ex

    def build_json(self, filename):
        # Just for reference: column names are: "ID_CENSUS_ANNOT", "ID_CENSUS", "ID_GENE", "GENE_NAME", "CELL_TYPE",
        # "PUBMED_PMID", "ID_DATA_CATEGORY", "DESCRIPTION", "DISPLAY", "SHORT", "CELL_LINE", "DESCRIPTION_1")
        with URLZSource(filename).open() as r_file:
            for i, row in enumerate(csv.DictReader(r_file, dialect='excel-tab'), start=1):

                PMID = re.sub(r'^"|"$', '', row["PUBMED_PMID"])
                Short = re.sub(r'^"|"$', '', row["SHORT"])
                GeneSymbol = re.sub(r'^"|"$', '', row["GENE_NAME"])
                Description_1 = re.sub(r'^"|"$', '', row["DESCRIPTION_1"])
                Description_1.rstrip()
                Description = re.sub(r'^"|"$', '', row["DESCRIPTION"])

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



