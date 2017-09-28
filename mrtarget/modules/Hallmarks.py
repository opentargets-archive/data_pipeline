import json
import re
import logging
from mrtarget.common import Actions
from mrtarget.Settings import Config, file_or_resource
from mrtarget.common.ElasticsearchQuery import ESQuery

HALLMARK_FILENAME = file_or_resource('census_annot.tsv')

class HallmarksActions(Actions):
    GENERATE_JSON = 'generatejson'

class Hallmarks():

    def __init__(self, loader=None, es=None, r_server=None):
        self.loader = loader
        self.es = es
        self.r_server = r_server
        self.esquery  = ESQuery(self.es)
        self.ensembl_current = {}
        self.symbols = {}
        self.hallmarks = {}
        self.logger = logging.getLogger(__name__)

    def process_hallmarks(self, infile=HALLMARK_FILENAME):
        self.load_Ensembl()
        self.build_json(filename=infile)
        self.save_to_elasticsearch()

    def load_Ensembl(self):
        self.logger.debug("Loading ES Ensembl {0} assembly genes and non reference assembly".format(
            Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY))

        n = 0

        for row in self.esquery.get_all_ensembl_genes():
            n += 1
            self.ensembl_current[row["id"]] = row
            # put the ensembl_id in symbols too
            display_name = row["display_name"]
            if display_name not in self.symbols:
                self.symbols[display_name] = {}
                self.symbols[display_name]["assembly_name"] = row["assembly_name"]
                self.symbols[display_name]["ensembl_release"] = row["ensembl_release"]
            if row["is_reference"]:
                self.symbols[display_name]["ensembl_primary_id"] = row["id"]
            else:
                if "ensembl_secondary_id" not in self.symbols[display_name] or row["id"] < \
                        self.symbols[display_name]["ensembl_secondary_id"]:
                    self.symbols[display_name]["ensembl_secondary_id"] = row["id"];
                if "ensembl_secondary_ids" not in self.symbols[display_name]:
                    self.symbols[display_name]["ensembl_secondary_ids"] = []
                self.symbols[display_name]["ensembl_secondary_ids"].append(row["id"])

        self.logger.debug("Loading "+ str(n) +" Ensembl Genes completed")

    def build_json(self, filename=HALLMARK_FILENAME):

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

                if GeneSymbol in self.symbols:
                    record = self.symbols
                    # u'GNB3': {'assembly_name': u'GRCh38', 'ensembl_release': 89,'ensembl_primary_id': u'ENSG00000111664'},
                    if "ensembl_primary_id" in record[GeneSymbol]:
                        ensembl_gene_id = record[GeneSymbol]["ensembl_primary_id"]
                    elif "ensembl_secondary_ids" in record:
                        ensembl_gene_id = record[GeneSymbol]["ensembl_secondary_ids"][0]
                    else:
                        self.logger.error("%s is in Ensembl but cound not find its ensembl_gene_id" %GeneSymbol)
                        continue

                    if ensembl_gene_id not in self.hallmarks:
                        self.hallmarks[ensembl_gene_id] = {}

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
                            self.hallmarks[ensembl_gene_id]["function_summary"].append(Description)
                        except KeyError:
                            self.hallmarks[ensembl_gene_id]["function_summary"] = list()
                            self.hallmarks[ensembl_gene_id]["function_summary"].append(Description)
                    elif Description_1 == 'cell division control':
                        try:
                            self.hallmarks[ensembl_gene_id]["cell_division_control"].append(Description)
                        except KeyError:
                            self.hallmarks[ensembl_gene_id]["cell_division_control"] = list()
                            self.hallmarks[ensembl_gene_id]["cell_division_control"].append(Description)
                    elif Description_1 == 'types of alteration in cancer':
                        try:
                            self.hallmarks[ensembl_gene_id]["alteration_in_cancer"].append(Description)
                        except KeyError:
                            self.hallmarks[ensembl_gene_id]["alteration_in_cancer"] = list()
                            self.hallmarks[ensembl_gene_id]["alteration_in_cancer"].append(Description)
                    elif Description_1 == 'role in cancer':
                        try:
                            self.hallmarks[ensembl_gene_id]["role_in_cancer"].append(Description)
                        except KeyError:
                            self.hallmarks[ensembl_gene_id]["role_in_cancer"] = list()
                            self.hallmarks[ensembl_gene_id]["role_in_cancer"].append(Description)
                    elif Description_1 == 'senescence':
                        try:
                            self.hallmarks[ensembl_gene_id]["senescence"].append(Description)
                        except KeyError:
                            self.hallmarks[ensembl_gene_id]["senescense"] = list()
                            self.hallmarks[ensembl_gene_id]["senescense"].append(Description)
                    # Should be then the Hallmarks
                    else:
                        # "Short"=> 'a' & 's'
                        h = {Description_1: Description, "Short": Short}

                        try:
                            self.hallmarks[ensembl_gene_id]["info"].append(h)
                        except KeyError:
                            self.hallmarks[ensembl_gene_id]["info"] = list()
                            self.hallmarks[ensembl_gene_id]["info"].append(h)

                    self.hallmarks[ensembl_gene_id]["symbol"] = GeneSymbol


    def save_to_elasticsearch(self):

        for genesymbol, data in self.hallmarks.items():
            self.loader.put(Config.ELASTICSEARCH_HALLMARK_INDEX_NAME,
                            Config.ELASTICSEARCH_HALLMARK_DOC_NAME,
                            genesymbol,
                            json.dumps(data),
                            True)






