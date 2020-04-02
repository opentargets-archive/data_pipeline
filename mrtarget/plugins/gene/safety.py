from yapsy.IPlugin import IPlugin
from opentargets_urlzsource import URLZSource
import logging
import simplejson as json
import csv

class Safety(IPlugin):

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self.safety = {}
        self.experimental_toxicity = {}

    def merge_data(self, genes, es, r_server, data_config, es_config):

        # Read adverse_effects and risk_info
        self.build_json_safety(filename=data_config.safety)

        # Read experimental toxicity
        self.build_json_experimental_toxicity(uri=data_config.experimental_toxicity)


        for gene_id, gene in genes.iterate():
            # extend gene with related safety data
            if gene.approved_symbol in self.safety:
                    gene.safety = dict()
                    gene.safety = self.safety[gene.approved_symbol]
            if gene.id in self.experimental_toxicity:
                if hasattr(gene, 'safety'):
                    gene.safety['experimental_toxicity'] = self.experimental_toxicity[gene.id]
                else:
                    gene.safety = {'experimental_toxicity' : self.experimental_toxicity[gene.id]}

    def build_json_safety(self, filename):
        with URLZSource(filename).open() as r_file:
            safety_data = json.load(r_file)
            for genekey in safety_data:
                if genekey not in self.safety:
                    self.safety[genekey] = safety_data[genekey]
                else:
                    self._logger.info("Safety gene id duplicated: " + genekey)

    # Create a dict using gene_id as key and an array to collapse the common info.
    def build_json_experimental_toxicity(self, uri):
        with URLZSource(uri).open() as f_obj:
            for row in csv.DictReader(f_obj, dialect='excel-tab'):
                toxicity_json = self.exp_toxicity_json_format(row)
                genekey = row["ensembl_gene_id"].strip()
                if genekey not in self.experimental_toxicity:
                    self.experimental_toxicity[genekey]= []
                self.experimental_toxicity[genekey].append(toxicity_json)

    # Shape the info as the user requested
    def exp_toxicity_json_format(self, row):
        exp_toxicity_dict = dict()
        for key, value in row.items():
            if key not in "ensembl_gene_id":
                if key in ("data_source","data_source_reference_link"):
                    exp_toxicity_dict[key] = value
                else:
                    if "experiment_details" not in exp_toxicity_dict:
                        exp_toxicity_dict["experiment_details"] = {}
                    exp_toxicity_dict["experiment_details"][key] = value

        return exp_toxicity_dict
