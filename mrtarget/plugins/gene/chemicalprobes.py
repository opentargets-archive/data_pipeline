from yapsy.IPlugin import IPlugin
from opentargets_urlzsource import URLZSource
import traceback
import logging
import csv
import configargparse

class ChemicalProbes(IPlugin):

    # Initiate ChemicalProbes object
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self.ensembl_current = {}
        self.symbols = {}
        self.chemicalprobes = {}

    def merge_data(self, genes, es, r_server, data_config, es_config):

        try:
            # Parse chemical probes data into self.chemicalprobes
            self.build_json(filename1=data_config.chemical_probes_1, 
                filename2=data_config.chemical_probes_2)

            # Iterate through all genes and add chemical probes data if gene symbol is present
            self._logger.info("Generating Chemical Probes data injection")
            for gene_id, gene in genes.iterate():
                # Extend gene with related chemical probe data
                if gene.approved_symbol in self.chemicalprobes:
                    gene.chemicalprobes=self.chemicalprobes[gene.approved_symbol]

        except Exception as ex:
            self._logger.exception(str(ex), exc_info=1)
            raise ex

    def build_json(self, filename1, filename2):
        # *** Work through manually curated chemical probes from the different portals ***
        # chemicalprobes column names are Probe, Target, SGClink, CPPlink, OSPlink, Note
        with URLZSource(filename1).open() as r_file:
            for i, row in enumerate(csv.DictReader(r_file, dialect='excel-tab'), start=1):
        # Generate 'line' for current target
                probelinks = []
                if row["SGClink"] != "":
                    probelinks.append({'source': "Structural Genomics Consortium", 'link': row["SGClink"]})
                if row["CPPlink"] != "":
                    probelinks.append({'source': "Chemical Probes Portal", 'link': row["CPPlink"]})
                if row["OSPlink"] != "":
                    probelinks.append({'source': "Open Science Probes", 'link': row["OSPlink"]})

                line = {
                    "gene": row["Target"],
                    "chemicalprobe": row["Probe"],
                    "sourcelinks": probelinks,
                    "note": row["Note"]
                }
                # Add data for current chemical probe to self.chemicalprobes[Target]['portalprobes']
                # If gene has not appeared in chemical probe list yet,
                # initialise self.chemicalprobes with an empty list
                if row["Target"] not in self.chemicalprobes:
                    self.chemicalprobes[row["Target"]] = {}
                    self.chemicalprobes[row["Target"]]['portalprobes'] = []
                self.chemicalprobes[row["Target"]]['portalprobes'].append(line)

        # *** Work through Probe Miner targets ***
        # probeminer column names are Target, UniPRotID, NrofProbes
        # probeminer column names are hgnc_symbol, uniprot_symbol, nr_of_probes
        with URLZSource(filename2).open() as r_file:
            for i, row in enumerate(csv.DictReader(r_file, dialect='excel-tab'), start=1):
                PMdata = {
                    "probenumber": row["nr_of_probes"],
                    "link": "https://probeminer.icr.ac.uk/#/"+row["uniprot_symbol"]
                }
                if row["hgnc_symbol"] not in self.chemicalprobes:
                    self.chemicalprobes[row["hgnc_symbol"]] = {}
                self.chemicalprobes[row["hgnc_symbol"]]['probeminer'] = PMdata

