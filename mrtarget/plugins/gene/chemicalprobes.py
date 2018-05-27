from yapsy.IPlugin import IPlugin
from mrtarget.Settings import Config
from tqdm import tqdm
import traceback
import logging
logging.basicConfig(level=logging.DEBUG)

class ChemicalProbes(IPlugin):

    # Initiate ChemicalProbes object
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self.loader = None
        self.r_server = None
        self.esquery = None
        self.ensembl_current = {}
        self.symbols = {}
        self.chemicalprobes = {}
        self.tqdm_out = None

    def print_name(self):
        self._logger.info("Chemical Probes plugin")

    def merge_data(self, genes, loader, r_server, tqdm_out):

        self.loader = loader
        self.r_server = r_server
        self.tqdm_out = tqdm_out

        try:
            # Parse chemical probes data into self.chemicalprobes
            self.build_json(filename=Config.CHEMICALPROBES_FILENAME)

            # Iterate through all genes and add chemical probes data if gene symbol is present
            self._logger.info("Generating Chemical Probes data injection")
            for gene_id, gene in tqdm(genes.iterate(),
                                      desc='Adding Chemical Probes data',
                                      unit=' gene',
                                      file=self.tqdm_out):
                # Extend gene with related chemical probe data
                if gene.approved_symbol in self.chemicalprobes:
                    self._logger.debug("Adding Chemical Probe data to gene %s", gene.approved_symbol)
                    gene.chemicalprobes=self.chemicalprobes[gene.approved_symbol]

        except Exception as ex:
            self._logger.exception(str(ex), exc_info=1)
            raise ex

    def build_json(self, filename=Config.CHEMICALPROBES_FILENAME):

        with open(filename, 'r') as input:
            for row in input:
                (Probe, Target, SGClink, CPPlink, OSPlink, Note) = tuple(row.rstrip().split(';'))

                # If gene has not appeared in chemical probe list yet,
                # initialise self.chemicalprobes with an empty list
                if Target not in self.chemicalprobes:
                    self.chemicalprobes[Target] = []

                # Create list of dictionaries for the links
                probelinks = []
                if SGClink != "":
                    probelinks.append({'source': "Structural Genomics Consortium", 'link': SGClink})
                if CPPlink != "":
                    probelinks.append({'source': "Chemical Probes Portal", 'link': CPPlink})
                if OSPlink != "":
                    probelinks.append({'source': "Open Science Probes", 'link': OSPlink})

                line = {
                    "gene": Target,
                    "chemicalprobe": Probe,
                    "sourcelinks": probelinks,
                    "note": Note
                }
                # Add data for current chemical probe to self.chemicalprobes
                self.chemicalprobes[Target].append(line)
