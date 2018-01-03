from yapsy.IPlugin import IPlugin
from mrtarget.modules.GeneData import Gene
from mrtarget.modules.ChEMBL import ChEMBLLookup
from tqdm import tqdm
import logging


class ChEMBL(IPlugin):
    def __init__(self, *args, **kwargs):
        self._logger = logging.getLogger(__name__)

    def _gen_chembl_map(self, chembl_id, synonyms):
        return {'id': chembl_id, 'synonyms': synonyms}

    def print_name(self):
        self._logger.info("ChEMBL gene data plugin")

    def merge_data(self, genes, loader, r_server, tqdm_out):
        self._logger.info("Retrieving ChEMBL Drug")
        chembl_handler = ChEMBLLookup()
        chembl_handler.download_molecules_linked_to_target()
        self._logger.info("Retrieving ChEMBL Target Class ")
        chembl_handler.download_protein_classification()
        self._logger.info("Adding ChEMBL data to genes ")

        for _, gene in tqdm(genes.iterate(),
                                  desc='Adding drug data from ChEMBL',
                                  unit=' gene',
                                  file=tqdm_out):
            target_drugnames = []
            ''' extend gene with related drug names '''
            if gene.uniprot_accessions:
                for a in gene.uniprot_accessions:
                    if a in chembl_handler.uni2chembl:
                        chembl_id = chembl_handler.uni2chembl[a]
                        if chembl_id in chembl_handler.target2molecule:
                            molecules = chembl_handler.target2molecule[chembl_id]
                            for mol in molecules:
                                if mol in chembl_handler.molecule2synonyms:
                                    synonyms = chembl_handler.molecule2synonyms[mol]
                                    target_drugnames.extend([self._gen_chembl_map(chembl_id, synonyms)])
                        if a in chembl_handler.protein_classification:
                            gene.protein_classification['chembl'] = chembl_handler.protein_classification[a]
                        break
            if target_drugnames:
                gene.drugs['chembl_drugs'] = target_drugnames
