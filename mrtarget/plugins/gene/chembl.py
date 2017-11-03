from yapsy.IPlugin import IPlugin
from mrtarget.modules.GeneData import Gene
from mrtarget.modules.ChEMBL import ChEMBLLookup
from tqdm import tqdm
import logging
logging.basicConfig(level=logging.INFO)

class ChEMBL(IPlugin):
    def print_name(self):
        logging.info("ChEMBL gene data plugin")

    def merge_data(self, genes, loader, r_server, tqdm_out):
    
        logging.info("Retrieving ChEMBL Drug")
        chembl_handler = ChEMBLLookup()
        chembl_handler.download_molecules_linked_to_target()
        logging.info("Retrieving ChEMBL Target Class ")
        chembl_handler.download_protein_classification()
        logging.info("Adding ChEMBL data to genes ")

        for gene_id, gene in tqdm(genes.iterate(),
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
                                    target_drugnames.extend(synonyms)
                        if a in chembl_handler.protein_classification:
                            gene.protein_classification['chembl'] = chembl_handler.protein_classification[a]
                        break
            if target_drugnames:
                gene.drugs['chembl_drugs'] = target_drugnames
