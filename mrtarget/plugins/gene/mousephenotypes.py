
import logging

from yapsy.IPlugin import IPlugin
import simplejson as json
import configargparse

from mrtarget.common import URLZSource
from opentargets_ontologyutils.rdf_utils import OntologyClassReader
import opentargets_ontologyutils.mp
from mrtarget.Settings import Config

#TODO move this to config
PHENOTYPE_CATEGORIES = {
    "MP:0005386" : "behavior/neurological phenotype",
    "MP:0005375" : "adipose tissue phenotype",
    "MP:0005385" : "cardiovascular system phenotype",
    "MP:0005384" : "cellular phenotype",
    "MP:0005382" : "craniofacial phenotype",
    "MP:0005381" : "digestive/alimentary phenotype",
    "MP:0005380" : "embryo phenotype",
    "MP:0005379" : "endocrine/exocrine phenotype",
    "MP:0005378" : "growth/size/body region phenotype",
    "MP:0005377" : "hearing/vestibular/ear phenotype",
    "MP:0005397" : "hematopoietic system phenotype",
    "MP:0005376" : "homeostasis/metabolism phenotype",
    "MP:0005387" : "immune system phenotype",
    "MP:0010771" : "integument phenotype",
    "MP:0005371" : "limbs/digits/tail phenotype",
    "MP:0005370" : "liver/biliary system phenotype",
    "MP:0010768" : "mortality/aging",
    "MP:0005369" : "muscle phenotype",
    "MP:0002006" : "neoplasm",
    "MP:0003631" : "nervous system phenotype",
    "MP:0002873" : "normal phenotype",
    "MP:0001186" : "pigmentation phenotype",
    "MP:0005367" : "renal/urinary system phenotype",
    "MP:0005389" : "reproductive system phenotype",
    "MP:0005388" : "respiratory system phenotype",
    "MP:0005390" : "skeleton phenotype",
    "MP:0005394" : "taste/olfaction phenotype",
    "MP:0005391" : "vision/eye phenotype"
}

class MousePhenotypes(IPlugin):

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self.loader = None
        self.r_server = None
        self.mouse_genes = {}
        self.ancestors = {}
        self.mps = {}
        self.mp_labels = {}
        self.mp_to_label = {}
        self.top_levels = {}
        self.homologs = {}
        self.human_genes = {}
        self.not_found_genes = {}
        self.human_ensembl_gene_ids = {}
        self.data_config = None

    def print_name(self):
        self._logger.debug("MousePhenotypes gene data plugin")

    def merge_data(self, genes, loader, r_server, data_config):

        self.loader = loader
        self.r_server = r_server
        self.data_config = data_config

        self._get_mp_classes(self.data_config.ontology_mp)

        self.get_genotype_phenotype()

        self.assign_to_human_genes()

        for gene_id, gene in genes.iterate():
            ''' extend gene with related mouse phenotype data '''
            if gene.approved_symbol in self.human_genes:
                    self._logger.debug("Adding %i phenotype data from MGI to gene %s" % (len(self.human_genes[gene.approved_symbol]["mouse_orthologs"][0]["phenotypes"]), gene.approved_symbol))
                    gene.mouse_phenotypes = self.human_genes[gene.approved_symbol]["mouse_orthologs"]

    def _get_mp_classes(self, mp_uri):
        self._logger.debug("_get_mp_classes")
        
        #load the onotology
        self.mp_ontology = OntologyClassReader()
        opentargets_ontologyutils.mp.load_mammalian_phenotype_ontology(self.mp_ontology, mp_uri)

        #TODO this is a moderately hideous bit of pointless munging, but I don't have time fix it now!

        for mp_id,label in self.mp_ontology.current_classes.items():

            mp_class = {}
            mp_class["label"] = label
            if mp_id not in self.mp_ontology.classes_paths:
                self._logger.warning("cannot find paths for "+mp_id)
                continue
            mp_class["path"] = self.mp_ontology.classes_paths[mp_id]['all']
            mp_class["path_codes"] = self.mp_ontology.classes_paths[mp_id]['ids']

            mp_id_key = mp_id.split("/")[-1].replace(":", "_")
            self.mps[mp_id_key] = mp_class
            self.mp_labels[mp_class["label"]] = mp_id
            self.mp_to_label[mp_id] = mp_class["label"]
            paths = []
            for path in mp_class["path"]:
                item = path[0]
                paths.append(item)

            self.top_levels[mp_id] = paths

            #self._logger.debug("top_levels paths %d for mp_id %s and class label %s", len(paths), mp_id, mp_class['label'])


    def assign_to_human_genes(self):

        self._logger.debug("Assigning %i entries to human genes ", len(self.mouse_genes))

        # for any mouse gene...
        for _, gene in self.mouse_genes.iteritems():

            self._logger.debug('retrieve the human orthologs...')
            for ortholog in gene["human_orthologs"]:
                human_gene_symbol = ortholog["gene_symbol"]

                self._logger.debug("Assign %i phenotype categories from mouse %s to human %s",
                                    len(gene["phenotypes"]),
                                    gene["gene_symbol"],
                                    human_gene_symbol)
                # assign all the phenotypes for this specific gene
                # all phenotypes are classified per category
                self.human_genes[human_gene_symbol]["mouse_orthologs"].append({ "mouse_gene_id" : gene["gene_id"],
                                                                          "mouse_gene_symbol" : gene["gene_symbol"],
                                                                          "phenotypes" : gene["phenotypes"].values()})

    def get_genotype_phenotype(self):
        self._logger.debug("get_genotype_phenotype")
        with URLZSource(self.data_config.mouse_phenotypes_orthology).open() as fi:
            self._logger.debug("get %s", self.data_config.mouse_phenotypes_orthology)

            for li, line in enumerate(fi):
                # a way too many false spaces just to bother people
                array = map(str.strip, line.strip().split("\t"))
                if len(array) == 7:
                    (human_gene_symbol, a, b, c, mouse_gene_symbol, mouse_gene_id, phenotypes_raw) = array

                    # at least 1 phenotype in phenotypes_raw
                    if len(phenotypes_raw) > 0:
                        try:
                            mouse_gene_id = mouse_gene_id.strip()
                            mouse_gene_symbol = mouse_gene_symbol.strip()
                            if mouse_gene_id not in self.mouse_genes:
                                self.mouse_genes[mouse_gene_id] = {"gene_id": mouse_gene_id, "gene_symbol": mouse_gene_symbol,
                                                                "phenotypes": {}, "human_orthologs": [], "phenotypes_summary" : list(phenotypes_raw.strip().split("\s+"))}
                            self.mouse_genes[mouse_gene_id]["human_orthologs"].append(
                                {"gene_symbol": human_gene_symbol, "gene_id": None})

                            if human_gene_symbol not in self.human_genes:
                                self.human_genes[human_gene_symbol] = {"gene_symbol": human_gene_symbol, "ensembl_gene_id": None,
                                                                    "gene_id": None,
                                                                    "mouse_orthologs": []}
                        except Exception as e:
                            self._logger.debug("exception processing a line %d: %s", li, str(e))

        self._logger.info("Retrieved %i mouse genes", len(self.mouse_genes))

        count_symbols = set()
        count_accepted_symbols = set()

        with URLZSource(self.data_config.mouse_phenotypes_report).open() as fi:
            # lines = response.readlines()
            self._logger.debug("get lines from mgi report phenotyes file %s",
                                self.data_config.mouse_phenotypes_report)

            # Allelic Composition	Allele Symbol(s)	Genetic Background	Mammalian Phenotype ID	PubMed ID	MGI Marker Accession ID
            for li, line in enumerate(fi):
                # a way too many false spaces just to bother people
                array = map(str.strip, line.strip().split("\t"))

                self._logger.debug('mouse KO array %s in line %d', str(array), li)

                if len(array) == 6:
                    (allelic_composition, allele_symbol, genetic_background, mp_id, pmid, mouse_gene_ids) = array
                    # check for double-mutant but exclude duplicates
                    for mouse_gene_id in set(mouse_gene_ids.split(",")):
                        # exclude heritable phenotypic marker like http://www.debugrmatics.jax.org/marker/MGI:97446
                        count_symbols.add(mouse_gene_id)

                        mp_id_key = mp_id.split("/")[-1].replace(":", "_")
                        self._logger.debug("Looking for mouse_gene_id "+mouse_gene_id)
                        self._logger.debug("Looking for mp_id_key "+mp_id_key)

                        if mouse_gene_id in self.mouse_genes and mp_id_key in self.mps:
                            self._logger.debug('process mouse KO gene %s', mouse_gene_id)
                            count_accepted_symbols.add(mouse_gene_id)
                            self._logger.debug('get class for %s'% mp_id)
                            mp_class = self.mps[mp_id_key]
                            mp_label = mp_class["label"]

                            for k, v in PHENOTYPE_CATEGORIES.iteritems():
                                if k not in self.mouse_genes[mouse_gene_id]["phenotypes"]:
                                    self.mouse_genes[mouse_gene_id]["phenotypes"][k] = \
                                        {
                                            "category_mp_identifier": k,
                                            "category_mp_label": v,
                                            "genotype_phenotype": []
                                        }

                            # it's possible that there are multiple paths to the same root.
                            mp_category_ids = set(map(lambda x: x[0], mp_class["path_codes"]))
                            for category_id in mp_category_ids:
                                mp_category_id = category_id.replace("_", ":")
                                self.mouse_genes[mouse_gene_id]["phenotypes"][mp_category_id]["genotype_phenotype"].append(
                                    {
                                        "subject_allelic_composition": allelic_composition,
                                        "subject_background": genetic_background,
                                        "pmid" : pmid,
                                        "mp_identifier": mp_id,
                                        "mp_label": mp_label
                                    })
                        else:
                            self._logger.warning('process mouse KO gene %s failed because not in self.mouse_genes set in line %d',
                                                mouse_gene_id, li)
                else:
                    self._logger.warning("could not process %i %s", len(array), line)

        self._logger.info("Count symbols %i / %i with phenotypes", len(count_accepted_symbols), len(count_symbols))
