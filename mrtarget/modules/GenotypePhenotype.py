import logging
import logging.config
import time
import json
import requests
from tqdm import tqdm
import csv
import gzip
from StringIO import StringIO
from mrtarget.common import TqdmToLogger
from mrtarget.common import Actions
from mrtarget.common.LookupHelpers import LookUpDataRetriever, LookUpDataType
from mrtarget.Settings import file_or_resource
from settings import Config

logging.config.fileConfig(file_or_resource('logging.ini'),
                              disable_existing_loggers=False)
from intermine.webservice import Service


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

class MousePhenotypeActions(Actions):
    PROCESS = 'process'
    UPLOAD = 'upload'

class MouseminePhenotypeETL(object):

    def __init__(self, loader=None, r_server=None):
        self.loader = loader
        self.r_server = r_server
        self._logger = logging.getLogger(__name__)
        self.tqdm_out = TqdmToLogger(self._logger, level=logging.INFO)
        self.service = Service("http://www.mousemine.org/mousemine/service")
        self.mouse_genes = dict()
        self.ancestors = dict()
        self.lookup_data = None
        self.mp_labels = dict()
        self.top_levels = dict()
        self.homologs = dict()
        self.human_genes = dict()
        self.human_ensembl_gene_ids = dict()

    def process_all(self):

        # process_all
        self._get_mp_classes()

    def _get_mp_classes(self):

        lookup_data_types = (LookUpDataType.MP,)
        self._logger.info(LookUpDataType.MP)
        self.lookup_data = LookUpDataRetriever(self.loader.es,
                                               self.r_server,
                                               data_types=lookup_data_types,
                                               autoload=True
                                               ).lookup
        mp_class = None
        for mp_id in self.lookup_data.available_mps.get_available_mp_ids():
            self._logger.info(mp_id)
            mp_class = self.lookup_data.available_mps.get_mp(mp_id,
                                                             self.r_server)
            self.mp_labels[mp_class["label"]] = mp_id
            paths = list()
            for path in mp_class["path"]:
                item = path[0]
                paths.append(item)
            self.top_levels[mp_id] = paths

        print json.dumps(mp_class, indent=2)

        #self._get_ortholog_data()

        self.get_genotype_phenotype()

        self.assign_to_human_genes()

    def _get_human_gene_ensembl_id(self, gene_symbol):

        if gene_symbol not in self.human_genes:
            self._logger.info("Getting Human ENSEMBL GENE ID for %s"%(gene_symbol))
            url = "https://rest.ensembl.org/lookup/symbol/homo_sapiens/%s?content-type=application/json;expand=0"%(gene_symbol)
            r = requests.get(url)
            results = r.json()
            gene_id = results["id"]
            self.human_genes[gene_symbol] = { "gene_symbol" : gene_symbol, "ensembl_gene_id" : gene_id, "gene_id": "http://identifiers.org/ensembl/" + gene_id, "mouse_orthologs" : dict() }
            self.human_ensembl_gene_ids[gene_id] = gene_symbol
        return self.human_genes[gene_symbol]

    def _get_homologs(self, gene_id=None, gene_symbol=None):

        if gene_symbol not in self.homologs:
            self._logger.info("Find homologs of %s"%(gene_symbol))
            query = self.service.new_query("Gene")
            query.add_view(
                "primaryIdentifier", "symbol", "organism.name",
                "homologues.homologue.primaryIdentifier", "homologues.homologue.symbol",
                "homologues.homologue.organism.name", "homologues.type",
                "homologues.dataSets.name"
            )
            query.add_constraint("homologues.type", "NONE OF",
                                 ["horizontal gene transfer", "least diverged horizontal gene transfer"], code="B")
            query.add_constraint("Gene", "LOOKUP", gene_symbol, "M. musculus", code="A")

            human_gene_symbols = set()

            for row in query.rows():
                human_gene_symbol = row["homologues.homologue.symbol"]
                if row["homologues.homologue.organism.name"] == 'Homo sapiens' and human_gene_symbol not in human_gene_symbols:
                    human_gene_symbols.add(human_gene_symbol)
                    ensembl_gene_id = self._get_human_gene_ensembl_id( row["homologues.homologue.symbol"] )
                    print ensembl_gene_id
                    self.mouse_genes[gene_id]["human_orthologs"].append( {"gene_symbol" : human_gene_symbol, "gene_id": ensembl_gene_id} )
                    print row["primaryIdentifier"], row["symbol"], row["organism.name"], \
                    row["homologues.homologue.primaryIdentifier"], human_gene_symbol, \
                    row["homologues.homologue.organism.name"], row["homologues.type"], \
                    row["homologues.dataSets.name"]

    def assign_to_human_genes(self):

        for id, obj in self.mouse_genes.items():
            for ortholog in obj["human_orthologs"]:
                gene_symbol = ortholog["gene_symbol"]
                self._logger.info("Assign mouse orthologs to human gene %s"%(gene_symbol))
                self.human_genes[gene_symbol]["mouse_orthologs"][id] = obj["phenotypes"]


    def get_genotype_phenotype(self):

        nb_categories = 0

        for (id, category) in PHENOTYPE_CATEGORIES.items():

            query = self.service.new_query("OntologyAnnotation")
            query.add_constraint("evidence.baseAnnotations.subject", "Genotype")
            query.add_constraint("subject", "SequenceFeature")
            query.add_constraint("ontologyTerm", "MPTerm")
            query.add_constraint("ontologyTerm.parents", "MPTerm")
            query.add_view(
                "subject.primaryIdentifier", "subject.symbol",
                "evidence.baseAnnotations.subject.symbol",
                "evidence.baseAnnotations.subject.background.name",
                "evidence.baseAnnotations.subject.zygosity", "ontologyTerm.identifier",
                "ontologyTerm.name"
            )
            query.add_sort_order("OntologyAnnotation.subject.symbol", "ASC")
        #query.add_constraint("ontologyTerm.parents", "LOOKUP", "*circulating glucose*", code="A")

            query.add_constraint("ontologyTerm.parents", "LOOKUP", "*%s*"%category, code="A")

            nb_rows = 0
            for row in query.rows():
                nb_rows+=1
                gene_id = row["subject.primaryIdentifier"]
                gene_symbol = row["subject.symbol"]
                if gene_id not in self.mouse_genes:
                    self.mouse_genes[gene_id] = {"gene_id" : gene_id, "gene_symbol" : gene_symbol, "phenotypes" : dict(), "human_orthologs" : [] }
                    self._get_homologs(gene_id=gene_id, gene_symbol=gene_symbol)
                if id not in self.mouse_genes[gene_id]:
                    self.mouse_genes[gene_id]["phenotypes"][id] = {"mp_category" : category.replace(" phenotype", ""), "genotype_phenotype" : [] }
                self.mouse_genes[gene_id]["phenotypes"][id]["genotype_phenotype"].append({
                    "subject_symbol" : row["evidence.baseAnnotations.subject.symbol"],
                    "subject_background" : row["evidence.baseAnnotations.subject.background.name"],
                    "subject_zygosity" : row["evidence.baseAnnotations.subject.zygosity"],
                    "mp_identifier" : row["ontologyTerm.identifier"],
                    "mp_label" : row["ontologyTerm.name"] })

                mp_id = row["ontologyTerm.identifier"].replace(":", "_")
                top_levels = map(lambda x: x["label"], self.top_levels[mp_id])
                human_orthologs = map(lambda x: x["gene_id"], self.mouse_genes[gene_id]["human_orthologs"])
                url = "http://www.informatics.jax.org/marker/" + gene_id

                print row["subject.primaryIdentifier"], row["subject.symbol"], \
                    row["evidence.baseAnnotations.subject.symbol"], \
                    row["evidence.baseAnnotations.subject.background.name"], \
                    row["evidence.baseAnnotations.subject.zygosity"], row["ontologyTerm.identifier"], \
                    row["ontologyTerm.name"], "; ".join(top_levels), \
                    json.dumps(human_orthologs)

                if nb_rows > 200:
                    return

            time.sleep(3)
            return
            #self.logger.debug(row["subject.primaryIdentifier"], row["subject.symbol"], \
            #   row["evidence.baseAnnotations.subject.symbol"], \
            #    row["evidence.baseAnnotations.subject.background.name"], \
            #    row["evidence.baseAnnotations.subject.zygosity"], row["ontologyTerm.identifier"], \
            #    row["ontologyTerm.name"])

def main():

    obj = MouseminePhenotypeETL()
    #obj.get_genotype_phenotype()

if __name__ == "__main__":
    main()
